import os
import argparse
import math
import time
import logging
from collections import Counter

from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException


def setup_logging(level: str = None):
    level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(__name__).info("Logging inicializado nivel=%s", level)


def load_client(kubeconfig=None) -> client.CoreV1Api:
    """
    - Si kubeconfig está definido -> modo fuera del cluster.
    - Si no -> in-cluster.
    """
    if kubeconfig:
        config.load_kube_config(config_file=kubeconfig)
        logging.getLogger(__name__).info("Cliente fuera de cluster (kubeconfig=%s)", kubeconfig)
    else:
        config.load_incluster_config()
        logging.getLogger(__name__).info("Cliente in-cluster")
    return client.CoreV1Api()


def bind_pod(api: client.CoreV1Api, pod: client.V1Pod, node_name: str):
    """
    Crea un Binding Pod -> Node.
    """
    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node_name)
    meta = client.V1ObjectMeta(name=pod.metadata.name, namespace=pod.metadata.namespace)
    body = client.V1Binding(metadata=meta, target=target)

    # _preload_content=False ayuda a evitar problemas de deserialización en algunos entornos
    api.create_namespaced_binding(
        namespace=pod.metadata.namespace,
        body=body,
        _preload_content=False,
    )
    logging.getLogger(__name__).info("Bound %s/%s -> %s", pod.metadata.namespace, pod.metadata.name, node_name)


def snapshot_node_load(api: client.CoreV1Api):
    """
    Devuelve:
      - lista de nodos
      - Counter con #pods "vivos" por nodo (excluye Succeeded/Failed)
    """
    nodes = [n.metadata.name for n in api.list_node().items]
    if not nodes:
        raise RuntimeError("No nodes available")

    pods = api.list_pod_for_all_namespaces().items
    alive = [
        p for p in pods
        if p.spec and p.spec.node_name
        and (p.status is None or p.status.phase not in ("Succeeded", "Failed"))
    ]
    load = Counter(p.spec.node_name for p in alive)
    return nodes, load


def choose_node(api: client.CoreV1Api, pod: client.V1Pod) -> str:
    """
    Política simple: nodo con menos pods running.
    (Se recalcula snapshot cada vez; para más rendimiento podrías cachear unos segundos.)
    """
    nodes, load = snapshot_node_load(api)
    pick = min(nodes, key=lambda n: load.get(n, 0))
    logging.getLogger(__name__).info("Nodo elegido para %s/%s: %s", pod.metadata.namespace, pod.metadata.name, pick)
    return pick


def should_schedule(pod: client.V1Pod, scheduler_name: str) -> bool:
    """
    Solo planifica pods:
      - sin node_name
      - en fase Pending (si está disponible)
      - que apunten a nuestro schedulerName
      - no en borrado
    """
    if pod is None or pod.spec is None or pod.metadata is None:
        return False
    if pod.metadata.deletion_timestamp:
        return False
    if getattr(pod.spec, "node_name", None):
        return False
    if getattr(pod.spec, "scheduler_name", None) != scheduler_name:
        return False
    phase = getattr(getattr(pod, "status", None), "phase", None)
    if phase is not None and phase != "Pending":
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler")
    parser.add_argument("--kubeconfig", default=None)
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(args.log_level)
    api = load_client(args.kubeconfig)

    logging.getLogger(__name__).info("[watch] scheduler starting… name=%s", args.scheduler_name)

    # Bucle externo: si el stream cae (timeout/red), reintenta
    while True:
        w = watch.Watch()
        try:
            for evt in w.stream(
                api.list_pod_for_all_namespaces,
                _request_timeout=60,
                timeout_seconds=60,
            ):
                obj = evt.get("object")
                etype = evt.get("type")

                if obj is None or not hasattr(obj, "spec"):
                    continue
                if etype not in ("ADDED", "MODIFIED"):
                    continue
                if not should_schedule(obj, args.scheduler_name):
                    continue

                try:
                    node = choose_node(api, obj)
                    bind_pod(api, obj, node)
                except ApiException:
                    logging.exception("Error API al bindear %s/%s",
                                    obj.metadata.namespace, obj.metadata.name)

        except Exception:
            logging.exception("Watch stream caído; reintentando en 2s")
            time.sleep(2)



if __name__ == "__main__":
    main()

