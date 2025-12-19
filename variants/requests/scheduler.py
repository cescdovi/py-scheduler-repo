import os
import argparse
import time
import logging
from typing import Dict, Tuple, List, Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

# ----------------------
# Logging y cliente K8s
# ----------------------

CPU_M = 1000  # millicores en 1 vCPU

MEM_BIN = {
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
}

MEM_DEC = {
    "K": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
    "P": 10**15,
    "E": 10**18,
}


def setup_logging(level: str = None) -> None:
    logging.basicConfig(
        level=(level or os.getenv("LOG_LEVEL", "INFO")).upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(__name__).info("Logging inicializado")


def load_client(kubeconfig: Optional[str] = None) -> client.CoreV1Api:
    try:
        if kubeconfig:
            config.load_kube_config(kubeconfig)
            logging.getLogger(__name__).info("Cliente fuera de cluster (kubeconfig=%s)", kubeconfig)
        else:
            config.load_incluster_config()
            logging.getLogger(__name__).info("Cliente in-cluster")
        return client.CoreV1Api()
    except Exception:
        logging.getLogger(__name__).exception("Error al cargar el cliente de Kubernetes")
        raise


# ----------------------
# Helpers: quantities
# ----------------------

def cpu_to_millicores(q) -> int:
    if q is None:
        return 0
    if isinstance(q, (int, float)):
        return int(float(q) * CPU_M)

    s = str(q).strip()
    if not s:
        return 0

    if s.endswith("m"):
        try:
            return int(float(s[:-1]))
        except ValueError:
            return 0

    try:
        return int(float(s) * CPU_M)
    except ValueError:
        return 0


def mem_to_bytes(q) -> int:
    if q is None:
        return 0
    if isinstance(q, (int, float)):
        return int(float(q))

    s = str(q).strip()
    if not s:
        return 0

    for suf, mul in MEM_BIN.items():
        if s.endswith(suf):
            try:
                return int(float(s[:-len(suf)]) * mul)
            except ValueError:
                return 0

    for suf, mul in MEM_DEC.items():
        if s.endswith(suf):
            try:
                return int(float(s[:-len(suf)]) * mul)
            except ValueError:
                return 0

    try:
        return int(float(s))
    except ValueError:
        return 0


# ----------------------
# Recursos de Pod / Nodo
# ----------------------

class PodRequests:
    """Solo requests (lo que normalmente se usa para scheduling)."""
    def __init__(self, cpu_req_m: int = 0, mem_req_b: int = 0):
        self.cpu_req_m = int(cpu_req_m)
        self.mem_req_b = int(mem_req_b)

    def __repr__(self) -> str:
        return f"PodRequests(cpu={self.cpu_req_m}m mem={self.mem_req_b}B)"


def aggregate_pod_requests(pod) -> PodRequests:
    cpu_r = 0
    mem_r = 0
    if not pod or not pod.spec or not pod.spec.containers:
        return PodRequests()

    for c in pod.spec.containers:
        if c.resources and c.resources.requests:
            cpu_r += cpu_to_millicores(c.resources.requests.get("cpu"))
            mem_r += mem_to_bytes(c.resources.requests.get("memory"))

    return PodRequests(cpu_r, mem_r)


def node_allocatable(n) -> Tuple[int, int]:
    """Devuelve (cpu_m, mem_B) allocatable del nodo."""
    alloc = getattr(n.status, "allocatable", {}) or {}
    cpu = cpu_to_millicores(alloc.get("cpu"))
    mem = mem_to_bytes(alloc.get("memory"))
    return cpu, mem


def is_node_ready(n) -> bool:
    conds = getattr(n.status, "conditions", None) or []
    for c in conds:
        if c.type == "Ready":
            return str(c.status).lower() == "true"
    return False


def is_node_schedulable(n) -> bool:
    # Si está cordoned: spec.unschedulable=True
    if getattr(n.spec, "unschedulable", False):
        return False
    return True


def pod_phase(p) -> str:
    try:
        return (p.status.phase or "").strip()
    except Exception:
        return ""


def should_schedule_pod(pod, scheduler_name: str) -> bool:
    """Actúa solo sobre pods Pendings, sin nodeName, de nuestro schedulerName y no en borrado."""
    if pod is None or not getattr(pod, "spec", None) or not getattr(pod, "metadata", None):
        return False

    if getattr(pod.metadata, "deletion_timestamp", None):
        return False

    if getattr(pod.spec, "node_name", None):
        return False

    if getattr(pod.spec, "scheduler_name", None) != scheduler_name:
        return False

    # Recomendado: Pending
    if pod_phase(pod) != "Pending":
        return False

    return True


def compute_used_by_node_from_pods(pods) -> Dict[str, Tuple[int, int]]:
    """
    Suma requests CPU/mem (millicores/bytes) por nodo usando un único listado de pods.
    Ignora Succeeded/Failed para aproximar uso activo.
    """
    used: Dict[str, Tuple[int, int]] = {}
    for p in pods:
        nname = getattr(p.spec, "node_name", None)
        if not nname:
            continue
        phase = pod_phase(p)
        if phase in ("Succeeded", "Failed"):
            continue

        pr = aggregate_pod_requests(p)
        cur_cpu, cur_mem = used.get(nname, (0, 0))
        used[nname] = (cur_cpu + pr.cpu_req_m, cur_mem + pr.mem_req_b)
    return used


# ----------------------
# Binding (Pod -> Node)
# ----------------------

def bind_pod(api: client.CoreV1Api, pod, node_name: str, max_retries: int = 4) -> None:
    """
    Crea un Binding para asignar el Pod a un Node.
    Retry simple con backoff ante errores transitorios.
    """
    log = logging.getLogger(__name__)

    target = client.V1ObjectReference(kind="Node", api_version="v1", name=node_name)
    meta = client.V1ObjectMeta(name=pod.metadata.name, namespace=pod.metadata.namespace)
    body = client.V1Binding(metadata=meta, target=target)

    backoff = 0.2
    for attempt in range(1, max_retries + 1):
        try:
            # En algunos entornos, _preload_content=False evita problemas de deserialización;
            # si no lo necesitas, puedes quitarlo.
            api.create_namespaced_binding(
                namespace=pod.metadata.namespace,
                body=body,
                _preload_content=False,
            )
            log.info("Bound %s/%s -> %s", pod.metadata.namespace, pod.metadata.name, node_name)
            return

        except ApiException as e:

            # Errores  típicos (server busy, timeouts, etc.)
            if e.status in (429, 500, 502, 503, 504):
                log.warning("API transient (%s) binding %s/%s -> %s. Retry %d/%d",
                            e.status, pod.metadata.namespace, pod.metadata.name, node_name, attempt, max_retries)
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)
                continue

            log.exception("ApiException binding %s/%s -> %s (status=%s)",
                          pod.metadata.namespace, pod.metadata.name, node_name, e.status)
            raise

        except Exception:
            log.exception("Error inesperado creando binding %s/%s -> %s",
                          pod.metadata.namespace, pod.metadata.name, node_name)
            raise


# ----------------------
# Política de selección
# ----------------------

def choose_node(
    nodes,
    used_by_node: Dict[str, Tuple[int, int]],
    pod_req_cpu_m: int,
    pod_req_mem_b: int,
) -> Optional[str]:
    """
    1) Filtra nodos Ready + schedulable (no cordoned).
    2) Filtra por capacidad: used + pod_req <= allocatable (CPU/Mem).
    3) Score: maximiza el slack mínimo tras asignar el pod:
       score = min(cpu_free_after, mem_free_after)
    """
    log = logging.getLogger(__name__)
    feasible: List[Tuple[str, int, int, int]] = []  # (name, score, cpu_free_after, mem_free_after)

    for n in nodes:
        name = n.metadata.name

        if not is_node_ready(n):
            log.debug("Node %s descartado: not Ready", name)
            continue
        if not is_node_schedulable(n):
            log.debug("Node %s descartado: unschedulable (cordoned)", name)
            continue

        alloc_cpu, alloc_mem = node_allocatable(n)
        used_cpu, used_mem = used_by_node.get(name, (0, 0))

        cpu_free_after = alloc_cpu - (used_cpu + pod_req_cpu_m)
        mem_free_after = alloc_mem - (used_mem + pod_req_mem_b)

        if cpu_free_after < 0 or mem_free_after < 0:
            log.debug("Node %s no factible: after(cpu=%sm, mem=%sB)", name, cpu_free_after, mem_free_after)
            continue

        score = min(cpu_free_after, mem_free_after)
        feasible.append((name, score, cpu_free_after, mem_free_after))

    if not feasible:
        return None

    feasible.sort(key=lambda t: (t[1], t[2], t[0]), reverse=True)
    pick = feasible[0][0]
    return pick


# ----------------------
# Main loop (polling)
# ----------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler")
    parser.add_argument("--kubeconfig", default=None)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    setup_logging(args.log_level)
    api = load_client(args.kubeconfig)
    log = logging.getLogger(__name__)
    log.info("[polling][resource-aware] scheduler starting… name=%s", args.scheduler_name)

    while True:
        try:
            # Un solo listado de pods por iteración: sirve para detectar pendings y calcular used_by_node
            all_pods = api.list_pod_for_all_namespaces().items
            used_by_node = compute_used_by_node_from_pods(all_pods)

            # Listado de nodos 1 vez por iteración
            nodes = api.list_node().items

            # Filtra pods a planificar
            pending = [p for p in all_pods if should_schedule_pod(p, args.scheduler_name)]

            for pod in pending:
                pr = aggregate_pod_requests(pod)
                req_cpu = max(0, pr.cpu_req_m)
                req_mem = max(0, pr.mem_req_b)

                node = choose_node(nodes, used_by_node, req_cpu, req_mem)
                if not node:
                    log.info(
                        "Sin nodo factible para %s/%s (req cpu=%sm mem=%sB). Queda Pending.",
                        pod.metadata.namespace, pod.metadata.name, req_cpu, req_mem
                    )
                    continue

                bind_pod(api, pod, node)
                cur_cpu, cur_mem = used_by_node.get(node, (0, 0))
                used_by_node[node] = (cur_cpu + req_cpu, cur_mem + req_mem)

        except Exception:
            log.exception("Error en el loop principal del scheduler (se reintenta en el siguiente ciclo).")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()