import os
import argparse, time, math, logging
from kubernetes import client, config

def setup_logging(level: str = None):
    """Configura el logging.
    Nivel por defecto INFO; se puede ajustar vía --log-level o env LOG_LEVEL.
    """
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(__name__).info("Logging inicializado")

def load_client(kubeconfig=None):
    """
    Carga la configuración de acceso al API server de Kubernetes y
    devuelve un cliente CoreV1Api para hacer peticiones.
    
    - Si se pasa un fichero kubeconfig, lo usa (modo fuera del cluster).
    - Si no, asume que está corriendo dentro del cluster y usa InClusterConfig.
    """
    try:
        logging.getLogger(__name__).debug("load_client(kubeconfig=%s)", kubeconfig)
        if kubeconfig:
            # Carga la configuración desde un fichero kubeconfig local
            config.load_kube_config(kubeconfig)
            logging.getLogger(__name__).info("Cliente fuera de cluster (kubeconfig)")
        else:
            # Carga la configuración desde las variables/secretos del Pod (in-cluster)
            config.load_incluster_config()
            logging.getLogger(__name__).info("Cliente in-cluster")
        # Devuelve un cliente de la API de núcleo (Pods, Nodes, Namespaces, etc.)
        return client.CoreV1Api()
    except Exception as e:
        logging.getLogger(__name__).exception("Error al cargar el cliente de Kubernetes")
        raise e

def bind_pod(api: client.CoreV1Api, pod, node_name: str):
    """
    Crea un 'binding' para asociar un Pod a un Node concreto.
    
    - api: cliente de Kubernetes.
    - pod: objeto Pod que queremos planificar.
    - node_name: nombre del nodo elegido.
    """
    try:
        target = client.V1ObjectReference()
        target.kind = "Node"
        target.api_version = "v1"
        target.name = node_name
        
        meta = client.V1ObjectMeta()
        meta.name = pod.metadata.name
        
        body = client.V1Binding(metadata=meta, target=target)
        
        # Workaround: usar _preload_content=False para evitar el bug de deserialización
        api.create_namespaced_binding(
            namespace=pod.metadata.namespace, 
            body=body,
            _preload_content=False
        )
        logging.getLogger(__name__).info("bind_pod: creado binding para %s/%s a %s",
                                        pod.metadata.namespace, pod.metadata.name, node_name)
    except Exception as e:
        logging.getLogger(__name__).exception("Error en bind_pod para %s/%s a %s",
                                             pod.metadata.namespace, pod.metadata.name, node_name)
        raise e

def choose_node(api: client.CoreV1Api, pod) -> str:
    """
    Elige un nodo para el Pod según una política muy simple:
    seleccionar el Node con MENOR número de Pods asignados.
    
    - api: cliente de Kubernetes.
    - pod: objeto Pod (no se usa directamente aquí, pero podría usarse
      para lógica más avanzada, como afinidades o tolerations).
    """
    try:
        # Obtenemos la lista de Nodes del cluster
        logging.getLogger(__name__).debug("choose_node: list_node")
        nodes = api.list_node().items
        if not nodes:
            # Si no hay nodos, no podemos planificar nada
            raise RuntimeError("No nodes available")
        
        # Obtenemos todos los Pods de todos los namespaces
        logging.getLogger(__name__).debug("choose_node: list_pod_for_all_namespaces")
        pods = api.list_pod_for_all_namespaces().items
        
        # min_cnt será el mínimo número de pods encontrado en un nodo
        min_cnt = math.inf
        # pick será el nombre del nodo elegido; inicializamos con el primero
        pick = nodes[0].metadata.name
        
        # Recorremos cada nodo
        for n in nodes:
            # Contamos cuántos pods están asignados a este nodo
            cnt = sum(1 for p in pods if p.spec.node_name == n.metadata.name)
            # Si este nodo tiene menos pods que el mínimo actual, lo elegimos
            if cnt < min_cnt:
                min_cnt = cnt
                pick = n.metadata.name
            logging.getLogger(__name__).debug("Node %s tiene %s pods", n.metadata.name, cnt)
        # Devolvemos el nombre del nodo elegido
        logging.getLogger(__name__).info("Nodo elegido para %s/%s: %s",
                                        pod.metadata.namespace, pod.metadata.name, pick)
        return pick
    except Exception as e:
        logging.getLogger(__name__).exception("Error en choose_node para %s/%s",
                                             pod.metadata.namespace, pod.metadata.name)
        raise e

def main():
    """
    Función principal del scheduler 'por sondeo' (polling):
    
    - Lee argumentos de línea de comandos (nombre del scheduler, kubeconfig, intervalo).
    - Crea el cliente de la API.
    - En un bucle infinito:
      * Lista los Pods sin nodo asignado (Pending).
      * Filtra por los que tengan nuestro schedulerName.
      * Llama a choose_node para elegir un Node.
      * Llama a bind_pod para fijar el Pod en ese Node.
      * Espera el intervalo configurado y repite.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler") # Nombre que debe tener spec.schedulerName en los Pods que gestionaremos
    parser.add_argument("--kubeconfig", default=None) # Ruta al kubeconfig (para ejecución fuera de cluster); si no se pasa, usa in-cluster
    parser.add_argument("--interval", type=float, default=2.0) # Intervalo de sondeo en segundos (cada cuánto tiempo mira Pods Pending)
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO")) # Nivel de logging
    args = parser.parse_args()

    setup_logging(args.log_level)
    # Creamos el cliente de la API de Kubernetes
    api = load_client(args.kubeconfig)
    logging.getLogger(__name__).info("[polling] scheduler starting… name=%s", args.scheduler_name)
    
    # Bucle infinito: este es el "control loop" del scheduler
    while True:
        # Listamos todos los Pods que no tienen nodeName asignado (están sin planificar)
        # field_selector="spec.nodeName=" significa: nodeName vacío
        logging.getLogger(__name__).debug("Listando pods Pending (sin nodeName)")
        pods = api.list_pod_for_all_namespaces(field_selector="spec.nodeName=").items
        
        # Recorremos cada Pod pendiente
        for pod in pods:
            # Solo queremos tocar los Pods que están configurados para nuestro scheduler
            if pod.spec.scheduler_name != args.scheduler_name:
                continue
            try:
                # Elegimos un nodo para este Pod
                node = choose_node(api, pod)
                # Creamos el binding Pod -> Node
                bind_pod(api, pod, node)
                # Log simple para ver qué pasó
                logging.getLogger(__name__).info("Bound %s/%s -> %s",
                                                pod.metadata.namespace, pod.metadata.name, node)
            except Exception as e:
                # Si algo falla (sin nodos, problema de conexión, etc.), lo mostramos
                logging.getLogger(__name__).exception("Fallo al planificar %s/%s",
                                                     pod.metadata.namespace, pod.metadata.name)
        
        # Esperamos el tiempo configurado antes de volver a comprobar
        logging.getLogger(__name__).debug("Sleep %ss", args.interval)
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
