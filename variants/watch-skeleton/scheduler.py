import argparse, math
from kubernetes import client, config, watch

# TODO: load_client(kubeconfig) -> CoreV1Api
#  - Use config.load_incluster_config() by default, else config.load_kube_config()

# TODO: bind_pod(api, pod, node_name)
#  - Create a V1Binding with metadata.name=pod.name and target.kind=Node,target.name=node_name
#  - Call api.create_namespaced_binding(namespace, body)

# TODO: choose_node(api, pod) -> str
#  - List nodes and pick one based on a simple policy (fewest running pods)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler-name", default="my-scheduler")
    parser.add_argument("--kubeconfig", default=None)
    args = parser.parse_args()

    # TODO: api = load_client(args.kubeconfig)

    print(f"[watch] scheduler starting… name={args.scheduler_name}")
    w = watch.Watch()
    # Stream Pod events across all namespaces
    for evt in w.stream(client.CoreV1Api().list_pod_for_all_namespaces, _request_timeout=60):
        obj = evt['object']
        if obj is None or not hasattr(obj, 'spec'):
            continue
        # TODO: Only act on Pending pods that target our schedulerName
        #  - if obj.spec.node_name is not set and obj.spec.scheduler_name == args.scheduler_name:
        #       node = choose_node(api, obj)
        #       bind_pod(api, obj, node)
        #       print(...)

if __name__ == "__main__":
    main()
