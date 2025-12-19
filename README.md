# Custom Kubernetes Scheduler

This repository provides a minimal custom scheduler written in **Python** using the
`kubernetes` Python client. It includes three variants:

- **main (polling)**: simple polling loop that finds Pending pods and binds them.
- **(watch-based)**: skeleton using the watch stream with TODOs.

Use `scripts/init_branches.sh` to create Git branches locally: `main`, `student`, `solution`, or use
your own approach for that.

## Quickstart

```bash
# 0) Prereqs
#    kind, kubectl, Docker

# 1) Create a kind cluster
kind create cluster --name sched-lab

# 2) Build & load image
make build
make kind-load

# 3) Deploy ServiceAccount/RBAC + Deployment
make deploy

# 4) Schedule a test Pod using your scheduler
make test

# 5) Watch logs for binding output
make logs
```

Cleanup:
```bash
make undeploy
kind delete cluster --name sched-lab
```

## Local run (optional)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scheduler.py --scheduler-name my-scheduler --kubeconfig ~/.kube/config
```
