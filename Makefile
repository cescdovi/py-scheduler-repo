APP=my-py-scheduler
KIND_CLUSTER=sched-lab

.PHONY: build kind-load deploy test logs undeploy

build:
	docker build -t $(APP):latest .

kind-load:
	kind load docker-image $(APP):latest --name $(KIND_CLUSTER)

deploy:
	kubectl apply -f rbac-deploy.yaml

test:
	kubectl apply -f test-pod.yaml

logs:
	kubectl -n kube-system logs deploy/my-scheduler -f

undeploy:
	kubectl delete -f rbac-deploy.yaml --ignore-not-found
	kubectl delete -f test-pod.yaml --ignore-not-found
