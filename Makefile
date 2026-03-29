IMG ?= valkey-operator:latest
CONTROLLER_GEN_VERSION ?= v0.20.1
CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

.PHONY: all
all: build

## Generate DeepCopy methods and CRD/RBAC manifests from kubebuilder markers.
.PHONY: generate
generate:
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: manifests
manifests:
	$(CONTROLLER_GEN) rbac:roleName=valkey-operator-manager-role crd paths="./..." \
		output:crd:artifacts:config=config/crd/bases \
		output:rbac:artifacts:config=config/rbac

## Build the manager binary.
.PHONY: build
build:
	go build -o bin/manager ./cmd/main.go

## Run against the configured Kubernetes cluster.
.PHONY: run
run:
	go run ./cmd/main.go

## Run tests.
.PHONY: test
test:
	go test ./... -coverprofile cover.out

## Install CRD into the cluster.
.PHONY: install
install: manifests
	kubectl apply -k config/crd

## Uninstall CRD from the cluster.
.PHONY: uninstall
uninstall:
	kubectl delete -k config/crd --ignore-not-found=true

## Deploy operator to the cluster.
.PHONY: deploy
deploy: manifests
	cd config/manager && kustomize edit set image valkey-operator-controller-manager=$(IMG)
	kubectl apply -k config/default

## Undeploy operator from the cluster.
.PHONY: undeploy
undeploy:
	kubectl delete -k config/default --ignore-not-found=true

## Build the operator Docker image.
.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)

SERVER_IMG ?= valkey-operator-server:latest

## Build the Valkey server image (with operator module embedded).
.PHONY: docker-build-server
docker-build-server:
	docker build -t $(SERVER_IMG) ./server

## Download dependencies.
.PHONY: tidy
tidy:
	go mod tidy

## Lint the code.
.PHONY: lint
lint:
	golangci-lint run ./...
