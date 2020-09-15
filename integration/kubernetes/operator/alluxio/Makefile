
# Image URL to use all building/pushing image targets
IMG ?= alluxio/alluxio-controller
# Produce CRDs that work back to Kubernetes 1.11 (no version conversion)
CRD_OPTIONS ?= "crd:trivialVersions=true"

WEBHOOK_IMG ?= alluxio/alluxio-webhook

MOUNT_IMG ?= alluxio/alluxio-mount

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif
BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_TAG=$(shell if [ -z "`git status --porcelain`" ]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
GIT_TREE_STATE=$(shell echo "clean")
GIT_SHA=$(shell git rev-parse --short HEAD || echo "HEAD")
GIT_VERSION=v0.1.0-${GIT_SHA}

all: manager

# Run tests
test: generate fmt vet manifests
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test ./... -coverprofile cover.out

# Build manager binary
manager: generate fmt vet
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build  -o bin/manager cmd/controller/main.go

# Build webhook
webhook: generate fmt vet
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build  -o bin/webhook cmd/webhook/*.go

# Run against the configured Kubernetes cluster in ~/.kube/config, add debug
run: generate fmt vet manifests
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go run -gcflags "all=-N -l" cmd/controller/main.go

# Run against the configured Kubernetes cluster in ~/.kube/config, add debug
runwebhook: generate fmt vet manifests
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go run -gcflags "all=-N -l" cmd/webhook/*.go

# Debug against the configured Kubernetes cluster in ~/.kube/config, add debug
debug: generate fmt vet manifests
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 dlv debug --headless --listen ":12345" --log --api-version=2 cmd/controller/main.go

# Install CRDs into a cluster
install: manifests
	kustomize build config/crd | kubectl apply -f -

# Deploy controller in the configured Kubernetes cluster in ~/.kube/config
deploy: manifests
	cd config/manager && kustomize edit set image controller=${IMG}:${GIT_VERSION}
	kustomize build config/default | kubectl apply -f -

# Generate manifests e.g. CRD, RBAC etc.
manifests: controller-gen
	GO111MODULE=off ENABLE_WEBHOOKS=false $(CONTROLLER_GEN) $(CRD_OPTIONS) rbac:roleName=manager-role webhook paths="./..." output:crd:artifacts:config=config/crd/bases

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

# Generate code
generate: controller-gen
	GO111MODULE=off $(CONTROLLER_GEN) object:headerFile=./hack/boilerplate.go.txt paths="./..."
	# GO111MODULE=off $(CONTROLLER_GEN) object:headerFile=./hack/boilerplate.go.txt paths=./pkg/common/api/types.go 

# Build the docker image
docker-build: generate fmt vet
	docker build . -t ${IMG}:${GIT_VERSION}

docker-build-webhook: generate fmt vet
	docker build . -f Dockerfile.webhook -t ${WEBHOOK_IMG}:${GIT_VERSION}

docker-build-debug: generate fmt vet
	docker build . -f Dockerfile.debug -t ${IMG}:${GIT_VERSION}-debug

docker-build-mount:
	docker build -f cmd/webhook/check-mount/Dockerfile -t ${MOUNT_IMG} cmd/webhook/check-mount

# Push the docker image
docker-push: docker-build
	docker push ${IMG}:${GIT_VERSION}

# Push the docker image of webhook
docker-push-webhook: docker-build-webhook
	docker push ${WEBHOOK_IMG}:${GIT_VERSION}

docker-push-mount: docker-build-mount
	docker push ${MOUNT_IMG}

docker-push-debug: docker-build-debug
	docker push ${IMG}:${GIT_VERSION}-debug

# find or download controller-gen
# download controller-gen if necessary
controller-gen:
ifeq (, $(shell which controller-gen))
	@{ \
	set -e ;\
	CONTROLLER_GEN_TMP_DIR=$$(mktemp -d) ;\
	cd $$CONTROLLER_GEN_TMP_DIR ;\
	go mod init tmp ;\
	go get sigs.k8s.io/controller-tools/cmd/controller-gen@v0.2.2 ;\
	rm -rf $$CONTROLLER_GEN_TMP_DIR ;\
	}
CONTROLLER_GEN=$(GOBIN)/controller-gen
else
CONTROLLER_GEN=$(shell which controller-gen)
endif
