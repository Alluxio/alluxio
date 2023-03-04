
ALLUXIO_DEV_IMAGE ?= alluxio/alluxio-dev
ALLUXIO_IMAGE ?= alluxio/alluxio
TAG := customizedUser


.PHONY: all
all: docker-build docker-push

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: ut
ut:
	go test -v -cover -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

.PHONY: alluxio
alluxio:
	docker build -t ${ALLUXIO_DEV_IMAGE} -f Dockerfile-dev .

.PHONY: customizedUser
customizedUser:
	docker build -t ${ALLUXIO_DEV_IMAGE}:${TAG} -f Dockerfile-dev .

.PHONY: customizedUser-arg
customizedUser-arg:
	docker build -t ${ALLUXIO_IMAGE}:${TAG} \
    --build-arg ALLUXIO_USERNAME=alluxio2 --build-arg ALLUXIO_UID=1001 \
    --build-arg ALLUXIO_GROUP=alluxio2 --build-arg ALLUXIO_GID=1001 .

.PHONY: fuse
fuse:
	docker run -e ALLUXIO_MASTER_HOSTNAME=alluxio-master --cap-add SYS_ADMIN --device /dev/fuse alluxio/alluxio fuse --fuse-opts=allow_other

.PHONY: docker-push
docker-push:
	docker push ${IMAGE}:${TAG}

install-lint:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@latest

lint: ## Run golangci-lint
	@echo "---lint---"
	golangci-lint run -v
