# Alluxio

[Alluxio](https://www.alluxio.io/) is a Data orchestration for analytics and machine learning in the cloud.


## Introduction

This chart bootstraps [Alluxio](https://www.alluxio.io/) cluster on a [Kubernetes]() cluster using the [Helm]() package manager.


## Prerequisites

* Kubernetes 1.11+ with Beta APIs enabled 


## Install the Chart

To install the Alluxio Chart into your Kubernetes cluster :

```
helm install --namespace "alluxio" --name "alluxio" alluxio
```

After installation succeeds, you can get a status of Chart

```
helm status "alluxio"
```

## Uninstall the Chart

If you want to delete your Chart, use this command:

```
helm delete  --purge "alluxio"
```

## Configuration

The following table lists the configurable parameters of the Alluxio chart and their default values.

| Parameter                                    | Description                                                                                  | Default                                              |
| -------------------------------------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `fsGroup`                                       | Volumes that support ownership management are modified to be owned and writable by the GID specified in fsGroup. | `0`                                                 |
| `fuse.shortCircuitPolicy`                    | fuse's short circuit policy, if it's local, it means to share the directory. If it's uuid, it means connecting through domain socket `alluxio.worker.data.server.domain.socket.address`                                                       | local or uuid                           |
| `image`                                      | `alluxio` image repository.                                                                    | `alluxio`                                              |
| `imageTag`                                   | `alluxio` image tag.                                                                           | `2.1.0-SNAPSHOT`                                             |



## Persistence

The [Alluxio](https://hub.docker.com/r/alluxio/alluxio) image stores the Journal data at the `/journal` path of the container.

By default an emptyDir is created and mounted into that directory, it's good for development and testing. If you'd like to persistent your journal data. Please specify `journal.pvcName`.

> *"An emptyDir volume is first created when a Pod is assigned to a Node, and exists as long as that Pod is running on that node. When a Pod is removed from a node for any reason, the data in the emptyDir is deleted forever."*
























https://github.com/helm/charts/tree/master/stable/alluxio
