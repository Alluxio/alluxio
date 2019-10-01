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

Please refer [https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html](https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html) for the configurations.

## Persistence

The [Alluxio](https://hub.docker.com/r/alluxio/alluxio) image stores the Journal data at the `/journal` path of the container.

By default an [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) is created and mounted into `/journal`. If you want the journal to be persisted, you will need a Persistent Volume Claim and the corresponding Persistent Volume. The Persistent Volume Claim name is specified by `journal.pvcName`. And you can find the related Kubernetes documentation [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).

> *"An emptyDir volume is first created when a Pod is assigned to a Node, and exists as long as that Pod is running on that node. When a Pod is removed from a node for any reason, the data in the emptyDir is deleted forever."*
