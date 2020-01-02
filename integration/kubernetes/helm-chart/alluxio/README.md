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

A Persistent Volume Claim is created for each master Pod defined in `volumeClaimTemplates` in `master/statefulset.yaml`.
The PVC is satisfied by Persistent Volumes provisioned in `master/journal-pv.yaml`.
