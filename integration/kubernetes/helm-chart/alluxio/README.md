# Alluxio

[Alluxio](https://www.alluxio.io/) is a Data orchestration for analytics and machine learning in the cloud.


## Introduction

This chart bootstraps [Alluxio](https://www.alluxio.io/) cluster on a [Kubernetes]() cluster using the [Helm]() package manager.


## Prerequisites

### Kubernetes
Kubernetes 1.11+ with Beta APIs enabled 

### Persistent volumes for Alluxio journal
If you are using local UFS journal or embedded journals, Alluxio masters need persistent volumes for storing the journals.
In that case, you will need to provision one persistent volume for one master replica.
In the Helm installation, Alluxio master Pods need the persistent volumes to start.

The required size for each journal volume is defined in `helm/alluxio/values.yaml` as follows. 
By default each journal persistent volume should be at least 1Gi.
```yaml
journal:
  size: 1Gi
```

If you are the cluster admin, you can provision one PersistentVolume as follows. 
Note that you need one such PersistentVolume for each Alluxio master Pod. 
```yaml
kind: PersistentVolume
apiVersion: v1
metadata:
  name: alluxio-journal-volume-0
  labels:
    type: local
spec:
  storageClassName: standard
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteMany
  hostPath:
    path: /tmp/alluxio-journal-0
```

If you are not the cluster admin and don't have the access to create PersistentVolumes,
please talk to your admin user and get the PersistentVolumes provisioned for Alluxio masters to use. 

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
The PVC should be satisfied by the PersistentVolume you create.
