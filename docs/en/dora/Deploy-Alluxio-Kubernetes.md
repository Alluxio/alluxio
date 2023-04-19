---
layout: global
title: Deploy Alluxio on Kubernetes
group: Dora
priority: 2
---

This documentation shows how to deploy Alluxio (Dora) on Kubernetes via Helm.

* Table of Contents
{:toc}

## Prerequisites

- A Kubernetes cluster with version at least 1.19, with feature gate enabled.
- Cluster access to an Alluxio Docker image [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/).
If using a private Docker registry, refer to the Kubernetes private image registry
[documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).
- Ensure the cluster's [Kubernetes Network Policy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
allows for connectivity between applications (Alluxio clients) and the Alluxio Pods on the defined
ports.
- The control plane of the Kubernetes cluster has [helm 3](https://helm.sh/docs/intro/install/) installed.

## Deploy

Following the steps below to deploy Dora on Kubernetes:

### Step 1

Download the Helm chart source code [here](https://github.com/Alluxio/k8s-operator/deploy/charts/alluxio)
and enter the helm chart directory.

### Step 2

Configure [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) for:

1. (Required) Embedded journal.
2. (Optional) Worker page store. HostPath is also supported for worker storage.
3. (Optional) Worker metastore. Only required if you use RocksDB for storing metadata on workers.

Here is an example of a persistent volume of type hostPath for Alluxio embedded journal:
```yaml
kind: PersistentVolume
apiVersion: v1
metadata:
  name: alluxio-journal-0
  labels:
    type: local
spec:
  storageClassName: standard
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /tmp/alluxio-journal-0
```
Note:
- Each journal volume should have capacity at least requested by its corresponding persistentVolumeClaim,
configurable through the configuration file which will be talked in step 3.
- If using local hostPath persistent volume, make sure user alluxio has RWX permission.
  - Alluxio containers run as user `alluxio` of group `alluxio` with UID 1000 and GID 1000 by default. 

### Step 3

Prepare a configuration file `config.yaml`.
All configurable properties can be found in file `values.yaml` from the code downloaded in step 1.

You must specify your dataset configurations to enable Dora in your `config.yaml`.
More specifically, the following section:
```yaml
## Dataset ##

dataset:
  # The path of the dataset. For example, s3://my-bucket/dataset
  path:
  # Any credentials for Alluxio to access the dataset. For example,
  # credentials:
  #   aws.accessKeyId: XXXXX
  #   aws.secretKey: xxxx
  credentials:
```

### Step 4
Install Dora cluster by running 
```console
$ helm install dora -f config.yaml .
```
Wait until the cluster is ready. You can check pod status and container readiness by running 
```console
$ kubectl get po
```

## Verify
You could Alluxio CLI to read part of UFS data to verify Dora is working as expected:
```console
$ kubectl exec -it dora-alluxio-master-0 -- bash
$ alluxio fs head <path-to-a-UFS-file>
```

## Uninstall
Uninstall Dora cluster as follows:
```console
$ helm delete dora
```
