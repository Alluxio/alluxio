---
layout: global
title: Install Alluxio on Kubernetes
---

This documentation shows how to install Alluxio (Dora) on Kubernetes via 
[Helm](https://helm.sh/), a kubernetes package manager, and 
[Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/),
a kubernetes extension for managing applications.

We recommend using the operator to deploy Alluxio on Kubernetes. However, 
if some required permissions are missing, consider using helm chart instead.


<iframe width="425" height="239" src="https://www.youtube.com/embed/FlvbekK_xG0" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

<iframe width="425" height="239" src="https://www.youtube.com/embed/zwhMwiYmO8M" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>

## Prerequisites

- A Kubernetes cluster with version at least 1.19, with feature gate enabled.
- Cluster access to an Alluxio Docker image [alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}](https://hub.docker.com/r/alluxio/{{site.ALLUXIO_DOCKER_IMAGE}}/).
If using a private Docker registry, refer to the Kubernetes private image registry
[documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).
- Ensure the cluster's [Kubernetes Network Policy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
allows for connectivity between applications (Alluxio clients) and the Alluxio Pods on the defined
ports.
- The control plane of the Kubernetes cluster has [helm 3](https://helm.sh/docs/intro/install/) with version at least 3.6.0 installed.

## Operator

### Extra prerequisites for Operator
You will need certain RBAC permission in the Kubernetes cluster to make Operator to work.
1. Permission to create CRD (Custom Resource Definition);
2. Permission to create ServiceAccount, ClusterRole, and ClusterRoleBinding for the operator pods;
3. Permission to create namespace that the operator will be in.


### Deploy Alluxio Kubernetes Operator
We use the Helm Chart for Alluxio K8s Operator for deploying.
Following the steps below to deploy Alluxio Operator:

#### 1. Download Alluxio Kubernetes Operator

Download the Alluxio Kubernetes Operator
[here](https://github.com/Alluxio/k8s-operator) and enter the root directory of the project.

#### 2. Install Operator

Install the operator by running:
```shell
$ helm install operator ./deploy/charts/alluxio-operator
```
Operator will automatically create namespace `alluxio-operator` and install
all the components there.

#### 3. Run Operator

Make sure the operator is running as expected:
```shell
$ kubectl get pods -n alluxio-operator
```

### Deploy Dataset

#### 1. Create Dataset Configuration

Create a dataset configuration `dataset.yaml`. Its `apiVersion` must be 
`k8s-operator.alluxio.com/v1alpha1` and `kind` must be `Dataset`. Here is an example:
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: Dataset
metadata:
  name: my-dataset
spec:
  dataset:
    path: <path of your dataset>
    credentials:
      - <property 1 for accessing your dataset>
      - <property 2 for accessing your dataset>
      - ...
```

#### 2. Deploy Dataset

Deploy your dataset by running 
```shell
$ kubectl create -f dataset.yaml
```

#### 3. Check Status of Dataset

Check the status of the dataset by running 
```shell
$ kubectl get dataset <dataset-name>
```

### Deploy Alluxio

#### 1. Configure Persistent Volumes

Configure [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) for:

1. (Optional) Embedded journal. HostPath is also supported for embedded journal.
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
- If using hostPath as volume for embedded journal, Alluxio will run an init container as root to grant RWX
permission of the path for itself.
- Each journal volume should have capacity at least requested by its corresponding persistentVolumeClaim,
configurable through the configuration file which will be talked in step 2.
- If using local hostPath persistent volume, make sure user alluxio has RWX permission.
  - Alluxio containers run as user `alluxio` of group `alluxio` with UID 1000 and GID 1000 by default.

#### 2. Prepare Resource Configuration File

Prepare a resource configuration file `alluxio-config.yaml`. Its `apiVersion` must be
`k8s-operator.alluxio.com/v1alpha1` and `kind` must be `AlluxioCluster`. Here is an example:
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: AlluxioCluster
metadata:
  name: my-alluxio-cluster
spec:
  <configurations>
```

`Dataset` in which the name of your dataset is **required** in the `spec` section. 

All other configurable properties in the `spec` section can be found in `deploy/charts/alluxio/values.yaml`.

#### 3. Deploy Alluxio Cluster

Deploy Alluxio cluster by running:
```shell
$ kubectl create -f alluxio-config.yaml
```

#### 4. Check Status of Alluxio Cluster

Check the status of Alluxio cluster by running:
```shell
$ kubectl get alluxiocluster <alluxio-cluster-name>
```

### Uninstall Dataset + Alluxio

Run the following command to uninstall Dataset and Alluxio cluster:
```shell
$ kubectl delete dataset <dataset-name>
$ kubectl delete alluxiocluster <alluxio-cluster-name>
```

### Bonus - Load the data into Alluxio

To load your data into Alluxio cluster, so that your application can read the data faster, create a
resource file `load.yaml`. Here is an example:
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: Load
metadata:
  name: my-load
spec:
  dataset: <dataset-name>
  path: /
```

Then run the following command to start the load:
```shell
$ kubectl create -f load.yaml 
```

To check the status of the load:
```shell
$ kubectl get load
```

### More bonus - an example alluxio cluster configuration for AI/ML use case
```yaml
apiVersion: k8s-operator.alluxio.com/v1alpha1
kind: AlluxioCluster
metadata:
  name: my-alluxio-cluster
spec:
  worker:
    count: 4
  pagestore:
    type: hostPath
    quota: 512Gi
    hostPath: /mnt/alluxio
  csi:
    enabled: true
```

## Helm

### Deploy Alluxio

Following the steps below to deploy Dora on Kubernetes:

#### 1. Download Helm Chart

Download the Helm chart [here](https://github.com/Alluxio/k8s-operator/deploy/charts/alluxio)
and enter the helm chart directory.

#### 2. Configure Persistent Volumes

Configure [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) for:

1. (Optional) Embedded journal. HostPath is also supported for journal storage.
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
- If using hostPath as volume for embedded journal, Alluxio will run an init container as root to grant RWX
permission of the path for itself.
- Each journal volume requires at least the storage of its corresponding persistentVolumeClaim,
configurable through the configuration file which will be talked in step 3.
- If using local hostPath persistent volume, make sure the user of UID 1000 and GID 1000 has RWX permission.
  - Alluxio containers run as user `alluxio` of group `alluxio` with UID 1000 and GID 1000 by default.

#### 3. Prepare Configuration File

Prepare a configuration file `config.yaml`.
All configurable properties can be found in file `values.yaml` from the code downloaded in step 1.

You **MUST** specify your dataset configurations to enable Dora in your `config.yaml`.
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

#### 4. Install Dora Cluster

Install Dora cluster by running 
```shell
$ helm install dora -f config.yaml .
```
Wait until the cluster is ready. You can check pod status and container readiness by running 
```shell
$ kubectl get po
```

### Uninstall

Uninstall Dora cluster as follows:
```shell
$ helm delete dora
```