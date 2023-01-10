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
helm install --namespace "alluxio" "alluxio" alluxio
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

Please refer [https://docs.alluxio.io/os/user/edge/en/kubernetes/Running-Alluxio-On-Kubernetes.html](https://docs.alluxio.io/os/user/edge/en/kubernetes/Running-Alluxio-On-Kubernetes.html) for the configurations.

## Persistence

The [Alluxio](https://hub.docker.com/r/alluxio/alluxio) image stores the Journal data at the `/journal` path of the container.

A Persistent Volume Claim is created for each master Pod defined in `volumeClaimTemplates` in `master/statefulset.yaml`.
The PVC should be satisfied by the PersistentVolume you create.

## Helm Chart Values

Full documentation can be found in the comments of the `values.yaml` file, but a high level overview is provided here.

__Common Values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `fullnameOverride` | To replace the generated name | `alluxio` |
| `image` | Docker image | `alluxio/alluxio` |
| `imageTag` | Docker image tag | `2.7.0-SNAPSHOT` |
| `imagePullPolicy` | Docker image pull policy | `IfNotPresent` |
| `user` | Security Context for user | `1000` |
| `group` | Security Context for group | `1000` |
| `fsGroup` | Security Context for fsGroup | `1000` |
| `secrets` | Format: (<name>:<mount path under /secrets/>) | `{}` |
| `secrets.master` | Shared by master and jobMaster containers | `{}` |
| `secrets.worker` | Shared by worker and jobWorker containers | `{}` |
| `properties` | Site properties for all the components | `{}` |
| `jvmOptions` | Recommended JVM Heap options for running in Docker. These JVM options are common to all Alluxio services | `[]` |
| `mounts` | Mount Persistent Volumes to all components | `[]` |
| `nodeSelector` | Use labels to run Alluxio on a subset of the K8s nodes | `{}` |

__Master values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `count` | Controls the number of StatefulSets. For multiMaster mode increase this to >1. | `1` |
| `replicas` | Controls #replicas in a StatefulSet and should not be modified in the usual case. | `1` |
| `env` | Extra environment variables for the master pod | `{}` |
| `args` | Arguments to Docker entrypoint | - master-only <br/> - --no-format |
| `properties` | Properties for the master component | `{}` |
| `resources.limits.cpu` | CPU Limit | `4` |
| `resources.limits.memory` | Memory Limit | `8G` |
| `resources.requests.cpu` | CPU Request | `1` |
| `resources.requests.memory` | Memory Request | `1G` |
| `ports.embedded` | EMBEDDED JOURNAL Port | `19200` |
| `ports.rpc` | RPC Port | `19998` |
| `ports.web` | Web Port | `19999` |
| `hostPID` | hostPID requires escalated privileges for using Java profile | `false` |
| `hostNetwork` | Controls whether the pod may use the node network namespace | `false` |
| `dnsPolicy` | `dnsPolicy` will be `ClusterFirstWithHostNet` if `hostNetwork: true` and `ClusterFirst` if `hostNetwork: false` | `ClusterFirst` |
| `jvmOptions` | JVM options specific to the master container | `[]` |
| `nodeSelector` | the nodeSelector configs for the master Pods | `{}` |
| `podAnnotations` | Pod Annotations for the masters | `{}` |

__jobMaster values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `args` | Arguments to Docker entrypoint | `- job-master` |
| `properties` | Properties for the jobMaster component | `{}` |
| `resources.limits.cpu` | CPU Limit | `4` |
| `resources.limits.memory` | Memory Limit | `8G` |
| `resources.requests.cpu` | CPU Request | `1` |
| `resources.requests.memory` | Memory Request | `1G` |
| `ports.embedded` | EMBEDDED JOURNAL Port | `20003` |
| `ports.rpc` | RPC Port | `20001` |
| `ports.web` | Web Port | `20002` |
| `jvmOptions` | JVM options specific to the jobMaster container | `[]` |

__Worker values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `env` | Extra environment variables for the worker pod | `{}` |
| `args` | Arguments to Docker entrypoint | - worker-only <br/> - --no-format |
| `properties` | Properties for the worker component | `{}` |
| `resources.limits.cpu` | CPU Limit | `4` |
| `resources.limits.memory` | Memory Limit | `4G` |
| `resources.requests.cpu` | CPU Request | `1` |
| `resources.requests.memory` | Memory Request | `2G` |
| `ports.rpc` | RPC Port | `29999` |
| `ports.web` | Web Port | `30000` |
| `hostPID` | hostPID requires escalated privileges for using Java profile | `false` |
| `hostNetwork` | Controls whether the pod may use the node network namespace | `false` |
| `dnsPolicy` | `dnsPolicy` will be `ClusterFirstWithHostNet` if `hostNetwork: true` and `ClusterFirst` if `hostNetwork: false` | `ClusterFirst` |
| `jvmOptions` | JVM options specific to the worker container | `[]` |
| `nodeSelector` | the nodeSelector configs for the worker Pods | `{}` |
| `podAnnotations` | Pod Annotations for the worker | `{}` |

__jobWorker values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `args` | Arguments to Docker entrypoint | `- job-worker` |
| `properties` | Properties for the jobWorker component | `{}` |
| `resources.limits.cpu` | CPU Limit | `4` |
| `resources.limits.memory` | Memory Limit | `4G` |
| `resources.requests.cpu` | CPU Request | `1` |
| `resources.requests.memory` | Memory Request | `1G` |
| `ports.rpc` | RPC Port | `30001` |
| `ports.data` | Data Port | `30002` |
| `ports.web` | Web Port | `30003` |
| `jvmOptions` | JVM options specific to the jobWorker container | `[]` |

__journal values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `type` | Alluxio supports journal type of `UFS` and `EMBEDDED` | `UFS` |
| `ufsType` | `local` or `HDFS`. Ignored if type is `EMBEDDED` | `local` |
| `folder` | Master journal folder | `/journal` |
| `volumeType` | `volumeType` controls the type of journal volume. It can be `persistentVolumeClaim` or `emptyDir` | `persistentVolumeClaim` |
| `size` | journal volume size | `1Gi` |
| `storageClass` | Attributes to use when the journal is `persistentVolumeClaim` | `standard` |
| `accessModes` | Access mode for the journal volume | `- ReadWriteOnce` |
| `medium` | Attributes to use when the journal is emptyDir | `""` |
| `format.runFormat` | Configuration for journal formatting job. Change to true to format journal | `false` |

__metastore values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `volumeType` | When metastore is enabled, Alluxio uses RocksDB to store metadata off heap. This controls what kind of volume to use for the off heap metastore. `persistentVolumeClaim` or `emptyDir` | `persistentVolumeClaim` |
| `size` | Volume Size | `1Gi` |
| `mountPath` | Mount Path | `/metastore` |
| `storageClass` | Attributes to use when the `metastore` is `persistentVolumeClaim` | `standard` |
| `accessModes` | Access Mode for Volume | `- ReadWriteOnce` |
| `medium` | Attributes to use when the `metastore` is `emptyDir` | `""` |

__tieredstore values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `levels` | Tiered Storage | `[]` |
| `levels.level` | Level # starts from 0 | `0` |
| `levels.alias` | Alias name of tiered storage | `MEM` |
| `levels.mediumtype` | `MEM`, `HDD`, `SSD` | `MEM` |
| `levels.type` | `hostPath`, `emptyDir` or `persistentVolumeClaim` | `emptyDir` |
| `levels.path` | Mount Path for Volume | `/dev/shm` |
| `levels.quota` | Volume Size | `1G` |
| `levels.high` | Upper usage bound | `0.95` |
| `levels.low` | Lower usage bound | `0.7` |

__shortCircuit values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `enabled` | Whether use short circuit or not | `true` |
| `policy` | The policy for short circuit can be `local` or `uuid` | `uuid` |
| `volumeType` | `volumeType` controls the type of `shortCircuit` volume. It can be `persistentVolumeClaim` or `hostPath` | `persistentVolumeClaim` |
| `size` | Volume Size | `1Mi` |
| `pvcName` | Attributes to use if the domain socket volume is PVC | `alluxio-worker-domain-socket` |
| `accessModes` | Access Mode | `- ReadWriteOnce` |
| `storageClass` | Storage Class | `standard` |
| `hostPath` | Attributes to use if the domain socket volume is `hostPath`. The `hostPath` directory to use | `/tmp/alluxio-domain` |

__fuse values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `env` | Extra environment variables for the fuse pod | `{}` |
| `image` | Docker image | `alluxio/alluxio` |
| `imageTag` | Docker image tag | `2.5.0-SNAPSHOT` |
| `imagePullPolicy` | Docker image pull policy | `IfNotPresent` |
| `enabled` | Set to true to deploy FUSE | `false` |
| `clientEnabled` | Set to true to deploy FUSE | `false` |
| `properties` | Properties for the fuse component | `{}` |
| `jvmOptions` | JVM options specific to the fuse container | `[]` |
| `hostNetwork` | Controls whether the pod may use the node network namespace | `true` |
| `hostPID` | hostPID requires escalated privileges for using Java profile | `true` |
| `dnsPolicy` | `dnsPolicy` will be `ClusterFirstWithHostNet` if `hostNetwork: true` and `ClusterFirst` if `hostNetwork: false` | `ClusterFirstWithHostNet` |
| `user` | Security Context for user | `0` |
| `group` | Security Context for group | `0` |
| `fsGroup` | Security Context for fsGroup | `0` |
| `args` | Arguments to Docker entrypoint | - fuse <br/> - --fuse-opts=allow_other |
| `mountPath` | Mount path in the host | `/mnt/alluxio-fuse` |
| `resources.limits.cpu` | CPU Limit | `4` |
| `resources.limits.memory` | Memory Limit | `4G` |
| `resources.requests.cpu` | CPU Request | `0.5` |
| `resources.requests.memory` | Memory Request | `1G` |
| `podAnnotations` | Pod Annotations for the FUSE pods | `{}` |

__Metrics values:__

| Parameter | Description | Default |
| --- | --- | --- |
| `enabled` | Enabling Alluxio Metrics | `false` |
| `ConsoleSink.enabled` | Enabling ConsoleSink | `false` |
| `ConsoleSink.period` | Poll period | `10` |
| `ConsoleSink.unit` | Unit of poll period | `seconds` |
| `CsvSink.enabled` | Enabling CsvSink | `false` |
| `CsvSink.period` | Poll period | `1` |
| `CsvSink.unit` | Unit of poll period | `seconds` |
| `CsvSink.directory` | Polling directory for CsvSink, ensure this directory exists! | `/tmp/alluxio-metrics` |
| `JmxSink.enabled` | Enabling JmxSink | `false` |
| `JmxSink.domain` | Jmx domain | `org.alluxio` |
| `GraphiteSink.enabled` | Enabling GraphiteSink | `false` |
| `GraphiteSink.host` | Hostname of Graphite server | `NONE` |
| `GraphiteSink.port` | Port of Graphite server | `NONE` |
| `GraphiteSink.period` | Poll period | `10` |
| `GraphiteSink.unit` | Unit of poll period | `seconds` |
| `GraphiteSink.prefix` | Prefix to prepend to metric name | `""` |
| `Slf4jSink.enabled` | Enabling Slf4jSink | `false` |
| `Slf4jSink.period` | Poll period | `10` |
| `Slf4jSink.unit` | Unit of poll period | `seconds` |
| `Slf4jSink.filterClass` | Contains all metrics | `null` |
| `Slf4jSink.filterRegex` | Contains all metrics | `null` |
| `PrometheusMetricsServlet.enabled` | Enabling PrometheusMetricsServlet | `false` |
| `podAnnotations` | Pod Annotations for the Prometheus | `{}` |
