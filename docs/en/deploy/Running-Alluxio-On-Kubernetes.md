---
layout: global
title: Deploy Alluxio on Kubernetes
nickname: Kubernetes
group: Install Alluxio
priority: 5
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run Alluxio
on Kubernetes using the specification included in the Alluxio Docker image or `helm`.

* Table of Contents
{:toc}

## Prerequisites

- A Kubernetes cluster (version >= 1.8). With the default specifications, Alluxio 
workers may use `emptyDir` volumes with a restricted size using the `sizeLimit`
parameter. This is an alpha feature in Kubernetes 1.8. Please ensure the feature is enabled.
- An Alluxio Docker image [alluxio/alluxio](https://hub.docker.com/r/alluxio/alluxio/). If using a
private Docker registry, refer to the Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).
- Alluxio workers use `hostNetwork` and `hostPath` volumes for locality scheduling and
short-circuit access respectively. Please ensure the security policy allows for provisioning these
resource types.
- Ensure the [Kubernetes Network Policy](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
allows for connectivity between applications (Alluxio clients) and the Alluxio Pods on the defined
ports.

## Basic Setup

This tutorial walks through a basic Alluxio setup on Kubernetes. Alluxio supports two methods of
installation on Kubernetes: either using [helm](https://helm.sh/docs/) charts or using `kubectl`.
When available, `helm` is the preferred way to install Alluxio. If `helm` is not available or if
additional deployment customization is desired, `kubectl` can be used directly using native
Kubernetes resource specifications.


### (Optional) Extract Kubernetes Specifications

If hosting a private `helm` repository or using native Kubernetes specifications,
extract the Kubernetes specifications required to deploy Alluxio from the Docker image.

```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/integration/kubernetes/ - > kubernetes.tar
$ docker rm -v $id 1>/dev/null
$ tar -xvf kubernetes.tar
$ cd kubernetes
```

### (Optional) Provision a Persistent Volume

Note: [Embedded Journal]({{ '/en/operation/Journal.html' | relativize_url }}#embedded-journal-configuration)
requires a Persistent Volume for each master Pod to be provisioned and is the preferred HA mechanism for
Alluxio on Kubernetes. The volume, once claimed, is persisted across restarts of the master process.

When using the [UFS Journal]({{ '/en/operation/Journal.html' | relativize_url }}#ufs-journal-configuration)
an Alluxio master can also be configured to use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
for storing the journal. If you are using UFS journal and use an external journal location like HDFS,
the rest of this section can be skipped.

There are multiple ways to create a PersistentVolume. This is an example which defines one with `hostPath`: 
```yaml
# Name the file alluxio-master-journal-pv.yaml
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
    - ReadWriteMany
  hostPath:
    path: /tmp/alluxio-journal-0
```
>Note: By default each journal volume should be at least 1Gi, because each Alluxio master Pod
will have one PersistentVolumeClaim that requests for 1Gi storage. You will see how to configure
the journal size in later sections.

Then create the persistent volume with `kubectl`:
```console
$ kubectl create -f alluxio-master-journal-pv.yaml
```

There are other ways to create Persistent Volumes as documented [here](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).

### Deploy Using `helm`

#### Prerequisites

A helm repo with the Alluxio helm chart must be available.

(Optional) To prepare a local helm repository:
```console
$ helm init
$ helm package helm-chart/alluxio/
$ mkdir -p helm-chart/charts/
$ cp alluxio-{{site.ALLUXIO_HELM_VERSION_STRING}}.tgz helm-chart/charts/
$ helm repo index helm-chart/charts/
$ helm serve --repo-path helm-chart/charts
$ helm repo add alluxio-local http://127.0.0.1:8879
$ helm repo update alluxio-local
```

#### Configuration

Once the helm repository is available, prepare the Alluxio configuration.
The minimal configuration must contain the under storage address:
```properties
properties:
  alluxio.master.mount.table.root.ufs: "<under_storage_address>"
```
> Note: The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.

To view the complete list of supported properties run the `helm inspect` command:
```console
$ helm inspect values alluxio-local/alluxio
```

The remainder of this section describes various configuration options with examples.

***Example: Amazon S3 as the under store***

To [mount S3]({{ '/en/ufs/S3.html' | relativize_url }}#root-mount-point) at the root of Alluxio
namespace specify all required properties as a key-value pair under `properties`.

```properties
properties:
  alluxio.master.mount.table.root.ufs: "s3a://<bucket>"
  alluxio.master.mount.table.root.option.aws.accessKeyId: "<accessKey>"
  alluxio.master.mount.table.root.option.aws.secretKey: "<secretKey>"
```

***Example: Single Master and Journal in a Persistent Volume***

The following configures [UFS Journal]({{ '/en/operation/Journal.html' | relativize_url }}#ufs-journal-configuration)
with a persistent volume claim `alluxio-pv-claim` mounted locally to the master Pod at location
`/journal`.

```properties
master:
  count: 1 # For multiMaster mode increase this to >1

journal:
  type: "UFS"
  ufsType: "local"
  folder: "/journal"
  pvcName: alluxio-pv-claim
  storageClass: "standard"
  size: 1Gi
```

***Example: HDFS as Journal***

First create secrets for any configuration required by an HDFS client. These are mounted under `/secrets`.
```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```

```properties
journal:
  type: "UFS"
  ufsType: "HDFS"
  folder: "hdfs://{$hostname}:{$hostport}/journal"

properties:
  alluxio.master.mount.table.root.ufs: "hdfs://<ns>"
  alluxio.master.journal.ufs.option.alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"

secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
```

***Example: Multi-master with Embedded Journal***

```properties
master:
  count: 3

journal:
  type: "EMBEDDED"
  folder: "/journal"
```

***Example: HDFS as the under store***

First create secrets for any configuration required by an HDFS client. These are mounted under `/secrets`.
```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```

```properties
properties:
  alluxio.master.mount.table.root.ufs: "hdfs://<ns>"
  alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
```

***Example: Off-heap Metastore Management***

The following configuration provisions an `emptyDir` volume with the specified configuration and
configures the Alluxio master to use the mounted directory for an on-disk RocksDB-based metastore.
```properties
properties:
  alluxio.master.metastore: ROCKS
  alluxio.master.metastore.dir: /metastore

master:
  metastore:
    medium: ""
    size: 1Gi
    mountPath: /metastore
```

> Limitation: Limits for the disk usage are not configurable as of now.

***Example: Multiple Secrets***

Multiple secrets can be mounted to both master and worker Pods.
The format for the section for each Pod is `<secretName>: <mountPath>`
```properties
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
```

***Examples: Alluxio Storage Management***

Alluxio manages local storage, including memory, on the worker Pods.
[Multiple-Tier Storage]({{ '/en/core-services/Caching.html#multiple-tier-storage' | relativize_url }})
can be configured using the following reference configurations.

**Memory Tier Only**

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM
    path: /dev/shm
    type: emptyDir
    high: 0.95
    low: 0.7
```

**Memory and SSD Storage in Multiple-Tiers**

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM
    path: /dev/shm
    type: hostPath
    high: 0.95
    low: 0.7
  - level: 1
    mediumtype: SSD
    path: /ssd-disk
    type: hostPath
    high: 0.95
    low: 0.7
```

> There 2 supported volume `type`: [hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) and [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)
`hostPath` volumes can only be used by the `root` user without resource limits.
[Local persistent volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local) instead of
`hostPath` must be configured manually as of now.

**Memory and SSD Storage in a Single-Tier**

```properties
tieredstore:
  levels:
  - level: 0
    mediumtype: MEM,SSD
    path: /dev/shm,/alluxio-ssd
    type: hostPath
    quota: 1GB,10GB
    high: 0.95
    low: 0.7
```

#### Install

Once the configuration is finalized in a file named `config.yaml`, install as follows:
```console
helm install --name alluxio -f config.yaml alluxio-local/alluxio --version {{site.ALLUXIO_HELM_VERSION_STRING}}
```

#### Uninstall

Uninstall Alluxio as follows:
```console
$ helm delete alluxio
```

### Deploy Using `kubectl`

#### Choose the Sample YAML Template

The specification directory contains a set of YAML templates for common deployment scenarios in
the sub-directories: *singleMaster-localJournal*, *singleMaster-hdfsJournal* and
*multiMaster-embeddedJournal*.
> *singleMaster* means the templates generate 1 Alluxio master process, while *multiMaster* means 3.
*embedded* and *ufs* are the 2 [journal modes]({{ '/en/operation/Journal.html' | relativize_url }}) that Alluxio supports.

- *singleMaster-localJournal* directory gives you the necessary Kubernetes ConfigMap, 1 Alluxio
master process and a set of Alluxio workers.
The Alluxio master writes journal to the journal volume requested by `volumeClaimTemplates`.
- *multiMaster-EmbeddedJournal* directory gives you the Kubernetes ConfigMap, 3 Alluxio masters and
a set of Alluxio workers.
Each Alluxio master writes journal to its own journal volume requested by `volumeClaimTemplates`.
- *singleMaster-hdfsJournal* directory gives you the Kubernetes ConfigMap, 1 Alluxio master with a
set of workers.
The journal is in a shared UFS location. In this template we use HDFS as the UFS.


#### Configuration

Once the deployment option is chosen, copy the template from the desired sub-directory:
```console
$ cp alluxio-configmap.yaml.template alluxio-configmap.yaml
```

Modify or add any configuration properties as required.
The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.
Add to `ALLUXIO_JAVA_OPTS`:
```properties
-Dalluxio.master.mount.table.root.ufs=<under_storage_address>
```

Note:
- Replace `<under_storage_address>` with the appropriate URI, for example s3://my-bucket.
If using an under storage which requires credentials be sure to specify those as well.
- When running Alluxio with host networking, the ports assigned to Alluxio services must
not be occupied beforehand.

Create a ConfigMap.
```console
$ kubectl create -f alluxio-configmap.yaml
```

#### Install

***Prepare the Specification***

Prepare the Alluxio deployment specs from the templates. Modify any parameters required, such as
location of the **Docker image**, and CPU and memory requirements for Pods.

For the master(s), create the `Service` and `StatefulSet`:
```console
$ mv master/alluxio-master-service.yaml.template master/alluxio-master-service.yaml
$ mv master/alluxio-master-statefulset.yaml.template master/alluxio-master-statefulset.yaml
```
> Note: `alluxio-master-statefulset.yaml` uses `volumeClaimTemplates` to define the journal volume
for each master if it needs one.

For the workers, create the `DaemonSet`:
```console
$ mv worker/alluxio-worker-daemonset.yaml.template worker/alluxio-worker-daemonset.yaml
```

Note: Please make sure that the version of the Kubernetes specification matches the version of the
Alluxio Docker image being used.

***(Optional) Remote Storage Access***

Additional steps may be required when Alluxio is connecting to storage hosts outside the
Kubernetes cluster it is deployed on. The remainder of this section explains how to configure the
connection to a remote HDFS accessible but not managed by Kubernetes.

**Step 1: Add `hostAliases` for your HDFS connection.**  Kubernetes Pods don't recognize network
hostnames that are not managed by Kubernetes (not a Kubernetes Service), unless if specified by
[hostAliases](https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/#adding-additional-entries-with-hostaliases).

For example if your HDFS service can be reached at `hdfs://<namenode>:9000` where `<namenode>` is a
hostname, you will need to add `hostAliases` in the `spec` for all Alluxio Pods creating a map from
hostnames to IP addresses.

```yaml
spec:
  hostAliases:
  - ip: "<namenode_ip>"
    hostnames:
    - "<namenode>"
```

For the case of a StatefulSet or DaemonSet as used in `alluxio-master-statefulset.yaml.template` and
`alluxio-worker-daemonset.yaml.template`, `hostAliases` section should be added to each section of
`spec.template.spec` like below.

```yaml
kind: StatefulSet
metadata:
  name: alluxio-master-0
spec:
  ...
  serviceName: "alluxio-master-0"
  replicas: 1
  template:
    metadata:
      labels:
        app: alluxio-master-0
    spec:
      hostAliases:
      - ip: "ip for hdfs-host"
        hostnames:
        - "hdfs-host"
```

**Step 2: Create Kubernetes Secret for HDFS configuration files.** Run the following command to
create a Kubernetes Secret for the HDFS client configuration.

```console
kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```
These two configuration files are referred in `alluxio-master-statefulset.yaml` and `alluxio-worker-daemonset.yaml`.
Alluxio processes need the HDFS configuration files to connect, and the location of these files in
the container is controlled by property `alluxio.underfs.hdfs.configuration`.

**Step 3: Modify `alluxio-configmap.yaml.template`.** Now that your Pods know how to talk to your
HDFS service, update `alluxio.master.journal.folder` and `alluxio.master.mount.table.root.ufs` to
point to the desired HDFS destination.

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```console
$ kubectl create -f ./master/
$ kubectl create -f ./worker/
```

#### Uninstall

Uninstall Alluxio as follows:
```console
$ kubectl delete -f ./worker/
$ kubectl delete -f ./master/
$ kubectl delete configmap alluxio-config
```
> Note: This will delete all resources under `./master/` and `./worker/`. 
Be careful if you have persistent volumes or other important resources you want to keep under those directories.  

#### Upgrade

This section will go over how to upgrade Alluxio in your Kubernetes cluster with `kubectl`.

**Step 1: Upgrade the docker image version tag**

Each released Alluxio version will have the corresponding docker image released on [dockerhub](https://hub.docker.com/r/alluxio/alluxio).

You should update the `image` field of all the Alluxio containers to use the target version tag. Tag `latest` will point to the latest stable version.
It is recommended that Alluxio masters and workers are running on the same version for the best compatibility.

For example, if you want to upgrade Alluxio to the latest stable version, update the containers as below:

```yaml
containers:
- name: alluxio-master
  image: alluxio/alluxio:latest
  imagePullPolicy: IfNotPresent
  ...
- name: alluxio-job-master
  image: alluxio/alluxio:latest
  imagePullPolicy: IfNotPresent
  ...
```

**Step 2: Stop running Alluxio master and worker Pods**

Kill all running Alluxio worker Pods by deleting its DaemonSet.

```console
$ kubectl delete daemonset -l app=alluxio
```

Then kill all running Alluxio master Pods by killing each StatefulSet and each Service with label `app=alluxio`.

```console
$ kubectl delete service -l app=alluxio
$ kubectl delete statefulset -l app=alluxio
```

Make sure all the Pods have been terminated before you move on to the next step.

**Step 3: Format journal and Alluxio storage if necessary**

Check the Alluxio upgrade guide page for whether the Alluxio master journal has to be formatted.
If no format is needed, you are ready to skip the rest of this section and move on to restart all Alluxio master and worker Pods.

There is a single Kubernetes [Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/)
template that can be used for only formatting the master.
The Job runs `alluxio formatMasters` and formats the journal for all masters.
You should make sure the Job runs with the same configMap with all your other Alluxio masters so it's able to find the journal persistent storage and format it.

```console
$ cp ./job/alluxio-format-journal-job.yaml.template ./job/alluxio-format-journal-job.yaml
$ kubectl apply -f ./job/alluxio-format-journal-job.yaml
```

After the Job completes, it will be deleted by Kubernetes after the defined `ttlSecondsAfterFinished`.
Then the clean journal will be ready for a new Alluxio master(s) to start with.

If you are running Alluxio workers with [tiered storage]({{ '/en/core-services/Caching.html#multiple-tier-storage' | relativize_url }}),
and you have Persistent Volumes configured for Alluxio, the storage should be cleaned up too.
You should delete and recreate the Persistent Volumes.

Once all the journals and Alluxio storage have been formatted, you are ready to restart the Alluxio master and worker Pods.

**Step 4: Restart Alluxio master and worker Pods**

Now that Alluxio masters and worker containers all use your desired version. Restart them to let it take effect.

Now restart the Alluxio master and worker Pods from the YAML files.

```console
$ kubectl create -f ./master/
$ kubectl create -f ./worker/
```

**Step 5: Verify the Alluxio master and worker Pods are back up**

You should verify the Alluxio Pods are back up in Running status.

```console
# You should see all Alluxio master and worker Pods
$ kubectl get pods
```

You can do more comprehensive verification following [Verify Alluxio]({{ '/en/deploy/Running-Alluxio-Locally.html?q=verify#verify-alluxio-is-running' | relativize_url }})

### Access the Web UI

The Alluxio UI can be accessed from outside the kubernetes cluster using port forwarding.
```console
$ kubectl port-forward alluxio-master-$i-0 19999:19999
```
Note: `i=0` for the the first master Pod. When running multiple masters, forward port for each
master. Only the primary master serves the Web UI.

### Verify

Once ready, access the Alluxio CLI from the master Pod and run basic I/O tests.
```console
$ kubectl exec -ti alluxio-master-0-0 /bin/bash
```

From the master Pod, execute the following:
```console
$ alluxio runTests
```

(Optional) If using persistent volumes for Alluxio master, the status of the volume(s) should change to
`CLAIMED`, and the status of the volume claims should be `BOUNDED`. You can validate the status as below:
```console
$ kubectl get pv
$ kubectl get pvc
```

## Advanced Setup

### POSIX API

Once Alluxio is deployed on Kubernetes, there are multiple ways in which a client application can
connect to it. For applications using the [POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url }}),
application containers can simply mount the Alluxio FileSystem.

In order to use the POSIX API, first deploy the Alluxio FUSE daemon.

***Using `kubectl`***
```console
$ cp alluxio-fuse.yaml.template alluxio-fuse.yaml
$ kubectl create -f alluxio-fuse.yaml
```
Note:
- The container running the Alluxio FUSE daemon must have the `securityContext.privileged=true` with
`SYS_ADMIN` capabilities.
Application containers that require Alluxio access do not need this privilege.
- A different Docker image [alluxio/alluxio-fuse](https://hub.docker.com/r/alluxio/alluxio-fuse/) based
on `ubuntu` instead of `alpine` is needed to run the FUSE daemon. Application containers can run on
any Docker image.

Verify that a container can simply mount the Alluxio FileSystem without any custom binaries or
capabilities using a `hostPath` mount of location `/alluxio-fuse`:
```console
$ cp alluxio-fuse-client.yaml.template alluxio-fuse-client.yaml
$ kubectl create -f alluxio-fuse-client.yaml
```

If using the template, Alluxio is mounted at `/alluxio-fuse` and can be accessed via the POSIX-API
across multiple containers.

## Troubleshooting

### Worker Host Unreachable

Alluxio workers use host networking with the physical host IP as the hostname. Check the cluster
firewall if an error such as the following is encountered:
```
Caused by: io.netty.channel.AbstractChannel$AnnotatedConnectException: finishConnect(..) failed: Host is unreachable: <host>/<IP>:29999
```

- Check that `<host>` matches the physical host address and is not a virtual container hostname.
Ping from a remote client to check the address is resolvable.
```console
$ ping <host>
```
- Verify that a client can connect to the workers on the ports specified in the worker
deployment specification. The default ports are `[29998, 29999, 29996, 30001, 30002, 30003]`.
Check access to the given port from a remote client using a network utility such as `ncat`:
```console
$ nc -zv <IP> 29999
```

### Permission Denied

From Alluxio v2.1 on, Alluxio Docker containers except Fuse will run as non-root user `alluxio` with UID 1000 and GID 1000 by default.
Kubernetes [`hostPath`](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) volumes
are only writable by root so you need to update the permission accordingly. 

### Enable Debug Logging

To change the log level for Alluxio servers (master and workers), use the CLI command `logLevel` as
follows:

Access the Alluxio CLI from the master Pod.
```console
$ kubectl exec -ti alluxio-master-0-0 /bin/bash
```

From the master Pod, execute the following:
```console
$ alluxio logLevel --level DEBUG --logName alluxio
```

### Accessing Logs

The Alluxio master and job master run as separate containers of the master Pod. Similarly, the
Alluxio worker and job worker run as separate containers of a worker Pod. Logs can be accessed for
the individual containers as follows.

Master:
```console
$ kubectl logs -f alluxio-master-0-0 -c alluxio-master
```

Worker:
```console
$ kubectl logs -f alluxio-worker-<id> -c alluxio-worker
```

Job Master:
```console
$ kubectl logs -f alluxio-master-0 -c alluxio-job-master
```

Job Worker:
```console
$ kubectl logs -f alluxio-worker-<id> -c alluxio-job-worker
```

### Short-circuit Access

Short-circuit access enables clients to perform read and write operations directly against the
worker bypassing the networking interface.
For performance-critical applications it is recommended to enable short-circuit operations
against Alluxio because it can increase a client's read and write throughput when co-located with
an Alluxio worker.

***Properties to Enable Short-Circuit Operations***

This feature is enabled by default, however requires extra configuration to work properly in
Kubernetes environments.
To disable short-circuit operations, set the property `alluxio.user.short.circuit.enabled=false`.

**Hostname Introspection:** Short-circuit operations between the Alluxio client and worker are
enabled if the client hostname matches the worker hostname.
This may not be true if the client is running as part of a container with virtual networking.
In such a scenario, set the following property to use filesystem inspection to enable short-circuit
operations and **make sure the client container mounts the directory specified as the domain socket
path**.
Short-circuit writes are then enabled if the worker UUID is located on the client filesystem.

> Note: This property should be set on all workers

```properties
alluxio.worker.data.server.domain.socket.as.uuid=true
```

**Domain Socket Path:** The domain socket is a volume which should be mounted on:

- All Alluxio workers
- All application containers which intend to read/write through Alluxio

The exact path of the domain socket on the host is defined in the helm chart at
`${ALLUXIO_HOME}/integration/kubernetes/helm/alluxio/values.yml`.
On the worker the path where the domain socket is mounted can be found within
`${ALLUXIO_HOME}/integration/kubernetes/helm/alluxio/templates/worker/daemonset.yaml`

As part of the Alluxio worker Pod creation, a directory
is created on the host at `/tmp/alluxio-domain` for the shared domain socket.
The workers then mount `/tmp/alluxio-domain` to `/opt/domain` within the
container.

```properties
alluxio.worker.data.server.domain.socket.address=/opt/domain
```

Compute application containers **must** mount the domain socket volume to the same path
(`/opt/domain`) as configured for the Alluxio workers.

***Verify***

To verify short-circuit reads and writes monitor the metrics displayed under:
1. the metrics tab of the web UI as `Domain Socket Alluxio Read` and `Domain Socket Alluxio Write`
1. or, the [metrics json]({{ '/en/operation/Metrics-System.html' | relativize_url }}) as
`cluster.BytesReadDomain` and `cluster.BytesWrittenDomain`
1. or, the [fsadmin metrics CLI]({{ '/en/operation/Admin-CLI.html' | relativize_url }}) as
`Short-circuit Read (Domain Socket)` and `Alluxio Write (Domain Socket)`

### POSIX API

In order for an application container to mount the `hostPath` volume, the node running the container
must have the Alluxio FUSE daemon running. The default spec `alluxio-fuse.yaml` runs as a DaemonSet,
launching an Alluxio FUSE daemon on each node of the cluster.

If there are issues accessing Alluxio using the POSIX API:
1. First identify which node the application container ran on using the command
`kubectl describe pods` or the dashboard.
1. After the node is identified, the command `kubectl describe nodes <node>` can be used to identify
the `alluxio-fuse` Pod running on that node.
1. Then tail the logs for the identified Pod to see if there were any errors encountered using
`kubectl logs -f alluxio-fuse-<id>`.
