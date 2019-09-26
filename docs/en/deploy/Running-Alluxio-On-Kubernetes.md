---
layout: global
title: Deploy Alluxio on Kubernetes
nickname: Kubernetes
group: Deploying Alluxio
priority: 3
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run Alluxio
on Kubernetes using the specification included in the Alluxio Docker image or `helm`.

* Table of Contents
{:toc}

## Prerequisites

- A Kubernetes cluster (version >= 1.8). With the default specifications, Alluxio services (both
masters and workers may use `emptyDir` volumes with a restricted size using the `sizeLimit`
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
does not require a Persistent Volume to be provisioned and is the preferred HA mechanism for
Alluxio on Kubernetes. If using this mechanism, the remainder of this section can be skipped.

When using the [UFS Journal]({{ '/en/operation/Journal.html' | relativize_url }}#ufs-journal-configuration}})
an Alluxio master can be configured to use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
for storing the journal. The volume, once claimed, is persisted across restarts of the master process.

Create the persistent volume spec from the template. The access mode `ReadWriteMany` is used to allow
multiple Alluxio master nodes to access the shared volume. When deploying a single master, the mode
can be changed to `ReadWriteOnce`.

```console
$ cp alluxio-journal-volume.yaml.template alluxio-journal-volume.yaml
```

Note: the spec provided uses a `hostPath` volume for demonstration on a single-node deployment. For a
multi-node cluster, you may choose to use NFS, AWSElasticBlockStore, GCEPersistentDisk or other available
persistent volume plugins.

Create the persistent volume.
```console
$ kubectl create -f alluxio-journal-volume.yaml
```

### Deploy Using `helm`

#### Prerequisites

A helm repo with the Alluxio helm chart must be available.
The offical public helm repo can install Alluxio from [stable/alluxio](https://github.com/helm/charts/tree/master/stable/alluxio).

(Optional) To prepare a local helm repository:
```console
$ helm init
$ helm package helm/alluxio/
$ mkdir -p helm/charts/
$ cp alluxio-{{site.ALLUXIO_VERSION_STRING}}.tgz helm/charts/
$ helm repo index helm/charts/
$ helm serve --repo-path helm/charts
$ helm repo add alluxio-local http://127.0.0.1:8879
$ helm repo update alluxio-local
```

#### Configuration

Once the helm repository is available, prepare the Alluxio configuration:
```console
$ cat << EOF > config.yaml
properties:
  alluxio.master.mount.table.root.ufs: "<under_storage_address>"
EOF
```
Note: The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.

***Example: Amazon S3 as the under store***
```console
$ cat << EOF > config.yaml
properties:
  alluxio.master.mount.table.root.ufs: "s3a://<bucket>"
  aws.accessKeyId: "<accessKey>"
  aws.secretKey: "<secretKey>"
EOF
```

The remainder of this section describes various configuration options with examples.

***Example: HDFS as the under store***

First create secrets for any configuration required by an HDFS client. These are mounted under `/secrets`.
```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=${HADOOP_CONF_DIR}/core-site.xml --from-file=${HADOOP_CONF_DIR}/hdfs-site.xml
```
Then mount these secrets to the Alluxio master and worker containers as follows:
```console
$ cat << EOF > config.yaml
properties:
  alluxio.master.mount.table.root.ufs: "hdfs://<ns>"
  alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig
EOF
```
Note: Multiple secrets can be mounted by adding more rows to the configuration such as:
```console
$ cat << EOF > config.yaml
...
secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
    ...
  worker:
    alluxio-hdfs-config: hdfsConfig
    alluxio-ceph-config: cephConfig
    ...
EOF
```

***Example: Off-heap Metastore Management***

The following configuration provisions an emptyDir volume with the specified configuration and
configures the Alluxio master to use the mounted directory for the RocksDB metastore.
```console
$ cat << EOF > config.yaml
properties:
  ...
  alluxio.master.metastore: ROCKS
  alluxio.master.metastore.dir: /metastore

volumes:
  master:
    metastore:
      medium: ""
      size: 1Gi
      mountPath: /metastore
EOF
```

#### Install

Once the configuration is finalized, install as follows:
```console
helm install --name alluxio -f config.yaml alluxio-local/alluxio --version {{site.ALLUXIO_VERSION_STRING}}
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
- *singleMaster* means the templates generate 1 Alluxio master process, while *multiMaster* means 3.
*embedded* and *ufs* are the 2 [journal modes]({{ '/en/operation/Journal.html' | relativize_url }}) that Alluxio supports.
- *singleMaster-localJournal* directory gives you the necessary Kubernetes ConfigMap, 1 Alluxio master process and a set of Alluxio workers.
The Alluxio master writes journal to the PersistentVolume defined in *alluxio-journal-volume.yaml.template*.
- *multiMaster-EmbeddedJournal* directory gives you the Kubernetes ConfigMap, 3 Alluxio masters and a set of Alluxio workers.
The Alluxio masters each write to its `alluxio-journal-volume`, which is an `emptyDir` that gets wiped out when the Pod is shut down.
- *singleMaster-hdfsJournal* directory gives you the Kubernetes ConfigMap, 3 Alluxio masters with a set of workers.
The journal is in a shared UFS location. In this template we use HDFS as the UFS.

Note: The default templates (one-level above the mentioned sub-directories) are just symbolic links
to the specifications found in *singleMaster-localJournal*.

#### Configuration

Once the deployment option is chosen, copy the template:
```console
$ cp alluxio-configMap.yaml.template alluxio-configMap.yaml
```

Modify or add any configuration properties as required.
The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.

```
# Replace <under_storage_address> with the appropriate URI, for example s3://my-bucket
# If using an under storage which requires credentials be sure to specify those as well
ALLUXIO_JAVA_OPTS=-Dalluxio.master.mount.table.root.ufs=<under_storage_address>
```

Note that when running Alluxio with host networking, the ports assigned to Alluxio services must
not be occupied beforehand.

Create a ConfigMap.
```console
$ kubectl create -f alluxio-configMap.yaml
```

#### Install

Prepare the Alluxio deployment specs from the templates. Modify any parameters required, such as
location of the **Docker image**, and CPU and memory requirements for pods.
```console
$ cp alluxio-master.yaml.template alluxio-master.yaml
$ cp alluxio-worker.yaml.template alluxio-worker.yaml
```
Note: Please make sure that the version of the Kubernetes specification matches the version of the
Alluxio Docker image being used.

***Remote Storage Access***

(Optional) Additional steps are required when Alluxio is connecting to storage hosts outside the
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

For the case of a StatefulSet or DaemonSet as used in `alluxio-master.yaml.template` and
`alluxio-worker.yaml.template`, `hostAliases` section should be added to each section of
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
These two configuration files are referred in `alluxio-master.yaml` and `alluxio-worker.yaml`.
Alluxio processes need the HDFS configuration files to connect, and the location of these files in
the container is controlled by property `alluxio.underfs.hdfs.configuration`.

**Step 3: Modify `alluxio-configMap.yaml.template`.** Now that your pods know how to talk to your
HDFS service, update `alluxio.master.journal.folder` and `alluxio.master.mount.table.root.ufs` to
point to the desired HDFS destination.

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```console
$ kubectl create -f alluxio-master.yaml
$ kubectl create -f alluxio-worker.yaml
```

#### Uninstall

Uninstall Alluxio as follows:
```console
$ kubectl delete -f alluxio-worker.yaml
$ kubectl delete -f alluxio-master.yaml
$ kubectl delete configmaps alluxio-config
```

Execute the following to remove the persistent volume storing the Alluxio journal. Note: Alluxio metadata
will be lost.
```console
$ kubectl delete -f alluxio-journal-volume.yaml
```

#### Upgrade Alluxio

This section will go over how to upgrade Alluxio in your Kubernetes cluster with `kubectl`.
This section will only cover the basic procedure of how to kill all existing Pods, upgrade the version and bring the Alluxio master and worker Pods back up.
More advanced rolling update procedure will be covered in the next section. 

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

**Step 2: Format journal if necessary**

Note that whether you need to re-format the journal and lose all file metadata depends on whether the journals are compatible.
If you are upgrading from v1.8 to v2.x, you have to format the journal. And by doing that you lose all the journal and file metadata.
If you are upgrading from v2.x to a higher minor version v2.y, or from v2.x.y to a higher minor version v2.x.z,
the higher version Alluxio master will be able to read the lower version Alluxio master journal to recover all the journal and file metadata.

If you need to format the journal, you can update the YAML file for Alluxio master containers to specify this on master startup time.
Remove the `--no-format` option from Alluxio master startup arguments to force the re-format of journal as illustrated below.

```yaml
containers:
- name: alluxio-master
  command: ["/entrypoint.sh"]
  # --no-format removed from the args
  args: ["master-only"]
  ...
```

**Step 3: Restart Alluxio master and worker Pods**

Now that Alluxio masters and worker containers all use your desired version. Restart them to let it take effect.

Kill the running Alluxio master and worker Pods first.

```console
$ kubectl delete -f alluxio-worker.yaml
$ kubectl delete service alluxio-master-0
$ kubeclt delete statefulset alluxio-master-0
```

If you have more alluxio-master Pods, delete them together with the corresponding Services.

```console
$ kubectl delete service alluxio-master-1
$ kubeclt delete statefulset alluxio-master-1
$ kubectl delete service alluxio-master-2
$ kubeclt delete statefulset alluxio-master-2
```

The reason why you don't want to do `kubectl delete -f alluxio-master.yaml` is that will delete the Persistent Volume Claim in the `alluxio-master.yaml` if any.
And that will result in your Persistent Volume getting released and the journal in it will be lost.

Now restart the Alluxio master and worker Pods from the YAML files.

```console
$ kubectl create -f alluxio-master.yaml
$ kubectl create -f alluxio-worker.yaml
```

**Step 4: Verify the Alluxio master and worker Pods are back up**

What a proper validation should be varies from case to case.
The example is just verifying that Alluxio is running.

```console
# You should see Alluxio master and worker Pods in Running status
$ kubectl get pods
# This runs a comprehensive sanity check
$ kubectl exec <alluxio-master-pod> -- /opt/alluxio/bin/alluxio runTests
```

#### Rolling Update Alluxio

In a production environment, it is often more desirable to do rolling updates to maintain the availability.

How the rolling update should be done depends on the journal type of Alluxio masters are configured and what storage Alluxio workers are using.
We will discuss the scenarios separately.

##### Alluxio Masters

The behavior of Alluxio masters will differ depends on which journal mode they are using. 
See the detailed discussion in [UFS Journal vs Embedded Journal]({{ '/en/operation/Journal.html#ufs-journal-vs-embedded-journal' | relativize_url }})

###### Case A: Rolling Update with Embedded Journal

We do not recommend doing rolling update with embedded journal. 

The reason is Alluxio masters using embedded journal have to reach a consensus before each journaling to its own journal destination.
The consensus logic of Alluxio masters can be changed in minor versions so if you have a group of Alluxio masters running different versions,
the behavior will be undefined. 

###### Case B: Rolling Update with UFS Journal

With UFS journal, there is only one shared journal destination and only the one primary master writes. All the secondary masters only read from it.

You are able to do rolling update among Alluxio masters as below.

**Step 1: Update the secondary master Pods and restart them**

**Step 2: Update the primary master Pods and restart them**

In this way no lower version Alluxio master will read the journal written by a higher version master.

##### Alluxio Workers 

The rolling update of Alluxio workers can go parallel to that of Alluxio masters.
Since Alluxio workers only communicate to masters, they can be updated individually.

### Access the Web UI

The Alluxio UI can be accessed from outside the kubernetes cluster using port forwarding.
```console
$ kubectl port-forward alluxio-master-0 19999:19999
```

### Verify

Once ready, access the Alluxio CLI from the master pod and run basic I/O tests.
```console
$ kubectl exec -ti alluxio-master-0 /bin/bash
```

From the master pod, execute the following:
```console
$ cd /opt/alluxio
$ ./bin/alluxio runTests
```

(Optional) If using peristent volumes for Alluxio master, the status of the volume should change to
`CLAIMED`.
```console
$ kubectl get pv alluxio-journal-volume
```

## Advanced Setup

### POSIX API

Once Alluxio is deployed on Kubernetes, there are multiple ways in which a client application can
connect to it. For applications using the [POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url }}),
application containers can simply mount the Alluxio FileSystem.

In order to use the POSIX API, first deploy the Alluxio FUSE daemon.
```console
$ cp alluxio-fuse.yaml.template alluxio-fuse.yaml
$ kubectl create -f alluxio-fuse.yaml
```
Note:
- The container running the Alluxio FUSE daemon must have the `securityContext.privileged=true` with
SYS_ADMIN capabilities. Application containers that require Alluxio access do not need this privilege.
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

### Enable Debug Logging

To change the log level for Alluxio servers (master and workers), use the CLI command `logLevel` as
follows:

Access the Alluxio CLI from the master pod.
```console
$ kubectl exec -ti alluxio-master-0 /bin/bash
```

From the master pod, execute the following:
```console
$ cd /opt/alluxio
$ ./bin/alluxio logLevel --level DEBUG --logName alluxio
```

### Accessing Logs

The Alluxio master and job master run as separate containers of the master pod. Similarly, the
Alluxio worker and job worker run as separate containers of a worker pod. Logs can be accessed for
the individual containers as follows.

Master:
```console
$ kubectl logs -f alluxio-master-0 -c alluxio-master
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

#### Properties to Enable Short-Circuit Operations

This feature is enabled by default, however requires extra configuration to work properly in
Kubernetes environments.
To disable short-circuit operations, set the property `alluxio.user.short.circuit.enabled=false`.

***Hostname Introspection***

Short-circuit operations between the Alluxio client and worker are enabled if the client hostname
matches the worker hostname.
This may not be true if the client is running as part of a container with virtual networking.
In such a scenario, set the following property to use filesystem inspection to enable short-circuit
operations and **make sure the client container mounts the directory specified as the domain socket
path**.
Short-circuit writes are then enabled if the worker UUID is located on the client filesystem.

> Note: This property should be set on all workers

```properties
alluxio.worker.data.server.domain.socket.as.uuid=true
```

***Domain Socket Path***

The domain socket is a volume which should be mounted on:

- All Alluxio workers
- All application containers which intend to read/write through Alluxio

The exact path of the domain socket on the host is defined in the helm chart at
`${ALLUXIO_HOME}/integration/kubernetes/helm/alluxio/values.yml`.
On the worker the path where the domain socket is mounted can be found within
`${ALLUXIO_HOME}/integration/kubernetes/helm/alluxio/templates/alluxio-worker.yml`

As part of the Alluxio worker pod creation, a directory
is created on the host at `/tmp/alluxio-domain` for the shared domain socket.
The workers then mount `/tmp/alluxio-domain` to `/opt/domain` within the
container.

```properties
alluxio.worker.data.server.domain.socket.address=/opt/domain
```

Compute application containers **must** mount the domain socket volume to the same path
(`/opt/domain`) as configured for the Alluxio workers.

#### Verify

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
the `alluxio-fuse` pod running on that node.
1. Then tail the logs for the identified pod to see if there were any errors encountered using
`kubectl logs -f alluxio-fuse-<id>`.
