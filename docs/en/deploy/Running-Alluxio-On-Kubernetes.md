---
layout: global
title: Deploy Alluxio on Kubernetes
nickname: Kubernetes
group: Deploying Alluxio
priority: 3
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run Alluxio
on Kubernetes using the specification included in the Alluxio Docker image.

* Table of Contents
{:toc}

## Prerequisites

- A Kubernetes cluster (version >= 1.8). Alluxio workers will use `emptyDir` volumes with a
restricted size using the `sizeLimit` parameter. This is an alpha feature in Kubernetes 1.8.
Please ensure the feature is enabled.
- An Alluxio Docker image [alluxio/alluxio](https://hub.docker.com/r/alluxio/alluxio/). If using a
private Docker registry, refer to the Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).

## Basic Setup

This tutorial walks through a basic Alluxio setup on Kubernetes.

### Extract Kubernetes Specs

Extract the Kubernetes specifications required to deploy Alluxio from the Docker image.

```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/integration/kubernetes/ - > kubernetes.tar
$ docker rm -v $id 1>/dev/null
$ tar -xvf kubernetes.tar
$ cd kubernetes
```

### Provision a Persistent Volume

Alluxio master can be configured to use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
for storing the journal. The volume, once claimed, is persisted across restarts of the master process.

Note: [Embedded Journal]({{ '/en/operation/Journal.html' | relativize_url }}#embedded-journal-configuration)
configuration is not supported on Kubernetes.

Create the persistent volume spec from the template. The access mode `ReadWriteMany` is used to allow
multiple Alluxio master nodes to access the shared volume.

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

### Deploy Alluxio

Alluxio can be deployed using a [helm](https://helm.sh/docs/) chart or directly using `kubectl` if `helm`
is not available or the deployment needs additional customization.

#### Using `helm`

***Pre-requisites:*** A helm repo with the Alluxio helm chart must be available. To prepare a local helm
repository, follow instructions as follows:
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

Once the helm repository is available, Alluxio prepare the Alluxio configuration:
```console
$ cat << EOF > config.yaml
properties:
  alluxio.master.mount.table.root.ufs: "<under_storage_address>"
EOF
```
Note: The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.
For example:
- If using Amazon S3 as the under store, add these properties:
```console
$ cat << EOF > config.yaml
properties:
  alluxio.master.mount.table.root.ufs: "s3a://<bucket>"
  aws.accessKeyId: "<accessKey>"
  aws.secretKey: "<secretKey>"
EOF
```
- If using HDFS as the under store, first create secrets for any configuration required by an HDFS
client. These are mounted under `/secrets`.
```console
$ kubectl create secret generic alluxio-hdfs-config --from-file=./core-site.xml --from-file=./hdfs-site.xml
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

Install
```console
helm install --name alluxio -f config.yaml alluxio-local/alluxio --version {{site.ALLUXIO_VERSION_STRING}}
```
#### Using `kubectl`

Define environment variables in `alluxio.properties`. Copy the properties template at
`integration/kubernetes/conf`, and modify or add any configuration properties as required.
The Alluxio under filesystem address MUST be modified. Any credentials MUST be modified.

```
# Replace <under_storage_address> with the appropriate URI, for example s3://my-bucket
# If using an under storage which requires credentials be sure to specify those as well
ALLUXIO_JAVA_OPTS=-Dalluxio.master.mount.table.root.ufs=<under_storage_address>
```

Note that when running Alluxio with host networking, the ports assigned to Alluxio services must
not be occupied beforehand.
```console
$ cp alluxio-configMap.yaml.template alluxio-configMap.yaml
```

Create a ConfigMap.
```console
$ kubectl create -f alluxio-configMap.yaml
```

Prepare the Alluxio deployment specs from the templates. Modify any parameters required, such as
location of the **Docker image**, and CPU and memory requirements for pods.
```console
$ cp alluxio-master.yaml.template alluxio-master.yaml
$ cp alluxio-worker.yaml.template alluxio-worker.yaml
```
Note: Please make sure that the version of the Kubernetes specification matches the version of the
Alluxio Docker image being used.

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```console
$ kubectl create -f alluxio-master.yaml
$ kubectl create -f alluxio-worker.yaml
```

Verify status of the Alluxio deployment.
```console
$ kubectl get pods
```

If using peristent volumes for Alluxio master, the status of the volume should change to `CLAIMED`.
```console
$ kubectl get pv alluxio-journal-volume
```

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

### Uninstall

#### Using `helm`

Uninstall Alluxio as follows:
```console
$ helm delete alluxio
```

#### Using `kubectl`
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

##### Hostname Introspection

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

##### Domain Socket Path

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
