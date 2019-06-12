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

```bash
id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
docker cp $id:/opt/alluxio/integration/kubernetes/ - > kubernetes.tar
docker rm -v $id 1>/dev/null
tar -xvf kubernetes.tar
cd kubernetes
```

### Provision a Persistent Volume

Alluxio master can be configured to use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
for storing the journal. The volume, once claimed, is persisted across restarts of the master process.

Note: [Embedded Journal]({{ '/en/operation/Journal.html' | relativize_url }}#embedded-journal-configuration)
configuration is not supported on Kubernetes.

Create the persistent volume spec from the template. The access mode `ReadWriteMany` is used to allow
multiple Alluxio master nodes to access the shared volume.

```bash
cp alluxio-journal-volume.yaml.template alluxio-journal-volume.yaml
```

Note: the spec provided uses a `hostPath` volume for demonstration on a single-node deployment. For a
multi-node cluster, you may choose to use NFS, AWSElasticBlockStore, GCEPersistentDisk or other available
persistent volume plugins.

Create the persistent volume.
```bash
kubectl create -f alluxio-journal-volume.yaml
```

### Configure Alluxio properties

Define environment variables in `alluxio.properties`. Copy the properties template at
`integration/kubernetes/conf`, and modify or add any configuration properties as required.
Note that when running Alluxio with host networking, the ports assigned to Alluxio services must
not be occupied beforehand.
```bash
cp conf/alluxio.properties.template conf/alluxio.properties
```

Create a ConfigMap.
```bash
kubectl create configmap alluxio-config --from-env-file=conf/alluxio.properties
```

### Deploy

Prepare the Alluxio deployment specs from the templates. Modify any parameters required, such as
location of the **Docker image**, and CPU and memory requirements for pods.
```bash
cp alluxio-master.yaml.template alluxio-master.yaml
cp alluxio-worker.yaml.template alluxio-worker.yaml
```
Note: Please make sure that the version of the Kubernetes specification matches the version of the
Alluxio Docker image being used.

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```bash
kubectl create -f alluxio-master.yaml
kubectl create -f alluxio-worker.yaml
```

Verify status of the Alluxio deployment.
```bash
kubectl get pods
```

If using peristent volumes for Alluxio master, the status of the volume should change to `CLAIMED`.
```bash
kubectl get pv alluxio-journal-volume
```

### Access the Web UI

The Alluxio UI can be accessed from outside the kubernetes cluster using port forwarding.
```bash
kubectl port-forward alluxio-master-0 19999:19999
```

### Verify

Once ready, access the Alluxio CLI from the master pod and run basic I/O tests.
```bash
kubectl exec -ti alluxio-master-0 /bin/bash
```

From the master pod, execute the following:
```bash
cd /opt/alluxio
./bin/alluxio runTests
```

### Uninstall

Uninstall Alluxio:
```bash
kubectl delete -f alluxio-worker.yaml
kubectl delete -f alluxio-master.yaml
kubectl delete configmaps alluxio-config
```

Execute the following to remove the persistent volume storing the Alluxio journal. Note: Alluxio metadata
will be lost.
```bash
kubectl delete -f alluxio-journal-volume.yaml
```

## Advanced Setup

### POSIX API

Once Alluxio is deployed on Kubernetes, there are multiple ways in which a client application can
connect to it. For applications using the [POSIX API]({{ '/en/api/POSIX-API.html' | relativize_url }}),
application containers can simply mount the Alluxio FileSystem.

In order to use the POSIX API, first deploy the Alluxio FUSE daemon.
```bash
cp alluxio-fuse.yaml.template alluxio-fuse.yaml
kubectl create -f alluxio-fuse.yaml
```
Note:
- The container running the Alluxio FUSE daemon must have the `securityContext.privileged=true` with
SYS_ADMIN capabilities. Application containers that require Alluxio access do not need this privilege.
- A different Docker image [alluxio/alluxio-fuse](https://hub.docker.com/r/alluxio/alluxio-fuse/) based
on `ubuntu` instead of `alpine` is needed to run the FUSE daemon. Application containers can run on
any Docker image.

Verify that a container can simply mount the Alluxio FileSystem without any custom binaries or
capabilities using a `hostPath` mount of location `/alluxio-fuse`:
```bash
cp alluxio-fuse-client.yaml.template alluxio-fuse-client.yaml
kubectl create -f alluxio-fuse-client.yaml
```

If using the template, Alluxio is mounted at `/alluxio-fuse` and can be accessed via the POSIX-API
across multiple containers.

## Troubleshooting

### Enable Debug Logging

To change the log level for Alluxio servers (master and workers), use the CLI command `logLevel` as
follows:

Access the Alluxio CLI from the master pod.
```bash
kubectl exec -ti alluxio-master-0 /bin/bash
```

From the master pod, execute the following:
```bash
cd /opt/alluxio
./bin/alluxio logLevel --level DEBUG --logName alluxio
```

### Accessing Logs

The Alluxio master and job master run as separate containers of the master pod. Similarly, the
Alluxio worker and job worker run as separate containers of a worker pod. Logs can be accessed for
the individual containers as follows.

Master:
```bash
kubectl logs -f alluxio-master-0 -c alluxio-master
```

Worker:
```bash
kubectl logs -f alluxio-worker-<id> -c alluxio-worker
```

Job Master:
```bash
kubectl logs -f alluxio-master-0 -c alluxio-job-master
```

Job Worker:
```bash
kubectl logs -f alluxio-worker-<id> -c alluxio-job-worker
```

### Short-circuit Access

Short-circuit access enables clients to perform read and write operations directly against the
worker bypassing the networking interface. As part of the Alluxio worker pod creation, a directory
is created on the host at `/tmp/domain` for the shared domain socket.

To disable this feature, set the property `alluxio.user.short.circuit.enabled=false`. By default,
short-circuit operations between the Alluxio client and worker are enabled if the client hostname
matches the worker hostname. This may not be true if the client is running as part of a container
with virtual networking. In such a scenario, set the following property to use filesystem inspection
to enable short-circuit and make sure the client container mounts the directory specified as the
domain socket address. Short-circuit writes are then enabled if the worker UUID is located on the
client filesystem.

```properties
alluxio.worker.data.server.domain.socket.as.uuid=true
alluxio.worker.data.server.domain.socket.address=/opt/domain
```

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
