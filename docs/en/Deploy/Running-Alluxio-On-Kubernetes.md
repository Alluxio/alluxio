---
layout: global
title: Deploy Alluxio on Kubernetes
nickname: Alluxio on Kubernetes
group: Deploying Alluxio
priority: 3
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run Alluxio
on Kubernetes using the specification that comes in the Alluxio Github repository.

# Basic Tutorial

This tutorial walks through a basic Alluxio setup on Kubernetes.

## Prerequisites

- A Kubernetes cluster (version >= 1.8). Alluxio workers will use `emptyDir` volumes with a 
restricted size using the `sizeLimit` parameter. This is an alpha feature in Kubernetes 1.8.
Please ensure the feature is enabled.
- An Alluxio Docker image. Refer to [this page](Running-Alluxio-On-Docker.html) for instructions
to build an image. The image must be available for a pull from all Kubernetes hosts running
Alluxio processes. This can be achieved by pushing the image to an accessible Docker registry,
or pushing the image individually to all hosts. If using a private Docker registry, refer to the
Kubernetes [documentation](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/).

## Clone the Alluxio repo

```bash
$ git clone https://github.com/Alluxio/alluxio.git
$ cd integration/kubernetes
```

The kubernetes specifications required to deploy Alluxio can be found under `integration/kubernetes`.

## Enable short-circuit operations

Short-circuit access enables clients to perform read and write operations directly against the 
worker memory instead of having to go through the worker process. Set up a domain socket on all hosts
eligible to run the Alluxio worker process to enable this mode of operation.

From the host machine, create a directory for the shared domain socket.
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
```

This step can be skipped in case short-circuit accesss is not desired or cannot be set up. To disable
this feature, set the property `alluxio.user.short.circuit.enabled=false` according to the instructions
in the configuration section below.

By default, short-circuit operations between the Alluxio client and worker are enabled if the client
hostname matches the worker hostname. This may not be true if the client is running as part of a container
with virtual networking. In such a scenario, set the following property to use filesystem inspection
to enable short-circuit. Short-circuit writes are then enabled if the worker UUID is located on the client
filesystem.
```properties
alluxio.worker.data.server.domain.socket.as.uuid=true
alluxio.worker.data.server.domain.socket.address=/path/to/domain/socket/directory
```

## Provision a Persistent Volume

Alluxio master can be configured to use a [persistent volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
for storing the journal. The volume, once claimed, is persisted across restarts of the master process.

Create the persistent volume spec from the template. The access mode `ReadWriteMany` is used to allow
multiple Alluxio master nodes to access the shared volume.
```bash
$ cp alluxio-journal-volume.yaml.template alluxio-journal-volume.yaml
```

Note: the spec provided uses a `hostPath` volume for demonstration on a single-node deployment. For a
multi-node cluster, you may choose to use NFS, AWSElasticBlockStore, GCEPersistentDisk or other available
persistent volume plugins.

Create the persistent volume.
```bash
$ kubectl create -f alluxio-journal-volume.yaml
```

## Configure Alluxio properties
Alluxio containers in Kubernetes use environment variables to set Alluxio properties. Refer to 
[Docker configuration](Running-Alluxio-On-Docker.html) for the corresponding environment variable
name for Alluxio properties in `conf/alluxio-site.properties`.

Define all environment variables in a single file. Copy the properties template at
`integration/kubernetes/conf`, and modify or add any configuration properties as required.
Note that when running Alluxio with host networking, the ports assigned to Alluxio services must
not be occupied beforehand.
```bash
$ cp conf/alluxio.properties.template conf/alluxio.properties
```

Create a ConfigMap.
```bash
$ kubectl create configmap alluxio-config --from-file=ALLUXIO_CONFIG=conf/alluxio.properties
```

## Deploy

Prepare the Alluxio deployment specs from the templates. Modify any parameters required, such as
location of the Docker image, and CPU and memory requirements for pods.
```bash
$ cp alluxio-master.yaml.template alluxio-master.yaml
$ cp alluxio-worker.yaml.template alluxio-worker.yaml
```

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```bash
$ kubectl create -f alluxio-master.yaml
$ kubectl create -f alluxio-worker.yaml
```

Verify status of the Alluxio deployment.
```bash
$ kubectl get pods
```

If using peristent volumes for Alluxio master, the status of the volume should change to `CLAIMED`.
```bash
$ kubectl get pv alluxio-journal-volume
```

## Verify

Once ready, access the Alluxio CLI from the master pod and run basic I/O tests.
```bash
$ kubectl exec -ti alluxio-master-0 /bin/bash
```

From the master pod, execute the following:
```bash
$ cd /opt/alluxio
$ ./bin/alluxio runTests
```

## Uninstall

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
