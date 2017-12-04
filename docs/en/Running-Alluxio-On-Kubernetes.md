---
layout: global
title: Running Alluxio on Kubernetes
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
- An Alluxio docker image. Refer to [this page](Running-Alluxio-On-Docker.html) for instructions
to build an image.

## Clone the Alluxio repo

```bash
$ git clone https://github.com/Alluxio/alluxio.git
$ cd integration/kubernetes
```

The kubernetes specifications required to deploy Alluxio can be found under `integration/kubernetes`.

## Short-circuit operations

Short-circuit access enable clients to perform read and write operations directly against the 
worker memory instead of having to go through the worker process. Setup a domain socket on all hosts
eligible to run the Alluxio worker process to enable this mode of operation.

From the host machine, create a directory for the shared domain socket.
```bash
$ mkdir /tmp/domain
$ chmod a+w /tmp/domain
$ touch /tmp/domain/d
$ chmod a+w /tmp/domain/d
```

## Journal Volume

Alluxio master can be configured to use a persistent volume for storing the journal. The volume,
once claimed, is persisted across restarts of the master process. Prepare a persistent volume on 
hosts eligible to run the Alluxio master process.

```bash
$ cp alluxio-pv-volume.yaml.template alluxio-pv-volume.yaml
$ kubectl create -f alluxio-pv-volume.yaml
```

## Alluxio Configuration Properties
Alluxio containers in Kubernetes use environment variables to set Alluxio properties. Refer to 
[docker configuration](Running-Alluxio-On-Docker.html) for the corresponding environment variable
name for Alluxio properties in `conf/alluxio-site.properties`.

Define all  environment variables in a single file and create a `ConfigMap`.
```bash
$ cp conf/alluxio.properties.template conf/alluxio.properties
$ kubectl create configmap alluxio-config --from-file=ALLUXIO_CONFIG=conf/alluxio.properties
```

## Deploy

Once all the pre-requisites and configuration have been setup, deploy Alluxio.
```bash
$ cp alluxio-master.yaml.template alluxio-master.yaml
$ kubectl create -f alluxio-master.yaml
$ cp alluxio-worker.yaml.template alluxio-worker.yaml
$ kubectl create -f alluxio-worker.yaml
```

Verify status of the Alluxio deployment.
```bash
$ kubectl get pods
```

If using peristent volumes for Alluxio master, the status of the volume should change to `CLAIMED`.
```bash
$ kubectl get pv alluxio-pv-volume
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
