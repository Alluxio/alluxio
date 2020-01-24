---
layout: global
title: Running Spark on Alluxio in Kubernetes
nickname: Spark on Kubernetes
group: Compute Integrations
priority: 7
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run a Spark job on Alluxio
running in a Kubernetes environment.

* Table of Contents
{:toc}

## Overview

Spark running on Kubernetes can use Alluxio as the data access layer.
This guide walks through an example Spark job on Alluxio in Kubernetes.
The example used in this tutorial is a job to count the number of lines in a file.
We refer to this job as `count` in the following text.

## Prerequisites

- A Kubernetes cluster (version >= 1.8).
- Alluxio is deployed on the Kubernetes cluster. For instructions on how to deploy Alluxio, refer to
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }})

## Basic Setup

First, we'll prepare a Spark Docker image including the Alluxio client and any other required jars.
This image should be made available on all Kubernetes nodes.

### Download Binaries

[Download](https://spark.apache.org/downloads.html) the desired Spark version.
We use the pre-built binary for the `spark-submit` command as well as building the Docker image
using Dockerfile included with Alluxio.
> Note: Download the package prebuilt for hadoop

```console
$ tar -xf spark-2.4.4-bin-hadoop2.7.tgz
$ cd spark-2.4.4-bin-hadoop2.7
```

### Build the Spark Docker Image

Extract the Alluxio client jar from the Alluxio Docker image:

```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/client/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar \
  - > alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar
$ docker rm -v $id 1>/dev/null
```

Add the required Alluxio client jar and build a Docker image used for the Spark driver and executor
pods. Run the following from the Spark distribution directory to add the Alluxio client jar.

```console
$ cp <path_to_alluxio_client>/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar jars/
```
> Note: Any jar copied to the `jars` directory is included in the Spark Docker image when built.

Build the Spark Docker image

```console
$ docker build -t spark-alluxio -f kubernetes/dockerfiles/spark/Dockerfile .
```

## Example(s)

This section shows how to use the built Docker image to launch a Spark job with Alluxio as the
data source.

### Short-circuit operations

Short-circuit access enables an Alluxio client in a Spark executor to access the Alluxio
worker storage on the host machine directly.
This improves performance by not communicating with the Alluxio worker using the networking stack.

If domain sockets were not setup when deploying Alluxio as per instructions on
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }}#short-circuit-access),
you can skip mounting the `hostPath` volumes to the Spark executors.

If a domain socket location was setup on hosts running the Alluxio worker process at location
`/tmp/alluxio-domain` with the Alluxio configuration as
`alluxio.worker.data.server.domain.socket.address=/opt/domain`, use the following Spark
configuration to mount `/tmp/alluxio-domain` to `/opt/domain` in the Spark executor pod.
The `spark-submit` command in the following section includes these properties.

```properties
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory
```

Note: 
- Volume support in Spark was added in version 2.4.0.
- You may observe a performance hit when not using short-circuit access via a domain socket.

### Run a Spark job

The following command runs an example word count job in the Alluxio location
`/LICENSE`.
The output and time taken can be seen in the logs for Spark driver pod. Refer to Spark
[documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html) for further instructions.

Create the service account (if required)

```console
$ kubectl create serviceaccount spark
$ kubectl create clusterrolebinding spark-role --clusterrole=edit \
  --serviceaccount=default:spark --namespace=default
```

Run the job from the Spark distribution directory
```console
$ ./bin/spark-submit --master k8s://https://<master>:8443 \
--deploy-mode cluster --name spark-alluxio --conf spark.executor.instances=1 \
--class org.apache.spark.examples.JavaWordCount \
--driver-memory 500m --executor-memory 1g \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=spark-alluxio \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory \
local:///opt/spark/examples/jars/spark-examples_2.11-2.4.4.jar \
alluxio://alluxio-master.default.svc.cluster.local:19998/LICENSE
```

## Troubleshooting

### Accessing Alluxio Client Logs

The Alluxio client logs can be found in the Spark driver and executor logs.
Refer to
[Spark documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#debugging)
for further instructions.
