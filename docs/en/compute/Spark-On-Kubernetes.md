---
layout: global
title: Running Spark on Alluxio in Kubernetes
nickname: Spark on Kubernetes
group: Data Applications
priority: 7
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run a Spark job on Alluxio
running in a Kubernetes environment.

* Table of Contents
{:toc}

## Overview

Spark running on Kubernetes can use Alluxio as the data access layer. This guide walks through an
example Spark job on Alluxio in Kubernetes. The example used in this tutorial is a job to count the
number of lines in a file. We refer to this job as `count` in the following text.

## Prerequisites

- A Kubernetes cluster (version >= 1.8).
- Alluxio is deployed on the Kubernetes cluster. For instructions on how to deploy Alluxio, refer to
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }})

## Basic Setup

First, prepare the Spark Docker image including the Alluxio client and any other required jars. This
image should be made available on all Kubernetes nodes.

### Download Binaries

[Download](https://spark.apache.org/downloads.html) the desired Spark version. We use the pre-built
binary for the `spark-submit` command as well as building the Docker image using the included Dockerfile.
```bash
tar -xf spark-2.4.0-bin-hadoop2.7.tgz
cd spark-2.4.0-bin-hadoop2.7
```

If running the `count` example, download the Alluxio examples jar.
```bash
wget https://alluxio-documentation.s3.amazonaws.com/examples/spark/alluxio-examples_2.12-1.0.jar
cp <path_to_alluxio_examples>/alluxio-examples_2.12-1.0.jar jars/
```
Note: Any jar copied to the `jars` directory is included in the Spark Docker image when built.

### Build the Spark Docker Image

Extract the Alluxio client jar from the Alluxio Docker image:
```bash
id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
docker cp $id:/opt/alluxio/client/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar - > alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar
docker rm -v $id 1>/dev/null
```

Add the required Alluxio client jar and build a Docker image used for the Spark driver and executor
pods. Run the following from the Spark distribution directory.

Add the Alluxio client jar
```bash
cp <path_to_alluxio_client>/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar jars/
```

Build the Spark Docker image
```bash
docker build -t spark-alluxio -f kubernetes/dockerfiles/spark/Dockerfile .
```

## Example(s)

This section shows how to use the built Docker image to launch a Spark job with Alluxio as the source.

### Short-circuit operations

Short-circuit access enables an Alluxio client in a Spark executor to access the Alluxio
worker storage on the host machine directly. This improves performance by not communicating with the
Alluxio worker using the networking stack.

If domain sockets were not setup when deploying Alluxio as per instructions on
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }}#short-circuit-access),
you can skip mounting the `hostPath` volumes to the Spark executors.

If a domain socket location was setup on hosts running the Alluxio worker process at location
`/tmp/domain` with the Alluxio configuration as `alluxio.worker.data.server.domain.socket.address=/opt/domain`,
use the following Spark configuration to mount `/tmp/domain` to `/opt/domain` in the Spark executor
pod. The `spark-submit` command in the following section includes these properties.
```properties
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/domain
spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory
```

Note: 
- Volume support in Spark was added in version 2.4.0.
- You may observe a performance hit when not using short-circuit access via a domain socket.

### Run a Spark job

The following command runs an example job to count the number of lines in the Alluxio location `/LICENSE`.
The output and time taken can be seen in the logs for Spark driver pod. Refer to Spark
[documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html) for further instructions.

Create the service account (if required)
```bash
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

Run the job from the Spark distribution directory
```bash
./bin/spark-submit --master k8s://https://<master>:8443 --deploy-mode cluster --name spark-alluxio --conf spark.executor.instances=1 \
--class alluxio.examples.Count --driver-memory 500m --executor-memory 1g \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=spark-alluxio \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory \
local:///opt/spark/jars/alluxio-examples_2.12-1.0.jar alluxio://alluxio-master.default.svc.cluster.local:19998/LICENSE
```

## Troubleshooting

### Accessing Alluxio Client Logs

The Alluxio client logs can be found in the Spark driver and executor logs. Refer to
[Spark documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#debugging) for
further instructions.
