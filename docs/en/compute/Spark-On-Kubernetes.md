---
layout: global
title: Deploy Alluxio on Kubernetes
nickname: Kubernetes
group: Deploying Alluxio
priority: 3
---

Alluxio can be run on Kubernetes. This guide demonstrates how to run a Spark job on Alluxio
running in a Kubernetes environment.

* Table of Contents
{:toc}

## Basic Tutorial

This tutorial walks through an example Spark job on Alluxio in Kubernetes. The example used in this
tutorial is a job to count the number of lines in a file. We refer to this job as `count` in the
following text.

### Prerequisites

- A Kubernetes cluster (version >= 1.8).
- Alluxio is deployed on the Kubernetes cluster. For instructions on how to deploy Alluxio, refer to
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }})

### Download Binaries

Download the desired Spark version. We use the pre-built binary for the `spark-submit` command as well
as building the Docker image using the included Dockerfile.
```bash
wget http://apache.mirrors.tds.net/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar -xf spark-2.4.0-bin-hadoop2.7.tgz
cd spark-2.4.0-bin-hadoop2.7
```

If running the `count` example, download the Alluxio examples jar.
```bash
wget https://alluxio-documentation.s3.amazonaws.com/examples/spark/alluxio-examples_2.12-1.0.jar
cp <path_to_alluxio_examples>/alluxio-examples_2.12-1.0.jar jars/
```
Note: Any jar copied to the `jars` directory is included in the Spark Docker image when built.

### Short-circuit operations

Short-circuit access enables an Alluxio client in a Spark executor to access the Alluxio
worker storage on the host machine directly. This improves performance by not communicating with the
Alluxio worker using the networking stack.

If domain sockets were not setup when deploying Alluxio as per instructions on
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#enable-short-circuit-operations' | relativize_url }}),
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

### Build the Spark Docker Image

Add the required Alluxio client jars and build a docker image used for the Spark driver and executor
pods. Run the following from the Spark distribution directory.

```bash
# If using minikube
cp ~/.minikube/apiserver.key bin/
cp ~/.minikube/apiserver.crt bin/
cp ~/.minikube/ca.crt bin/

# Add the Alluxio client jar
cp <path_to_alluxio_client>/client/alluxio-2.0.0-preview-client.jar jars/

# Build the Spark docker image
docker build -t spark-alluxio -f kubernetes/dockerfiles/spark/Dockerfile .
```

### Run a Spark job

The following command runs an example job to count the number of lines in the Alluxio location `/LICENSE`.
The output and time taken can be seen in the logs for Spark driver pod. Refer to Spark [documentation]
(https://spark.apache.org/docs/latest/running-on-kubernetes.html) for further instructions.

```bash
# Create service account if required
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default

# Run the job from the Spark distribution directory
./bin/spark-submit --master k8s://https://<master>:8443 --deploy-mode cluster --name spark-alluxio --conf spark.executor.instances=1 \
--class alluxio.examples.Count --driver-memory 500m --executor-memory 1g \
--conf "spark.executor.extraJavaOptions= -Dalluxio.user.network.netty.channel=NIO -Dalluxio.worker.network.netty.channel=NIO" \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=spark-alluxio \
--conf spark.kubernetes.authenticate.submission.caCertFile=./bin/ca.crt \
--conf spark.kubernetes.authenticate.submission.clientKeyFile=./bin/apiserver.key \
--conf spark.kubernetes.authenticate.submission.clientCertFile=./bin/apiserver.crt \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/domain \
--conf spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory \
local:///opt/spark/jars/alluxio-examples_2.12-1.0.jar alluxio://alluxio-master.default.svc.cluster.local:19998/LICENSE
```

Note: By default, Alpine Linux does not contain the native libraries required to create an EPOLL mode netty
channel. On linux distributions or docker images with the native libraries included, EPOLL can be used by
removing the properties `alluxio.*.network.netty.channel` from the Spark executor java options.
