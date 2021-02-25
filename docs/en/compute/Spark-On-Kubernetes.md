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
> Note: **Make sure all your nodes (where the spark-driver and spark-executor pods will run)
have this image.**

## Example(s)

This section shows how to use the built Docker image to launch a Spark job with Alluxio as the
data source.

### Short-circuit operations

Short-circuit access enables an Alluxio client in a Spark executor to access the Alluxio
worker storage on the host machine directly.
This improves performance by not communicating with the Alluxio worker using the networking stack.

If domain sockets were not setup when deploying Alluxio as per instructions on
[this page]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }}#enable-short-circuit-access),
you can skip mounting the `hostPath` volumes to the Spark executors.

If a domain socket location was setup on hosts running the Alluxio worker process at location
`/tmp/alluxio-domain` with the Alluxio configuration as
`alluxio.worker.data.server.domain.socket.address=/opt/domain`, use the following Spark
configuration to mount `/tmp/alluxio-domain` to `/opt/domain` in the Spark executor pod.
The `spark-submit` command in the following section includes these properties.

The domain socket on the Alluxio worker can be a `hostPath` Volume or a `PersistententVolumeClaim`,
depending on your setup.
You can find more details on how to configure Alluxio worker to use short circuit 
[here]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#short-circuit-access' | relativize_url }}).
The spark-submit arguments will be different for these two options.
You can find more about how to mount volumes to Spark executors in Spark
[documentation](https://spark.apache.org/docs/2.4.4/running-on-kubernetes.html#using-kubernetes-volumes).

{% navtabs domainSocket %}
  {% navtab hostPath %}
  If you are using `hostPath` domain sockets, you should pass these properties to Spark:
  
  ```properties
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.path=/opt/domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.mount.readOnly=true
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.path=/tmp/alluxio-domain
  spark.kubernetes.executor.volumes.hostPath.alluxio-domain.options.type=Directory
  ```
  {% endnavtab %}
  {% navtab PersistententVolumeClaim %}
  If you are using `PersistententVolumeClaim` domain sockets, you should pass these properties to Spark:
  
  ```properties
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.path=/opt/domain \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.mount.readOnly=true \
  spark.kubernetes.executor.volumes.persistentVolumeClaim.alluxio-domain.options.claimName=<domainSocketPVC name>
  ```
  
  {% endnavtab %}
{% endnavtabs %}

> Note: 
> - Volume support in Spark was added in version 2.4.0.
> - You may observe a performance hit when not using short-circuit access via a domain socket.

### Run a Spark job

#### Create the service account (optional)

You can create one service account for running the spark job with the required access as below  
if you do not have one to use.

```console
$ kubectl create serviceaccount spark
$ kubectl create clusterrolebinding spark-role --clusterrole=edit \
  --serviceaccount=default:spark --namespace=default
```

#### Submit a Spark job

The following command runs an example word count job in the Alluxio location
`/LICENSE`. Please ensure this file exists in your Alluxio cluster, or otherwise
change the path to a file which does exist.

The output and time taken can be seen in the logs for Spark driver pod. Refer to
the Spark [documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
for further details about running Spark on Kubernetes. For example, you may find
additional details on some of the flags used in this command
[here](https://spark.apache.org/docs/latest/running-on-kubernetes.html?q=cluster-info#cluster-mode).

Run the job from the Spark distribution directory
```console
$ ./bin/spark-submit --master k8s://https://<kubernetes-api-server>:6443 \
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
alluxio://<alluxio-master>:19998/LICENSE
```

> Note:
> - You can find the address and port of the Kubernetes API server by running `kubectl cluster-info`.
>   - The default Kubernetes API server port is 6443 but may differ based on your cluster's configuration
> - It is recommended to set the `<alluxio-master>` hostname in this command to the Kubernetes Service
  name for your Alluxio master (eg., `alluxio-master-0`).
> - If you are using a different version of Spark, please ensure the path to the
  `spark-examples_2.11-2.4.4.jar` is correctly set for your version of Spark
> - You should also take care to ensure the volume properties align with your
  [domain socket volume type]({{ '/en/compute/Spark-On-Kubernetes.html#short-circuit-operations' | relativize_url }}).

## Troubleshooting

### Accessing Alluxio Client Logs

The Alluxio client logs can be found in the Spark driver and executor logs.
Refer to
[Spark documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#debugging)
for further instructions.

### HTTP 403 on Kubernetes client

If your spark job failed due to failure in the Kubernetes client like the following:
```
WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed
...
ERROR SparkContext: Error initializing SparkContext.
io.fabric8.kubernetes.client.KubernetesClientException
```

This is probably due to a [known issue](https://issues.apache.org/jira/browse/SPARK-28921) 
that can be resolved by upgrading `kubernetes-client.jar` to 4.4.x.
You can patch the docker image by updating the `kubernetes-client-x.x.jar` before building the 
`spark-alluxio` image.

```console
rm spark-2.4.4-bin-hadoop2.7/jars/kubernetes-client-*.jar
wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-client-4.4.2.jar 
cp kubernetes-client-4.4.2.jar spark-2.4.4-bin-hadoop2.7/jars
```

Then build the `spark-alluxio` image and distribute to all your nodes.

### Service account does not have access

If you see errors like below complaining some operations are forbidden, that is because the
service account you use for the spark job does not have enough access to perform the action.

```
ERROR Utils: Uncaught exception in thread main
io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: DELETE at: \
https://kubernetes.default.svc/api/v1/namespaces/default/pods/spark-alluxiolatest-exec-1. \
Message: Forbidden!Configured service account doesn't have access. Service account may have been revoked. \
pods "spark-alluxiolatest-exec-1" is forbidden: User "system:serviceaccount:default:default" \
cannot delete resource "pods" in API group "" in the namespace "default".
```

You should ensure you have the correct access by 
[creating a service account]({{ '/en/compute/Spark-On-Kubernetes.html#create-the-service-account-optional' | relativize_url }}).
