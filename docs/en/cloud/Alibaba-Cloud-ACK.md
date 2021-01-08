---
layout: global
title: Running Alluxio on Alibaba Cloud Container Service for Kubernetes (ACK)
nickname: Alibaba Cloud ACK
group: Cloud Native
---

This guide describes how to install and configure Alluxio on [Alibaba Cloud Container Service for Kubernetes (ACK)](https://www.alibabacloud.com/product/kubernetes).

* Table of Contents
{:toc}

## Prerequisites
* ACK version  >= 1.12.6

## Install Alluxio in ACK

This section introduces how to install Alluxio on Alibaba Cloud Container Service for Kubernetes (ACK) in a few steps.

### Specify Which Nodes to Install Alluxio
Before installing Alluxio components, you need to label the target Kubernetes nodes with "alluxio=true", the steps are as follows:

{% accordion label %}
  {% collapsible Select cluster %}
Login to [Container Service - Kubernetes Console](https://cs.console.aliyun.com/#/k8s/cluster/list).
Under the Kubernetes menu, click "**Clusters**" > "**Nodes**" in the left navigation bar to enter the node list page.
Select the specific cluster and click the "**Manage Labels**" in the upper right corner of the page.
  ![](http://application-catalog-hk.oss-cn-hongkong.aliyuncs.com/alluxio/images/image-1.jpg)
  {% endcollapsible %}

  {% collapsible Select nodes %}
In the node list, select nodes in batches, and then click "**Add Label**".
  ![](http://application-catalog-hk.oss-cn-hongkong.aliyuncs.com/alluxio/images/image-2.jpg)
  {% endcollapsible %}

  {% collapsible Add label %}
Fill in the label name as "alluxio" and the value as "true", click "**OK**".
  ![](http://application-catalog-hk.oss-cn-hongkong.aliyuncs.com/alluxio/images/image-3.jpg)
  {% endcollapsible %}
{% endaccordion %}

### Install Alluxio Using App Catalog

Login to [Container Service - Kubernetes Console](https://cs.console.aliyun.com/#/k8s/cluster/list).
Select "**Marketplace**" > "**App Catalog**" on the left navigation bar, and select Alluxio on the right.
On the "**App Catalog**" > "**Alluxio**" page, select the cluster and namespace created in the prerequisites in the creation panel on the right, and click "**Create**".

### Verify Installation
Use `kubectl` to check whether the Alluxio pods are running:

```console
# kubectl get po -n alluxio
NAME READY STATUS RESTARTS AGE
alluxio-fuse-pjw5x 1/1 Running 0 83m
alluxio-fuse-pqgz4 1/1 Running 0 83m
alluxio-master-0 2/2 Running 0 83m
alluxio-worker-8lcpb 2/2 Running 0 83m
alluxio-worker-hqv8l 2/2 Running 0 83m
```

Use `kubectl` to log in to the Alluxio master pod and check the health of this Alluxio cluster:

```console
# kubectl exec -ti alluxio-master-0 -n alluxio bash

bash-4.4# alluxio fsadmin report capacity

Capacity information for all workers:
    Total Capacity: 2048.00MB
        Tier: MEM  Size: 2048.00MB
    Used Capacity: 0B
        Tier: MEM  Size: 0B
    Used Percentage: 0%
    Free Percentage: 100%

Worker Name      Last Heartbeat   Storage       MEM
192.168.5.202    0                capacity      1024.00MB
                                  used          0B (0%)
192.168.5.201    0                capacity      1024.00MB
                                  used          0B (0%)
```

## Example: Running Spark Jobs

{% accordion Spark %}
  {% collapsible Install spark-operator %}
Go to [Container Service Application Catalog](https://cs.console.aliyun.com/#/k8s/catalog/list),
search for "ack-spark-operator" in the search box in the upper right:
![image-4.png](http://application-catalog-hk.oss-cn-hongkong.aliyuncs.com/alluxio/images/image-4.jpg)

Choose to install "ack-spark-operator" on the target cluster (the cluster in this document is "ack-create-by-openapi-1"), and then click "**create**", as shown in the figure:
![image-5.png](http://application-catalog-hk.oss-cn-hongkong.aliyuncs.com/alluxio/images/image-5.jpg)
  {% endcollapsible %}

  {% collapsible Build Spark docker image %}
Download the required Spark version from [Spark download page](https://spark.apache.org/downloads.html). The Spark version used in this example is 2.4.6.
Run the following command to download Spark:

```console
$ cd /root
$ wget https://mirror.bit.edu.cn/apache/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
```

After the download is complete, unzip the package and set env "`SPARK_HOME`":
```console
$ tar -xf spark-2.4.6-bin-hadoop2.7.tgz
$ export SPARK_HOME=$(pwd)/spark-2.4.6-bin-hadoop2.7
```

The spark docker image is the image we used when submitting the Spark task. 
This image needs to include the Alluxio client jar package. 
You can obtain the Alluxio client jar package as follows:
```console
$ id=$(docker create alluxio/alluxio:{{site.ALLUXIO_VERSION_STRING}})
$ docker cp $id:/opt/alluxio/client/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar \
	$SPARK_HOME/jars/alluxio-{{site.ALLUXIO_VERSION_STRING}}-client.jar
$ docker rm -v $id 1>/dev/null
```

After the Alluxio client jar package is ready, start building the image:
```console
$ docker build -t \
  spark-alluxio:2.4.6 -f $SPARK_HOME/kubernetes/dockerfiles/spark/Dockerfile $SPARK_HOME
```
You can find more details about running Alluxio with Spark on Kubernetes [here]({{ '/en/compute/Spark-On-Kubernetes.html' | relativize_url }}).

After the image is built, there are two ways to distribute the image:

* If there is a private image warehouse, push the image to the private image warehouse, and ensure that the K8s cluster node can pull the image.
* If there is no private image warehouse, you need to use the `docker save` command to export the image, then scp the image to each node of the K8s cluster, use the `docker load` command on each node to load the image.
 {% endcollapsible %}

 {% collapsible Upload files to Alluxio %}
As mentioned at the beginning: In this experiment we will submit a Spark job to K8s. 
The Spark job will perform a word count calculation on a certain file. 
Before we kick off the Spark job, we need to upload the file to the Alluxio storage. 
Here, for convenience, we directly upload the file `/opt/alluxio-{{site.ALLUXIO_VERSION_STRING}}/LICENSE` from the Alluxio master node (the file path may be slightly different due to the Alluxio version) 
to the Alluxio namespace.

Use `kubectl exec` to enter the Alluxio master pod, and upload the `LICENSE` file from the current directory to the root directory in Alluxio:

```console
$ kubectl exec -ti alluxio-master-0 -n alluxio bash
# The following steps are executed in the alluxio-master-0 pod
bash-4.4# alluxio fs copyFromLocal LICENSE /
```

Then check which workers store the blocks of file `LICENSE`.

```console
$ kubectl exec -ti alluxio-master-0 -n alluxio bash
# The following steps are executed in the alluxio-master-0 pod

bash-4.4# alluxio fs stat /LICENSE
/LICENSE is a file path.
FileInfo{fileId=33554431, fileIdentifier=null, name=LICENSE, path=/LICENSE, ufsPath=/opt/alluxio/underFSStorage/LICENSE, length=27040, blockSizeBytes=67108864, creationTimeMs=1592381889733, completed= true, folder=false, pinned=false, pinnedlocation=[], cacheable=true, persisted=false, blockIds=[16777216], inMemoryPercentage=100, lastModificationTimesMs=1592381890390, ttl=-1, lastAccessTimesMs=1592381890390, ttlAction=DELETE, owner=root, group=root, mode=420, persistenceState=TO_BE_PERSISTED, mountPoint=false, replicationMax=-1, replicationMin=0, fileBlockInfos=[FileBlockInfo{blockInfo=BlockInfo{id=16777216, length=27040, locations=[BlockLocation {workerId=8217561227881498090, address=WorkerNetAddress{host=192.168.8.17, containerHost=, rpcPort=29999, dataPort=29999, webPort=30000, domainSocketPath=, tieredIdentity=TieredIdentity(node=192.168.8.17, rack=null)}, tierAlias =MEM, mediumType=MEM}]}, offset=0, ufsLocations=[]}], mountId=1, inAlluxioPercentage=100, ufsFingerprint= , acl=user::rw-,group::r--,other::r--, defaultAcl=}
Containing the following blocks:
BlockInfo{id=16777216, length=27040, locations=[BlockLocation{workerId=8217561227881498090, address=WorkerNetAddress{host=192.168.8.17, containerHost=, rpcPort=29999, dataPort=29999, webPort=30000, domainSocketPath=, tieredIdentity=TieredIdentity (node=192.168.8.17, rack=null)}, tierAlias=MEM, mediumType=MEM}]}
```

As shown, this `LICENSE` file has only one block whose id is 16777216,  placed on the K8s node `192.168.8.17`.

We use `kubectl` to identify that the node name is `cn-beijing.192.168.8.17`:

```console
$ kubectl get nodes -o wide | awk '{print $1,$6}'
NAME  INTERNAL-IP
cn-beijing.192.168.8.12  192.168.8.12
cn-beijing.192.168.8.13  192.168.8.13
cn-beijing.192.168.8.14  192.168.8.14
cn-beijing.192.168.8.15  192.168.8.15
cn-beijing.192.168.8.16  192.168.8.16
cn-beijing.192.168.8.17  192.168.8.17
```
  {% endcollapsible %}

  {% collapsible Submit Spark job %}
The following steps will submit a Spark job to the K8s cluster. 
The job is mainly to count the number of occurrences of each word in the `/LICENSE` file in Alluxio.

In the previous step, we see that the blocks contained in the `LICENSE` file are all on the node `cn-beijing.192.168.8.17`. 
In this experiment, we specify the node selector to let the Spark driver and Spark executor run on the node `cn-beijing. 192.168.8.17`. 
Then we verify that the communication between the Spark executor and the Alluxio worker is completed through 
the domain socket when Alluxio's short-circuit function is turned on.

* **Description**: If Alluxio short-circuit is enabled, and the block of the spark executor and the file it wants to access
 (`/LICENSE` in this experiment) are on the same k8s node, then the communication between the Alluxio client and the Alluxio worker 
 on the K8s node is done through domain socket.

First generate a YAML file for submitting the Spark job.
Note that you should update the variables in the below example YAML file in your test.

```console
$ export SPARK_ALLUXIO_IMAGE="spark-alluxio:2.4.6"
$ export ALLUXIO_MASTER="alluxio-master-0"
$ export TARGET_NODE="cn-beijing.192.168.8.17"
$ cat > /tmp/spark-example.yaml <<- EOF
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-count-words
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "$SPARK_ALLUXIO_IMAGE"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.JavaWordCount
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.6.jar"
  arguments:
    - alluxio://${ALLUXIO_MASTER}.alluxio:19998/LICENSE
  sparkVersion: "2.4.6"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
    - name: "alluxio-domain"
      hostPath:
        path: "/tmp/alluxio-domain"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 2.4.6
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
      - name: "alluxio-domain"
        mountPath: "/opt/domain"
    nodeSelector:
      kubernetes.io/hostname: "$TARGET_NODE"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 2.4.6
    nodeSelector:
      kubernetes.io/hostname: "$TARGET_NODE"
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
      - name: "alluxio-domain"
        mountPath: "/opt/domain"
EOF
```

Then, use `sparkctl` to submit the Spark job:
```console
$ sparkctl create /tmp/spark-example.yaml
```

* **Description**: if `sparkctl` is not installed, please refer to [sparkctl](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/tree/master/sparkctl) to install it.
  {% endcollapsible %}

  {% collapsible Check Results %}
After submitting the task, use `kubectl` to check the Spark driver status:

```console
$ kubectl get po -l spark-role=driver
NAME                               READY STATUS    RESTARTS   AGE
spark-alluxio-1592296972094-driver 0/1   Completed 0          4h33m
```

When the status is `Completed`, it means the job has finished.
Now read the spark driver log to find the result:

```console
$ kubectl logs spark-alluxio-1592296972094-driver --tail 20

USE,: 3
Patents: 2
d): 1
comment: 1
executed: 1
replaced: 1
mechanical: 1
20/06/16 13:14:28 INFO SparkUI: Stopped Spark web UI at http://spark-alluxio-1592313250782-driver-svc.default.svc:4040
20/06/16 13:14:28 INFO KubernetesClusterSchedulerBackend: Shutting down all executors
20/06/16 13:14:28 INFO KubernetesClusterSchedulerBackend$KubernetesDriverEndpoint: Asking each executor to shut down
20/06/16 13:14:28 WARN ExecutorPodsWatchSnapshotSource: Kubernetes client has been closed (this is expected if the application is shutting down.)
20/06/16 13:14:28 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
20/06/16 13:14:28 INFO MemoryStore: MemoryStore cleared
20/06/16 13:14:28 INFO BlockManager: BlockManager stopped
20/06/16 13:14:28 INFO BlockManagerMaster: BlockManagerMaster stopped
20/06/16 13:14:28 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
20/06/16 13:14:28 INFO SparkContext: Successfully stopped SparkContext
20/06/16 13:14:28 INFO ShutdownHookManager: Shutdown hook called
20/06/16 13:14:28 INFO ShutdownHookManager: Deleting directory /var/data/spark-2f619243-59b2-4258-ba5e-69b8491123a6/spark-3d70294a-291a-423a-b034-8fc779244f40
20/06/16 13:14:28 INFO ShutdownHookManager: Deleting directory /tmp/spark-054883b4-15d3-43ee-94c3-5810a8a6cdc7
```

Finally, we login to the Alluxio master and check the statistics on the relevant indicators:

```console
$ kubectl exec -ti alluxio-master-0 -n alluxio bash

bash-4.4# alluxio fsadmin report metrics
Cluster.BytesReadRemote  (Type: COUNTER, Value: 0B)
Cluster.BytesReadRemoteThroughput  (Type: GAUGE, Value: 0B/MIN)
Cluster.BytesReadDomain  (Type: COUNTER, Value: 237.66KB)
Cluster.BytesReadDomainThroughput  (Type: GAUGE, Value: 47.53KB/MIN)
......
```

From the metrics above,
**BytesReadRemote** and **BytesReadRemoteThroughput** represent data transmission via the network stack; **BytesReadDomain** and **BytesReadDomainThroughput** represent data transmission via domain socket. 
You can observe that all data is transferred via the domain socket.
  {% endcollapsible %}
{% endaccordion %}
