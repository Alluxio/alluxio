---
layout: global
title: Alluxio Cluster Membership Management
---

In Alluxio 2.x, masters served the purpose of keeping a view of workers list and liveness. As of 304, Membership module is introduced to replace worker-registration on master.
Membership module offers the following way of managing worker members. 

1) **STATIC** - uses a static config file(default is $ALLUXIO_HOME/conf/workers) to configure a list of workers hostnames to form the alluxio cluster, it doesn't provide membership capability as to track any new member joining / leaving, member liveliness. It's merely used as a simple quickstart deployment way to spin up a DORA alluxio cluster.
2) **ETCD** - uses a pre-configured standalone etcd cluster to manage worker membership. On startup, worker will register itself to etcd, and then keeping its liveness to etcd throughout its process lifetime.

These are the capability two of these modules can do:
|                             | STATIC  |  ETCD  |
| ----------------------------| ------- | ------ |
| Get full worker list        |  YES    |  YES   |
| Get live/failed worker list |  NO     |  YES   |


## Deployment

### STATIC
Use a static file, following the format of conf/workers.
Append the hostname of each node into conf/workers. Append the hostname of each worker node to a new line into conf/workers. Comment out localhost if necessary. For example,
```
# An Alluxio Worker will be started on each of the machines listed below. 
# localhost
ec2-1-111-11-111.compute-1.amazonaws.com
ec2-2-222-22-222.compute-2.amazonaws.com
```
And configure the alluxio-site.properties with:
``` 
alluxio.worker.membership.manager.type=STATIC
alluxio.worker.static.config.file=<absolute_path_to_static_config_workerlist_file>
```
or just 
```    
alluxio.worker.membership.manager.type=STATIC
```
then conf/workers will be used.        


### ETCD
Depending on the deployment environment, Bare Metal or K8s, users could setup etcd cluster and alluxio cluster individually, or through helm install with alluxio's k8s operator for a one-click install for both.

#### 1) Bare Metal
Set up etcd cluster, refer to etcd doc here: https://etcd.io/docs/v3.4/op-guide/clustering/
For versions, we recommend using V3 etcd version as we don't support V2 versions. But we don't have a specific requirement of which V3 version as of now.

e.g. Say we have an etcd 3 node setup:

Name | Address | Hostname
-- | -- | --
infra0 | 10.0.1.10 | infra0.example.com
infra1 | 10.0.1.11 | infra1.example.com
infra2 | 10.0.1.12 | infra2.example.com

<br class="Apple-interchange-newline">

Configure alluxio-site.properties:
```
alluxio.worker.membership.manager.type=ETCD
alluxio.etcd.endpoints=http://infra0.example.com:2379,http://infra1.example.com:2379,http://infra2.example.com:2379
```
> **[NOTICE]** As etcdmembership module relies on etcd's high availability to provide membership service, include ALL the etcd cluster nodes in configuration (or at lease all initial ones if new nodes has been bootstrapped into etcd later) to allow etcdmembership module to redirect connection to etcd leader automatically.

After spin up alluxio workers, use `bin/alluxio info nodes` to check status of worker registration.
```
WorkerId	Address	Status
6e715648b6f308cd8c90df531c76a028	127.0.0.1:29999	ONLINE
```

#### 2) K8s
For k8s deployment with alluxio k8s operator, refer to {{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.md' }}
we can spin up a DORA alluxio cluster along with etcd cluster pod(s) with helm (refer to {{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.md#helm' }})

To pull etcd dependency for helm repo, do
```shell
$ helm dependency update 
```

To configure alluxio with a single pod etcd cluster, enable etcd component in k8s-operator/deploy/charts/alluxio/config.yaml
```
image: <docker_username>/<image-name>
imageTag: <tag>
dataset:
  path: <ufs path>
  credentials: # s3 as example. Leave it empty if not needed. 
    aws.accessKeyId:xxxxxxxxxx
    aws.secretKey: xxxxxxxxxxxxxxx
  etcd:
    enabled: true
```
then under `k8s-operator/deploy/charts/alluxio/` do:
```
$ helm install <cluster name> -f config.yaml .
```
then with `$kubectl get pods` you can see etcd pod:
```                                       
NAME                                    READY   STATUS     RESTARTS   AGE
<cluster name>-etcd-0                   0/1     Running    0          3s
```
Other values to configure for etcd, setting replicaCount to 3 will start a 3-member etcd cluster
```
etcd:
  enabled: true
  replicaCount: 3
```
More info on the parameters, refer to : https://artifacthub.io/packages/helm/bitnami/etcd#etcd-statefulset-parameters

For detailed introduction on how the Registration/ServiceDiscovery is done with Etcd, check this doc: https://github.com/Alluxio/alluxio/wiki/Etcd-backed-membership




