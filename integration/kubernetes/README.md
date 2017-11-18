## Pre-requisites:

Alluxio workers use `emptyDir` volumes with `sizeLimit`; which is an alpha feature in Kubernetes 1.8. Please ensure feature is enabled.

Nodes running Alluxio workers require a manual step for using domain sockets. Execute the following on host nodes.
```bash
mkdir /tmp/domain
chmod a+w /tmp/domain
touch /tmp/domain/d
chmod a+w /tmp/domain/d
```

## Deploy

Create configuration maps
```bash
kubectl create configmap alluxio-common-config --from-file=ALLUXIO_COMMON_CONFIG=conf/common.properties
kubectl create configmap alluxio-master-config --from-file=ALLUXIO_MASTER_CONFIG=conf/master.properties
kubectl create configmap alluxio-worker-config --from-file=ALLUXIO_WORKER_CONFIG=conf/worker.properties
```

Start daemons:
```bash
kubectl create -f alluxio-master.yaml
kubectl create -f alluxio-worker.yaml
```

## Verify 
To run the Alluxio CLI:
```bash
kubectl exec -ti alluxio-master-0 /bin/bash

cd /opt/alluxio
./bin/alluxio runTests
```
