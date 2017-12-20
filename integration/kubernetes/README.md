## Pre-requisites

- Alluxio workers use `emptyDir` volumes with `sizeLimit`; an alpha feature in Kubernetes 1.8. Please ensure the feature is enabled.

- Nodes running Alluxio workers require a manual step for using domain sockets. Execute the following on host nodes.
```bash
mkdir /tmp/domain
chmod a+w /tmp/domain
touch /tmp/domain/d
chmod a+w /tmp/domain/d
```

- Alluxio masters use a persistent volume for storing the journal. Create a persistent volume using the template.
```bash
mv alluxio-journal-volume.yaml.template alluxio-journal-volume.yaml
# Create persistent volume
kubectl create -f alluxio-journal-volume.yaml
# Inspect status
kubectl get pv alluxio-journal-volume
```

## Create spec and configuration from templates

Create Alluxio master and worker specs (modify defaults)
```bash
mv alluxio-master.yaml.template alluxio-master.yaml
mv alluxio-worker.yaml.template alluxio-worker.yaml
```

Create configuration file
```bash
mv conf/alluxio.properties.template conf/alluxio.properties
```

## Deploy

Create configuration maps
```bash
kubectl create configmap alluxio-config --from-file=ALLUXIO_CONFIG=conf/alluxio.properties
```

Start Alluxio:
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
