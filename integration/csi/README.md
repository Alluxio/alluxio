## What is Alluxio
[Alluxio](https://www.alluxio.io) (formerly known as Tachyon)
is a virtual distributed storage system. It bridges the gap between
computation frameworks and storage systems, enabling computation applications to connect to
numerous storage systems through a common interface. Read more about
[Alluxio Overview](https://docs.alluxio.io/os/user/stable/en/Overview.html).

The Alluxio project originated from a research project called Tachyon at AMPLab, UC Berkeley,
which was the data layer of the Berkeley Data Analytics Stack ([BDAS](https://amplab.cs.berkeley.edu/bdas/)).
For more details, please refer to Haoyuan Li's PhD dissertation
[Alluxio: A Virtual Distributed File System](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html).

## Container Storage Interface for Alluxio

This repository contains the CSI implementation to provide POSIX access to Alluxio in
containerized environments such as Kubernetes.

## How to build

Build alluxio-csi docker image

`docker build . -t alluxio/alluxio-csi:<version_tag>`

## Example

### Deploy

Go to `deploy` folder, Run following commands:
```bash
kubectl apply -f csi-alluxio-driver.yml
kubectl apply -f csi-alluxio-daemon.yml
kubectl apply -f csi-alluxio-controller.yml
``` 

### Example Nginx application
The `/examples` folder contains `PersistentVolume`, `PersistentVolumeClaim`, `StorageClass` and an nginx `Pod` mounting the alluxio volume under `/data`.

You will need to update the alluxio `MASTER_HOST_NAME` and the share information under `volumeAttributes` in `alluxio-pv.yml` file to match your alluxio master configuration.

If you want to use dynamic provisioning, please update the alluxio `MASTER_HOST_NAME` under `parameters` in `alluxio-storageclass.yml`.

Run following commands to create nginx pod mounted with alluxio volume:
```bash
kubectl apply -f alluxio-pv.yml
kubectl apply -f alluxio-pvc.yml
kubectl apply -f nginx.yml
```

For dynamic provisioning, please run following commands:
```bash
kubectl apply -f alluxio-storageclass.yml
kubectl apply -f alluxio-pvc-storageclass.yml
kubectl apply -f nginx.yml
```

