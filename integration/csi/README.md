# Alluxio CSI

This module implement container storage interface(https://github.com/container-storage-interface/spec) for Alluxio.

## Requirements

Kubernetes 1.14 or higher, RBAC enabled in API server(https://kubernetes.io/docs/reference/access-authn-authz/rbac/).

## Usage


### Build docker image

Please run `docker build . -t alluxio/alluxio-csi:<version_tag>` to build CSI docker image. Alluxio doesn't provide official CSI docker image currently.
You need to build the image by yourself.

### Deploy

Please use `helm-generate.sh` to generate related templates. All CSI related templates should under `integration/kubernetes/<deploy-mode>/csi` folder.

You need to deploy `alluxio-csi-controller`, `alluxio-csi-nodeplugin`, `alluxio-csi-driver` before mount volume via CSI.

We provide two types of provisioning methods. For static provisioning, you need to create `PersistentVolume` and `PersistentVolumeClaim` first.
For dynamic provisioning, you need to create `StorageClass` and  `PersistentVolumeClaim`.

All these examples will be generated after running `helm-generate.sh`.

### Configuration

You can customize alluxio volumes via several configurations.

The options you can customize:
| Options | Description |
| --- | --- |
| `alluxioPath` | The path in alluxio |
| `javaOptions` | The customized options which will be passes to fuse daemon |
| `mountOptions` | Alluxio fuse mount options |

If you use dynamic provisioning, please put your cutomized parameters under `StorageClass.parameters` and `StorageClass.mountOptions`.

An example of Alluxio StorageClass Spec:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: alluxio
provisioner: alluxio
parameters:
  alluxioPath: /data
  javaOptions: "-Dalluxio.user.metadata.cache.enabled=true "
volumeBindingMode: Immediate
mountOptions:
  - kernel_cache
  - allow_other
  - entry_timeout=36000
  - attr_timeout=36000
  - max_readahead=0
```

If you use static provisioning, you can customize these options in `PersistentVolume.spec.csi.volumeAttributes` and `PersistentVolume.spec.mountOptions`

An example of Alluxio PersistentVolume Spec:
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: alluxio-pv
  labels:
    name: alluxio-pv
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: 100Gi
  csi:
    driver: alluxio
    volumeHandle: alluxio
    volumeAttributes:
      alluxioPath: /data
      javaOptions: "-Dalluxio.user.metadata.cache.enabled=true "
  mountOptions:
    - kernel_cache
    - allow_other
    - entry_timeout=36000
    - attr_timeout=36000
```
