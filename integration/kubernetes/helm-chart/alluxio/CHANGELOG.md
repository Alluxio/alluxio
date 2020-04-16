0.1.0

- Init support
- Modularized the directory structure
- Java and docker native integration(Consider memory, https://developers.redhat.com/blog/2017/03/14/java-inside-docker/)
- Made more configurable, like node selector, tolerance

0.2.0

- Two choices to decide short circuit: copying from directory directly or unix socket
- Simplified tiered storage (Combine alluxio properties and Persistent volume)
- Use init container to do format /no format with intelligence
- Fuse daemonset

0.3.0

- Both support one layer contains multiple storages(https://docs.alluxio.io/os/user/stable/en/advanced/Alluxio-Storage-Management.html#single-tier-storage) and multiple layers

0.4.0

- Added local time zone

0.5.0

- Merged with Alluxio Helm chart structure
- Added multiple template directories for sample use cases
- Merged helm-generate.sh which generates the templates 


0.5.1

- Fixed tiered store issue
- Fixed the issue of the user group of worker is not configurable

0.5.2

- Fixed apiVersion key in Chart.yaml

0.5.3

- Changed to using one single StatefulSet for all master Pods
- Changed embedded journal from emptyDir to auto-created PVC
- Changed values.yaml structure for ports and update configmap
- Define alluxio.master.hostname individually for each Pod in env variable, and update configmap
- Moved a few duplicated blocks into _helpers.tpl

0.5.4

- Updated the journal formatting Job logic
- Misc formatting and parameters updates

0.5.5

- Removed extra resources created by Helm install https://github.com/Alluxio/alluxio/issues/10321

0.5.6

- Added readiness and liveness probes for master and worker containers
- Removed formatting script under format/

0.5.7

- Moved journal formatting from job/format-journal-job.yaml to initContainer in master/statefulset.yaml
- Changed the master RocksDB metastore volume from emptyDir to PVC
- Added support for using PVC for tiered storage

0.5.8

- Added option to disable worker short-circuit
- Changed worker domain socket volume from hostPath to PVC
- Changed hostNetwork to false
- Added alluxio.worker.container.hostname property to use podIP
- Added selector labels to worker domain socket PVC
