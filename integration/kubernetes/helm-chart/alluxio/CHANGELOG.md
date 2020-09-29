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

0.5.9

- Refactored configmap to generate config properties in a list
- Changed JVM options from one string to a list
- Supported Helm version upgraded to 3.X

0.6.0

- Fix alluxio-fuse container fail to restart when it exited with error

0.6.1

- Infer hostNetwork, dnsPolicy and domain socket volume type based on the user

0.6.2

- Fix alluxio chart failed to deploy with helm when "fuse.enabled" is true in values.yaml(issue: #11542)

0.6.3

- Enabled worker domain socket to choose between hostPath and PVC
- Refactored some worker domain socket PVC properties to be consistent with documentation
- Enabled master metastore to choose between emptyDir and PVC
- Enabled master journal to choose between emptyDir and PVC
- Moved metastore configuration properties to the root level, to be the same as journal
- Removed inferring hostNetwork, dnsPolicy and domain socket from whether user is root
- Added inferring dnsPolicy from hostNetwork
- Fixed one typo in ALLUXIO_CLIENT_JAVA_OPTS for FUSE

0.6.4

- Fixed Fuse crash issue
- Changed master service to headless from NodePort
- Made the single master access itself without service

0.6.5

- Removed alluxio.worker.hostname from ALLUXIO_JAVA_OPTS for Fuse
- Increase the default memory limit to match the default xmx
- Added hostPID for using Java profile

0.6.6

- Removed obsolete master journal formatting job configuration properties
- Set hostPID default to false
- Increase the default memory usage for Fuse

0.6.7

- Add environment variables to master, worker, fuse

0.6.8

- Fixed parsing issue with multiple medium types for tiered storage #11778

0.6.9

- Pass alluxio.user.hostname via ALLUXIO_USER_JAVA_OPTS for FUSE

0.6.10

- Change liveness and readiness probes to TCP probes

0.6.11

- Pass alluxio.user.hostname via ALLUXIO_FUSE_JAVA_OPTS for FUSE

0.6.12

- Added options to set up proxy service next to workers
