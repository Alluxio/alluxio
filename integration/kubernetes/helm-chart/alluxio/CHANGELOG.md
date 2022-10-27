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

- Add Metrics configurations including Prometheus
- Add a table of keys and default values for the Helm templates in README

0.6.13

- Add remote logger for Alluxio services, putting the centralized logs in emptyDir/hostPath/PVC

0.6.14

- Migrate master StatefulSet and worker DaemonSet securityContext to Pod-level (see Issue [#13096](https://github.com/Alluxio/alluxio/issues/13096))

0.6.15

- Fix incorrect indentation in logserver secret volume mount

0.6.16

- Change helm-chart fuse hostPath type from File to CharDevice

0.6.17

- Add hostAliases in Master and Worker Pods

0.6.18

- Add support for Node tolerations

0.6.19

- Add serviceAccountName in Master, Worker, and FUSE Pods

0.6.20

- Add Master StatefulSet podManagementPolicy Parallel (see Issue [#13323](https://github.com/Alluxio/alluxio/issues/13323))

0.6.21

- Change logserver PVC default selectors to empty, so dynamic provisioning works by default configuration.

0.6.22

- Enable configuring logserver Deployment strategy to address [#13422](https://github.com/Alluxio/alluxio/issues/13422)

0.6.23

- Add Alluxio CSI support

0.6.24

- Fix Alluxio CSI `nodeplugin.yaml` indentation

0.6.25

- Fix Alluxio CSI `nodeplugin.yaml` indentation, add support for dns policy & change CSI log level

0.6.26

- Add livenessProbe and readinessProbe to values.yaml to allow for overriding

0.6.27

- Enable Fuse process embedded in worker process

0.6.28

- Fix Alluxio CSI docker image reference. Add CSI README.md instructions.

0.6.29

- Add startupProbe and checking for web port when using HA without embedded journal

0.6.30

- Add ImagePullSecrets Pod spec option

0.6.31

- Update CSI launch script

0.6.32

- Add toggles to disable master/worker resource deployment

0.6.33

- Fix CSI typo. Upgrade CSI driver and provisioner. Improve CSI static pvc template.
- Fix typo in specifying fuse mount options.

0.6.34

- Enable mounting a specific directory in Alluxio through Fuse
- Fixes a typo in rendering the fuse mount directory.

0.6.35

- Remove usage of Helm hook annotations in Charts

0.6.36

- Fix volumeMounts indentations in master statefulset

0.6.37

- Fix jobMaster.env indentations in master statefulset

0.6.38

- Fix MOUNT_POINT env in fuse daemonset

0.6.39

- Fix CSI controller rbac rule not specifying namespace.
- Fix CSI driver compatibility issue under kubernetes 18+ version.

0.6.40

- Fix incorrect directory when mounting & formatting master journal volume

0.6.41

- Add property to enable launching Fuse process in a separate pod in CSI
- Fix default CSI accessMode

0.6.42

- Rename mountPath to mountPoint in fuse
- Require non-empty values for fuse mountPoint
- Passing fuse mountPoint and alluxioPath into containers as args instead of env variables

0.6.43

- Rename static pvc metadata.name in csi

0.6.44

- Add liveness probe to fuse pod

0.6.45

- Move helm-generate.sh script and README.md inside helm chart
- The 3 deploy mode templates are moved inside helm-chart directory from base directory
- The generated fuse yaml templates are moved inside fuse/ directory from base directory
- The generated csi yaml templates are moved inside csi/ directory from base directory
- Fuse and csi yaml templates depend on default values.yaml file instead of config files generated by helm-generate.sh script

0.6.46

- Support ConfigMap mounts and allow users to mount ConfigMap volumes similar to Secrets

0.6.47

- Support alluxio proxy

0.6.48

- Replace all default space size to "i" unit to avoid the unit discrepancy between Alluxio and Kubernetes

0.6.49

- Remove dummy example alluxio-fuse-client

0.6.50

- Remove dummy example nginx from CSI

0.6.51

- Remove alluxio-fuse-client generation from helm-generate.sh script
- Fix global mount indentation issue in master

0.6.52

- Fix generating proxy templates when running helm-generate.sh script
- Fix helm-generate.sh script logging

0.6.53

- Improve indentation in worker daemonset template
- Configure ports in master service following values.yaml
