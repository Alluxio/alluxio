# Alluxio Helm Chart

## Pre-requisites
Refer to the [helm](https://helm.sh/docs/using_helm/#installing-helm) documentation to install helm locally.
For example, on Mac OS X install as follows:
```bash
brew install kubernetes-helm
```

## Generate kubectl yaml templates from Helm chart

To remove redundancy, Helm chart is used to generate the templates which can be used to deploy Alluxio
using `kubectl` directly. 

## Directly use existing YAML templates for your use case

Alluxio comes with a few sets of YAML templates for some common use cases. They are in *singleMaster-localJournal*, *singleMaster-hdfsJournal* and *multiMaster-embeddedJournal* directories.
*singleMaster* means the templates generate 1 Alluxio master process, while *multiMaster* means 3.
*embedded* and *ufs* are the 2 [journal modes](https://docs.alluxio.io/ee/user/stable/en/operation/Journal.html) that Alluxio supports.

*singleMaster-localJournal* directory gives you the necessary Kubernetes ConfigMap, 1 Alluxio master process and 2 Alluxio workers.
The Alluxio master writes journal to the PersistentVolume defined in *alluxio-journal-volume.yaml.template* so you will need that too.

*multiMaster-EmbeddedJournal* directory gives you the Kubernetes ConfigMap, 3 Alluxio masters and 2 Alluxio workers.
The Alluxio masters each writes to its `alluxio-journal-volume`, which is an `emptyDir` that gets wiped out when the Pod goes down.

*singleMaster-hdfsJournal* directory gives you the Kubernetes ConfigMap, 3 Alluxio masters with 2 workers.
The journals are in a shared UFS location. In the templates we use HDFS as the UFS.
There are some extra setup you have to do before the Alluxio processes are able to connect to your HDFS.
The next section will go over what extra setup is needed for HDFS connection.

### Modify *singleMaster-hdfsJournal* template for HDFS connection

This section explains how to set up the templates for connecting to HDFS.
In this case the HDFS is accessible to your Kubernetes cluster, but is not managed by Kubernetes. 

#### Step 1: Add `hostAliases` for your HDFS connection. 

Kubernetes Pods don't recognize network hostnames that are not managed by Kubernetes (not Kubernetes Services), unless specified by 
[Kubernetes add hostAliases](https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/#adding-additional-entries-with-hostaliases)

For example if your HDFS service can be reached at `hdfs://hdfs-cluster:9000` where `hdfs-cluster` is a hostname,
you will need to add the `hostAliases` in the `spec` for this Pod (Note that `hostAliases` is an array.). You will need to find the IP address for this hostname `hdfs-cluster`.

```yaml
spec:
  hostAliases:
  - ip: "ip for hdfs-cluster"
    hostnames:
    - "hdfs-cluster"
```

For the case of a StatefulSet or ReplicaSet as used in alluxio-master.yaml.template and alluxio-worker.yaml.template,
`hostAliases` section should be added to each section of `spec.template.spec` like below.

```yaml
kind: StatefulSet
metadata:
  name: alluxio-master-0
spec:
  ...
  serviceName: "alluxio-master-0"
  replicas: 1
  template:
    metadata:
      labels:
        app: alluxio-master-0
    spec:
      hostAliases:
      - ip: "ip for hdfs-cluster"
        hostnames:
        - "hdfs-cluster"
```

#### Step 2: Create Kubernetes Secret for HDFS configuration files. 

You need to run the following command to create a Kubernetes Secret for Alluxio to use.

```bash
kubectl create secret generic alluxio-hdfs-config --from-file={$path-to}/core-site.xml --from-file={$path-to}/hdfs-site.xml
```

These two config files will be named `alluxio-hdfs-config` and referred to in *alluxio-master.yaml.template*.
Alluxio processes need the HDFS configuration files to work with it properly, and this is controlled by property `alluxio.underfs.hdfs.configuration`.
For more details see [Create Kubernetes Secret for HDFS](https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store).
<!-- TODO(jiacheng): Use doc for a stable version -->

#### Step 3: Modify *alluxio-configMap.yaml.template*.

Now that your pods know how to talk to your HDFS service,
you need to update `-Dalluxio.master.journal.folder` and `alluxio.master.mount.table.root.ufs` to point them to the correct HDFS destination. 

You will also see the `alluxio.underfs.hdfs.configuration` that is pointing to the Kubernetes Secret you just created.


### Update Helm templates

If the existing 3 directories do not meet your specific use case,
you can always either find the templates that are the closest to your case and modify them,
or use Helm to generate the templates with parameters.
In fact, templates in the 3 directories are generated with Helm templates defined in *helm/alluxio/templates*.

The following section will require some prerequisite knowledge about [Helm Chart Templates](https://helm.sh/docs/chart_template_guide/#the-chart-template-developer-s-guide)

#### Step 1: Update values.yaml

The Helm templates are configured with `helm/alluxio/values.yaml`. 
You should make sure the following 2 values align with the templates you want to generate:

```yaml
# Example: For singleMaster localJournal
resources:
  masterCount: 1 # For multiMaster mode increase this to >1
  ...
journal:
  type: "UFS"
  ufsType: "local" # Ignored if type is "EMBEDDED"
  folder: "/journal"
```

The 2 values above are the default for *singleMaster* setup. For *multiMaster* mode, `resources.masterCount` should be greater than 1.

#### Step 2: Create config.yaml

A `config.yaml` supplies additional parameters to Helm. If you you are generating templates for *singleMaster-localJournal* or *multiMaster-embeddedJournal* and you have no extra parameter to use,
you can skip this step.

For *singleMaster-hdfsJournal*, you must configure `config.yaml` for the extra parameters for the UFS.
See [Create Helm config.yaml for HDFS](https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store)
for what the config.yaml will need for using HDFS.

#### Step 3: Generate templates

Now that you have set up all that Helm needs, you can use the helper script `helm-generate.sh` to help you quickly generate the templates(overwriting the existing ones).
The script takes only one argument, the target setup name.

```bash
# Example: This will regenerate the templates for singleMaster-localJournal
bash helm-generate.sh single-ufs local
# Example: This will regenerate the templates for multiMaster-embeddedJournal
bash helm-generate.sh multi-embedded

```

For *singleMaster-hdfsJournal* the script relies on the `config.yaml` file you just created under the hood.

```bash
# Example: This will regenerate the templates for hdfsJournal
bash helm-generate.sh single-ufs hdfs
```

Feel free to configure and run Helm yourself if the helper script does not satisfy what you need.
