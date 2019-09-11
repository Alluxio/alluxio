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

## Directly use existing YAML templates for your deployment scenario

Alluxio comes with a few sets of YAML templates for some common deployment scenarios.
They are in *singleMaster-localJournal*, *singleMaster-hdfsJournal* and *multiMaster-embeddedJournal* directories.
See [Find existing YAML templates for your deployment scenario in Alluxio documentation](https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#find-existing-yaml-templates-for-your-deployment-scenario)

### Update Helm templates

If the existing 3 directories do not meet your specific deployment scenario,
you can always either find the templates that are the closest to your scenario and modify them,
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
