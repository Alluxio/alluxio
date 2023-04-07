# Alluxio Helm Chart

## Pre-requisites
### Helm installation
Refer to the [helm](https://helm.sh/docs/using_helm/#installing-helm) documentation to install helm locally.
For example, on Mac OS X install as follows:
```bash
brew install kubernetes-helm
```

### Persistent volume for Alluxio master journal
If you are using local UFS journal or embedded journals, Alluxio masters need persistent volumes for storing the journals.
In that case, you will need to provision one persistent volume for one master replica.
Alluxio master Pods defined in `{templicateDir}/master/alluxio-master-statefulset.yaml.template`
will need existing PVs to run.

For details of the persistent volumes please refer to `./helm-chart/alluxio/README.md`.

## Generate kubectl yaml templates from Helm chart

To remove redundancy, Helm chart is used to generate the templates which can be used to deploy Alluxio
using `kubectl` directly. 

## Directly use existing YAML templates for your deployment scenario

Alluxio comes with a few sets of YAML templates for some common deployment scenarios.
They are in *singleMaster-localJournal*, *singleMaster-hdfsJournal* and *multiMaster-embeddedJournal* directories.

### Update Helm templates

If the existing 3 directories do not meet your specific deployment scenario,
you can always either find the templates that are the closest to your scenario and modify them,
or use Helm to generate the templates with parameters.
In fact, templates in the 3 directories are generated with Helm templates defined in *helm/alluxio/templates*.

The following section will require some prerequisite knowledge about [Helm Chart Templates](https://helm.sh/docs/chart_template_guide/#the-chart-template-developer-s-guide)

#### Step 1: Update config.yaml

The Helm templates are configured with `alluxio/values.yaml`.
We use the `config.yaml` in the corresponding template directory to override the parameters in `alluxio/values.yaml`.
For example `singleMaster-localJournal/config.yaml` sets parameters specific to its deployment scenario.

You should make sure the following 2 values align with the templates you want to generate:

```yaml
# Example: For singleMaster localJournal
resources:
  masterCount: 1 # For multiMaster mode increase this to >1

journal:
  type: "UFS"
  ufsType: "local" # Ignored if type is "EMBEDDED"
  folder: "/journal"
```

The 2 values above are the default for *singleMaster* setup. For *multiMaster* mode, `resources.masterCount` should be greater than 1.

For *singleMaster-hdfsJournal*, `config.yaml` contains some extra parameters for HDFS.
See `singleMaster-hdfsJournal/config.yaml` for the example and
[Create Helm config.yaml for HDFS](https://docs.alluxio.io/os/user/edge/en/kubernetes/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store)
for documentation.

#### Step 2: Generate templates

Now that you have set up all that Helm needs, you can use the helper script `helm-generate.sh` to help you quickly generate the templates(overwriting the existing ones).
The script takes only one argument, the target setup name.

```bash
# Example: This will regenerate the templates for singleMaster-localJournal
bash helm-generate.sh single-ufs local
# Example: This will regenerate the templates for multiMaster-embeddedJournal
bash helm-generate.sh multi-embedded

```

For all modes the script relies on the `config.yaml` file in the corresponding template directory.

```bash
# Example: This will regenerate the templates for hdfsJournal
bash helm-generate.sh single-ufs hdfs
```

You can also regenerate all templates.

```bash
# Example: This will regenerate templates for all combinations
bash helm-generate.sh all
```

Feel free to configure and run Helm yourself if the helper script does not satisfy what you need.
