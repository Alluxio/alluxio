# Alluxio Helm Chart

## Pre-requisites
Refer to the [helm](https://helm.sh/docs/using_helm/#installing-helm) documentation to install helm locally.
For example, on Mac OS X install as follows:
```bash
brew install kubernetes-helm
```

## Generate kubectl yaml templates from Helm chart

To remove redundancy, the helm chart is used to generate the templates which can be used to deploy Alluxio
using `kubectl` directly. To generate the templates, execute the following:
```bash
helm template helm/alluxio/ -x templates/alluxio-master.yaml > alluxio-master.yaml.template
helm template helm/alluxio/ -x templates/alluxio-worker.yaml > alluxio-worker.yaml.template
helm template helm/alluxio/ -x templates/alluxio-configMap.yaml > alluxio-configMap.yaml.template
```
