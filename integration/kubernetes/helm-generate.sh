#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

function printUsage {
  echo "Usage: MODE [UFS]"
  echo
  echo "MODE is one of:"
  echo -e " single-ufs        \t Generate Alluxio YAML templates for a single-master environment using UFS journal."
  echo -e " multi-embedded    \t Generate Alluxio YAML templates with multiple masters using embedded journal."
  echo -e " all               \t Generate Alluxio YAML templates for all combinations."
  echo
  echo "UFS is only for single-ufs mode. It should be one of:"
  echo -e " local             \t Use a local destination for UFS journal."
  echo -e " hdfs              \t Use HDFS for UFS journal"
}

function generateTemplatesWithConfig {
  echo "Generating templates into $dir"
  config=./$dir/config.yaml
  if [[ ! -f "$config" ]]; then
    echo "A config file $config is needed in $dir!"
    echo "See https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store"
    echo "for the format of config.yaml."
    exit 1
  fi

  generateConfigTemplatesWithConfig
  generateMasterTemplatesWithConfig
  generateWorkerTemplatesWithConfig
  generateFuseTemplatesWithConfig

#  helm template helm/alluxio/ -x templates/alluxio-master.yaml -f ./$dir/config.yaml > "$dir/alluxio-master.yaml.template"
#  helm template helm/alluxio/ -x templates/alluxio-worker.yaml -f ./$dir/config.yaml > "$dir/alluxio-worker.yaml.template"
#  helm template helm/alluxio/ -x templates/alluxio-configMap.yaml -f ./$dir/config.yaml > "$dir/alluxio-configMap.yaml.template"
}

function generateConfigTemplatesWithConfig {
  echo "Generating configmap templates into $dir"
  helm template helm-chart/alluxio/ -x templates/config/alluxio-conf.yaml -f $dir/config.yaml > "$dir/alluxio-configmap.yaml.template"
}

function generateMasterTemplatesWithConfig {
  echo "Generating master templates into $dir"
  helm template helm-chart/alluxio/ -x templates/master/statefulset.yaml -f $dir/config.yaml > "$dir/alluxio-master-statefulset.yaml.template"
  helm template helm-chart/alluxio/ -x templates/master/service.yaml -f $dir/config.yaml > "$dir/alluxio-master-service.yaml.template"
  helm template helm-chart/alluxio/ -x templates/master/journal-pv.yaml -f $dir/config.yaml > "$dir/alluxio-master-journal-pv.yaml.template"
  helm template helm-chart/alluxio/ -x templates/master/journal-pvc.yaml -f $dir/config.yaml > "$dir/alluxio-master-journal-pvc.yaml.template"
  # TODO(jiacheng): format job
}

function generateWorkerTemplatesWithConfig {
  echo "Generating worker templates into $dir"
  helm template helm-chart/alluxio/ -x templates/worker/daemonset.yaml -f $dir/config.yaml > "$dir/alluxio-worker-daemonset.yaml.template"
}

function generateFuseTemplatesWithConfig {
  echo "Generating fuse templates"
  helm template helm-chart/alluxio/ -x templates/fuse/daemonset.yaml -f $dir/config.yaml > "alluxio-fuse-daemonset.yaml.template"
  # TODO(jiacheng): fuse client
}

function generateMasterServiceTemplatesWithConfig {
  helm template helm-chart/alluxio/ -x templates/master/service.yaml -f $dir/config.yaml > "$dir/alluxio-master-service.yaml.template"
}

#function generateVolumeTemplatesWithConfig {
#  echo "Generating persistent volume templates into $dir"
#  helm template helm/alluxio/ -x templates/alluxio-journal-volume.yaml -f ./$dir/config.yaml > "$dir/alluxio-journal-volume.yaml.template"
#}

#function generateJobTemplatesWithConfig {
#  echo "Generating job templates into $dir"
#  helm template helm/alluxio/ -x templates/alluxio-format-master.yaml -f ./$dir/config.yaml > "$dir/alluxio-format-master.yaml.template"
#}

function generateSingleUfsTemplates {
  echo "Target FS $1"
  targetFs=$1
  case $targetFs in
    "local")
      echo "Using local journal"
      dir="singleMaster-localJournal"
      generateTemplatesWithConfig
      ;;
    "hdfs")
      echo "Journal UFS $ufs"
      dir="singleMaster-hdfsJournal"
      generateTemplatesWithConfig
      ;;
    *)
      echo "Unknown Journal UFS type $ufs"
      printUsage
      exit 1
  esac
}

function generateMultiEmbeddedTemplates {
  dir="multiMaster-embeddedJournal"
  generateTemplatesWithConfig
}

function generateAllTemplates {
  generateSingleUfsTemplates "local"
  generateSingleUfsTemplates "hdfs"
  generateMultiEmbeddedTemplates
}

function main {
  mode=$1
  case $mode in
    "single-ufs")
      echo "Generating templates for $mode"
      if ! [ $# -eq 2 ]; then
        printUsage
        exit 1
      fi
      ufs=$2
      generateSingleUfsTemplates "$ufs"
      ;;
    "multi-embedded")
      echo "Generating templates for $mode"
      generateMultiEmbeddedTemplates
      ;;
    "all")
      echo "Generating templates for all combinations"
      generateAllTemplates
      ;;
    *)
      echo "Unknown mode $mode"
      printUsage
      exit 1
  esac
}

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  printUsage
  exit 1
fi

main "$@"
