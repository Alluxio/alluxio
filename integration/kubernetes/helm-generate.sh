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
  echo
  echo "UFS is only for single-ufs mode. It should be one of:"
  echo -e " local             \t Use a local destination for UFS journal."
  echo -e " hdfs              \t Use HDFS for UFS journal"
}

function generatePodTemplates {
  echo "Generating templates into $dir"
  helm template helm/alluxio/ -x templates/alluxio-master.yaml > "$dir/alluxio-master.yaml.template"
  helm template helm/alluxio/ -x templates/alluxio-worker.yaml > "$dir/alluxio-worker.yaml.template"
  helm template helm/alluxio/ -x templates/alluxio-configMap.yaml > "$dir/alluxio-configMap.yaml.template"
}

function generatePodTemplatesWithConfig {
  echo "Generating templates into $dir"
  config=./config.yaml
  if [[ ! -f "$config" ]]; then
    echo "A config file $config is needed to generate templates for HA mode UFS!"
    echo "See https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store"
    echo "for the format of config.yaml."
    exit 1
  fi
  helm template helm/alluxio/ -x templates/alluxio-master.yaml -f ./config.yaml > "$dir/alluxio-master.yaml.template"
  helm template helm/alluxio/ -x templates/alluxio-worker.yaml -f ./config.yaml > "$dir/alluxio-worker.yaml.template"
  helm template helm/alluxio/ -x templates/alluxio-configMap.yaml -f ./config.yaml > "$dir/alluxio-configMap.yaml.template"
}

function generateVolumeTemplates {
  echo "Generating persistent volume templates into $dir"
  helm template helm/alluxio/ -x templates/alluxio-journal-volume.yaml > "$dir/alluxio-journal-volume.yaml.template"
}

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  printUsage
  exit 1
fi

mode=$1

if [ "$mode" == "single-ufs" ]; then
  echo "Generating templates for $mode"
  if ! [ $# -eq 2 ]; then
    printUsage
    exit 1
  fi
  ufs=$2
  if [ "$ufs" == "local" ]; then
    echo "Using UFS $ufs"
    dir="single-local"
    generateVolumeTemplates
    generatePodTemplates
  elif [ "$ufs" == "hdfs" ]; then
    echo "Using UFS $ufs"
    dir="single-hdfs"
    generatePodTemplatesWithConfig
  else
    echo "Unknown UFS type"
    printUsage
    exit 1
  fi
elif [ "$mode" == "multi-embedded" ]; then
  echo "Generating templates for $mode"
  dir="multi-embedded"
  generatePodTemplates
else
  echo "Unknown mode $mode"
  printUsage
  exit 1
fi