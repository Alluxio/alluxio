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

readonly RELEASE_NAME='alluxio'

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

function generateTemplates {
  echo "Generating templates into $dir"
  # Prepare target directories
  if [[ ! -d "${dir}/master" ]]; then
    mkdir -p ${dir}/master
  fi
  if [[ ! -d "${dir}/worker" ]]; then
    mkdir -p ${dir}/worker
  fi

  config=./$dir/config.yaml
  if [[ ! -f "$config" ]]; then
    echo "A config file $config is needed in $dir!"
    echo "See https://docs.alluxio.io/os/user/edge/en/deploy/Running-Alluxio-On-Kubernetes.html#example-hdfs-as-the-under-store"
    echo "for the format of config.yaml."

    touch $config
    echo "Using default config"
    echo "${defaultConfig}"
    cat << EOF >> $config
${defaultConfig}
EOF
  fi

  generateConfigTemplates
  generateMasterTemplates
  generateWorkerTemplates
  generateFuseTemplates
}

function generateConfigTemplates {
  echo "Generating configmap templates into $dir"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/config/alluxio-conf.yaml -f $dir/config.yaml > "$dir/alluxio-configmap.yaml.template"
}

function generateMasterTemplates {
  echo "Generating master templates into $dir"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/master/statefulset.yaml -f $dir/config.yaml > "$dir/master/alluxio-master-statefulset.yaml.template"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/master/service.yaml -f $dir/config.yaml > "$dir/master/alluxio-master-service.yaml.template"
}

function generateWorkerTemplates {
  echo "Generating worker templates into $dir"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/worker/daemonset.yaml -f $dir/config.yaml > "$dir/worker/alluxio-worker-daemonset.yaml.template"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/worker/domain-socket-pvc.yaml -f $dir/config.yaml > "$dir/worker/alluxio-worker-pvc.yaml.template"
}

function generateFuseTemplates {
  echo "Generating fuse templates"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --set fuse.enabled=true --show-only templates/fuse/daemonset.yaml -f $dir/config.yaml > "alluxio-fuse.yaml.template"
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --set fuse.clientEnabled=true --show-only templates/fuse/client-daemonset.yaml -f $dir/config.yaml > "alluxio-fuse-client.yaml.template"
}

function generateMasterServiceTemplates {
  helm template --name-template ${RELEASE_NAME} helm-chart/alluxio/ --show-only templates/master/service.yaml -f $dir/config.yaml > "$dir/alluxio-master-service.yaml.template"
}

function generateSingleUfsTemplates {
  echo "Target FS $1"
  targetFs=$1
  case $targetFs in
    "local")
      echo "Using local journal"
      dir="singleMaster-localJournal"
      read -r -d '' defaultConfig << 'EOM'
master:
  count: 1 # For multiMaster mode increase this to >1

journal:
  type: "UFS"
  ufsType: "local"
  folder: "/journal"

EOM
      generateTemplates
      ;;
    "hdfs")
      echo "Journal UFS $ufs"
      dir="singleMaster-hdfsJournal"

      read -r -d '' defaultConfig << 'EOM'
master:
  count: 1

journal:
  type: "UFS"
  ufsType: "HDFS"
  folder: "hdfs://{$hostname}:{$hostport}/journal"

properties:
  alluxio.master.mount.table.root.ufs: "hdfs://{$hostname}:{$hostport}/{$underFSStorage}"
  alluxio.master.journal.ufs.option.alluxio.underfs.hdfs.configuration: "/secrets/hdfsConfig/core-site.xml:/secrets/hdfsConfig/hdfs-site.xml"

secrets:
  master:
    alluxio-hdfs-config: hdfsConfig
  worker:
    alluxio-hdfs-config: hdfsConfig

EOM

      generateTemplates
      ;;
    *)
      echo "Unknown Journal UFS type $ufs"
      printUsage
      exit 1
  esac
}

function generateMultiEmbeddedTemplates {
  dir="multiMaster-embeddedJournal"

  read -r -d '' defaultConfig << 'EOM'
master:
  count: 3

journal:
  type: "EMBEDDED"
  ufsType: "local" # This field will not be looked at
  folder: "/journal"

EOM

  generateTemplates
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
