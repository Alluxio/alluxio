#!/bin/bash

set -xe

ROOT=$(cd $(dirname $0)/../../; pwd)

set -o errexit
set -o nounset
set -o pipefail

#export CA_BUNDLE=$(kubectl config view --raw --minify --flatten -o jsonpath='{.clusters[].cluster.certificate-authority-data}')

#if [ -z "${CA_BUNDLE}" ]; then
export CA_BUNDLE=$(kubectl get secrets -o jsonpath="{.items[?(@.metadata.annotations['kubernetes\.io/service-account\.name']=='default')].data.ca\.crt}")
# kubectl get secrets -n pillars-system -o jsonpath="{.items[?(@.metadata.annotations['kubernetes\.io/service-account\.name']=='default')].data.ca\.crt}"
#fi

#if command -v envsubst >/dev/null 2>&1; then
#    envsubst
#else
    # sed -e "s|\${CA_BUNDLE}|${CA_BUNDLE}|g"
sed -e "s|fake|${CA_BUNDLE}|g"
#fi
