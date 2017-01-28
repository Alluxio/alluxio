#!/usr/bin/env bash

set -e

if [[ $# -ne 1 ]]; then
  echo 'expected one argument: "master", "worker", or "proxy"'
  exit 1
fi

service=$1

home=/opt/$(ls /opt | grep alluxio)
cd ${home}

for keyvaluepair in $(env | grep "ALLUXIO_"); do
  # split around the "="
  key=$(echo ${keyvaluepair} | cut -d= -f1)
  value=$(echo ${keyvaluepair} | cut -d= -f2)
  key=$(echo ${key} | sed "s/_/./g" | tr '[:upper:]' '[:lower:]')
  echo "${key}=${value}" >> conf/alluxio-site.properties
done

case ${service,,} in
  master)
    bin/alluxio format
    integration/docker/bin/alluxio-master.sh
    ;;
  worker)
    bin/alluxio formatWorker
    integration/docker/bin/alluxio-worker.sh
    ;;
  proxy)
    integration/docker/bin/alluxio-proxy.sh
    ;;
  *)
    echo 'expected "master", "worker", or "proxy"';
    exit 1
    ;;
esac
