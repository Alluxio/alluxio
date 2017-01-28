#!/usr/bin/env bash

set -e

if [[ $# -ne 1 ]]; then
  echo 'expected one argument; either "master" or "worker"'
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
  worker)
    bin/alluxio formatWorker
    integration/docker/bin/alluxio-worker.sh
    ;;
  master)
    bin/alluxio format
    integration/docker/bin/alluxio-master.sh
    ;;
  *)
    echo 'expected either "master" or "worker"';
    exit 1
    ;;
esac
