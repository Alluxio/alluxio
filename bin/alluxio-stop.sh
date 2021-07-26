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

. $(dirname "$0")/alluxio-common.sh

USAGE="Usage: alluxio-stop.sh [-h] [component] [-c cache]
Where component is one of:
  all [-c cache]      \tStop all masters, proxies, and workers.
  job_master          \tStop local job master.
  job_masters         \tStop job masters on master nodes.
  job_worker          \tStop local job worker.
  job_workers         \tStop job workers on worker nodes.
  local [-c cache]    \tStop all processes locally.
  master              \tStop local primary master.
  secondary_master    \tStop local secondary master.
  masters             \tStop masters on master nodes.
  proxy               \tStop local proxy.
  proxies             \tStop proxies on master and worker nodes.
  worker  [-c cache]  \tStop local worker.
  workers [-c cache]  \tStop workers on worker nodes.
  logserver           \tStop the logserver

-c cache   save the worker ramcache(s) from the worker node(s) to the
           specified directory (relative to each worker node's host filesystem).
-h         display this help."

DEFAULT_LIBEXEC_DIR="${BIN}/../libexec"
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

stop_job_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioJobMaster"
}

stop_job_masters() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh" "job_master"
}

stop_job_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioJobWorker"
}

stop_job_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "job_worker"
}

stop_master() {
  if [[ ${ALLUXIO_MASTER_SECONDARY} == "true" ]]; then
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioSecondaryMaster"
  else
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioMaster"
  fi
}

stop_masters() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh" "master"
}

stop_proxy() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh" "proxy"
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "proxy"
}

stop_worker() {
  if [[ ! -z "${cache}" ]]; then
    echo "Cache directory: ${cache}"
    if [[ -d "${cache}" ]]; then
      echo "Directory already exists at ${cache}; overwritting its contents"
      # recursively delete all (including hidden) files of the ramdisk
      # - avoiding deleting the directory itself to avoid permission
      #   changes/requirements on the parent directory
      shopt -s dotglob
      rm -rf "${cache}/"*
      shopt -u dotglob
    elif [[ -e "${cache}" ]]; then
      echo "Non-directory file already exists at the path ${cache}; aborting"
      exit 1
    else
      mkdir -p "${cache}"
      if [[ ${?} -ne 0 ]]; then
        echo "Failed to create directory: ${cache}"
        exit 2
      fi
    fi

    get_ramdisk_array # see alluxio-common.sh
    for dir in "${RAMDISKARRAY[@]}"; do
      if [[ ! -e "${dir}" ]]; then
        echo "Alluxio has a configured ramcache path ${dir}, but that path does not exist"
        exit 2
      fi
      if [[ ! -d "${dir}" ]]; then
        echo "Alluxio has a configured ramcache path ${dir}, but that path is not a directory"
        exit 2
      fi

      echo "Saving worker ramcache at ${dir} to ${cache}/${dir}"
      mkdir -p "${cache}/${dir}"
      if [[ ${?} -ne 0 ]]; then
        echo "Failed to create directory: ${cache}/${dir}"
        exit 2
      fi

      # recursively copy all contents of the src directory
      # (including hidden files) to the destination directory
      cp -R "${dir}/." "${cache}/${dir}/"
      if [[ ${?} -ne 0 ]]; then
        echo "Failed to copy worker ramcache from ${dir} to ${cache}/${dir}"
        exit 1
      fi
    done
  fi

  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

stop_workers() {
  start_opts=""
  if [[ -n ${cache} ]]; then
    start_opts="-c ${cache}"
  fi
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "worker" ${start_opts}
}

stop_logserver() {
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.logserver.AlluxioLogServer"
}


WHAT=${1:--h}

# shift argument index for getopts
shift
while getopts "c:" o; do
  case "${o}" in
    c)
      cache="${OPTARG}"
      ;;
    *)
      echo -e "${USAGE}" >&2
      exit 1
      ;;
  esac
done

case "${WHAT}" in
  all)
    stop_proxies
    stop_job_workers
    stop_workers
    stop_job_masters
    stop_masters
    ;;
  local)
    stop_proxy
    stop_job_worker
    stop_job_master
    stop_worker
    ALLUXIO_MASTER_SECONDARY=true
    stop_master
    ALLUXIO_MASTER_SECONDARY=false
    stop_master
    ;;
  job_master)
    stop_job_master
    ;;
  job_masters)
    stop_job_masters
    ;;
  job_worker)
    stop_job_worker
    ;;
  job_workers)
    stop_job_workers
    ;;
  master)
    stop_master
    ;;
  secondary_master)
    ALLUXIO_MASTER_SECONDARY=true
    stop_master
    ALLUXIO_MASTER_SECONDARY=false
    ;;
  masters)
    stop_masters
    ;;
  proxy)
    stop_proxy
    ;;
  proxies)
    stop_proxies
    ;;
  worker)
    stop_worker
    ;;
  workers)
    stop_workers
    ;;
  logserver)
    stop_logserver
    ;;
  -h)
    echo -e "${USAGE}"
    exit 0
    ;;
  *)
    echo "Error: Invalid component: ${WHAT}" >&2
    echo -e "${USAGE}" >&2
    exit 1
    ;;
esac
