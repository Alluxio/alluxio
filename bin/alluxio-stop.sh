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

USAGE="Usage: alluxio-stop.sh [-h] [component] [-c cache] [-s]
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
-s         processes won't be forcibly killed even if operation is timeout.
-h         display this help."

DEFAULT_LIBEXEC_DIR="${BIN}/../libexec"
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
declare -a KILL_COMMAND

generate_kill_command() {
  unset KILL_COMMAND
  local process_class=$1
  KILL_COMMAND+=("${BIN}/alluxio" "killAll")
  if [[ "${soft_kill}" = true ]]; then
    KILL_COMMAND+=("-s")
  fi
  KILL_COMMAND+=("${process_class}")
}

generate_cluster_kill_command() {
  unset KILL_COMMAND
  local script_type=$1
  local process_type=$2
  if [[ "${script_type}" = "master" ]]; then
    KILL_COMMAND+=("${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh")
  elif [[ "${script_type}" = "worker" ]]; then
    KILL_COMMAND+=("${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh")
  else
    echo "Error: Invalid script_type: ${script_type} "
    exit 1
  fi
  KILL_COMMAND+=("${process_type}")
  if [[ "${soft_kill}" = true ]]; then
    KILL_COMMAND+=("-s")
  fi
}

stop_job_master() {
  generate_kill_command "alluxio.master.AlluxioJobMaster"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_job_masters() {
  generate_cluster_kill_command "master" "job_master"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_job_worker() {
  generate_kill_command "alluxio.worker.AlluxioJobWorker"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_job_workers() {
  generate_cluster_kill_command "worker" "job_worker"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_master() {
  if [[ ${ALLUXIO_MASTER_SECONDARY} == "true" ]]; then
    generate_kill_command "alluxio.master.AlluxioSecondaryMaster"
    ${LAUNCHER} "${KILL_COMMAND[@]}"
  else
    generate_kill_command "alluxio.master.AlluxioMaster"
    ${LAUNCHER} "${KILL_COMMAND[@]}"
  fi
}

stop_masters() {
  generate_cluster_kill_command "master" "master"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_proxy() {
  generate_kill_command "alluxio.proxy.AlluxioProxy"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_proxies() {
  generate_cluster_kill_command "master" "proxy"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
  generate_cluster_kill_command "worker" "proxy"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
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
  generate_kill_command "alluxio.worker.AlluxioWorker"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}

stop_workers() {
  start_opts=""
  if [[ -n ${cache} ]]; then
    start_opts="-c ${cache}"
  fi
  generate_cluster_kill_command "worker" "worker"
  ${LAUNCHER} "${KILL_COMMAND[@]}" ${start_opts}
}

stop_logserver() {
  generate_kill_command "alluxio.logserver.AlluxioLogServer"
  ${LAUNCHER} "${KILL_COMMAND[@]}"
}


WHAT=${1:--h}

# shift argument index for getopts
shift
while getopts "c:s" o; do
  case "${o}" in
    c)
      cache="${OPTARG}"
      ;;
    s)
      soft_kill=true
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
