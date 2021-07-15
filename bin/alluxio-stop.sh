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

USAGE="Usage: alluxio-stop.sh [-h] [component]
Where component is one of:
  all     [-c cache]  \tStop all masters, proxies, and workers.
    -c cache   save the worker MEM-type cache(s) from the worker node(s) to the
               specified directory (relative to each worker node's host filesystem).
  job_master          \tStop local job master.
  job_masters         \tStop job masters on master nodes.
  job_worker          \tStop local job worker.
  job_workers         \tStop job workers on worker nodes.
  local   [-c cache]  \tStop all processes locally.
    -c cache   save the worker MEM-type cache(s) from the worker node(s) to the
               specified directory (relative to each worker node's host filesystem).
  master              \tStop local primary master.
  secondary_master    \tStop local secondary master.
  masters             \tStop masters on master nodes.
  proxy               \tStop local proxy.
  proxies             \tStop proxies on master and worker nodes.
  worker  [-c cache]  \tStop local worker.
    -c cache   save the worker MEM-type cache(s) from the worker node(s) to the
               specified directory (relative to each worker node's host filesystem).
  workers [-c cache]  \tStop workers on worker nodes.
    -c cache   save the worker MEM-type cache(s) from the worker node(s) to the
               specified directory (relative to each worker node's host filesystem).
  logserver           \tStop the logserver

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

stop_worker() { # [-c cache]
  local cache="${1:-}"
  if [[ ! -z ${cache} ]]; then
    echo "Cache directory: ${cache}"
    mkdir -p ${cache}
    if [[ ${?} -ne 0 ]]; then
      echo "Failed to create directory: ${cache}"
      exit 2
    fi

    num_tiers=$(get_alluxio_property "alluxio.worker.tieredstore.levels")
    for ((i=0;i<num_tiers;i++)); do
      echo "Checking Worker tiered store level ${i}..."

      tier_types=$(get_alluxio_property "alluxio.worker.tieredstore.level${i}.dirs.mediumtype")
      tier_dirs=$(get_alluxio_property "alluxio.worker.tieredstore.level${i}.dirs.path")
      # Use "Internal Field Separator (IFS)" variable to split strings on a
      # delimeter and parse into an array
      # - https://stackoverflow.com/a/918931
      IFS=',' read -ra tier_types_arr <<< "${tier_types}"
      IFS=',' read -ra tier_dirs_arr <<< "${tier_dirs}"

      # iterate over the array elements using indices
      # - https://stackoverflow.com/a/6723516
      for j in "${!tier_types_arr[@]}"; do
        tier_type=${tier_types_arr[$j]}
        if [[ "${tier_type}" -eq "MEM" ]]; then
          tier_dir=${tier_dirs_arr[$j]}

          echo "Saving Worker tiered store at ${tier_dir} to ${cache}/tier${i}/${tier_dir}"
          mkdir -p "${cache}/tier${i}/${tier_dir}"
          cp -a "${tier_dir}/." "${cache}/tier${i}/${tier_dir}/"
          if [[ ${?} -ne 0 ]]; then
            echo "Failed to copy directory from ${tier_dir} to ${cache}/tier${i}/${tier_dir}"
            exit 2
          fi
        fi
      done
    done
  fi

  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

stop_workers() { # [-c cache]
  local cache="${1:-}"
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "worker" ${cache}
}

stop_logserver() {
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.logserver.AlluxioLogServer"
}


WHAT=${1:--h}

# TODO(czhu): Implement usage of bash flags (eg: `getopts`)
CACHE=${2:-}

case "${WHAT}" in
  all)
    stop_proxies
    stop_job_workers
    stop_workers ${CACHE}
    stop_job_masters
    stop_masters
    ;;
  local)
    stop_proxy
    stop_job_worker
    stop_job_master
    stop_worker ${CACHE}
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
    stop_worker ${CACHE}
    ;;
  workers)
    stop_workers ${CACHE}
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
