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

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

USAGE="Usage: alluxio-stop.sh [-h] [component]
Where component is one of:
  all     \tStop master and all proxies and workers.
  local   \tStop local master, proxy, and worker.
  master  \tStop local master.
  proxy   \tStop local proxy.
  proxies \tStop all remote proxies.
  worker  \tStop local worker.
  workers \tStop all remote workers.

-h  display this help."

stop_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioMaster"
}

stop_proxy() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

stop_remote_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_remote_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

WHAT=${1:--h}

case "${WHAT}" in
  all)
    stop_remote_proxies
    stop_remote_workers
    stop_proxy
    stop_master
    ;;
  local)
    stop_proxy
    stop_worker
    stop_master
    ;;
  master)
    stop_master
    ;;
  proxy)
    stop_proxy
    ;;
  proxies)
    stop_remote_proxies
    ;;
  worker)
    stop_worker
    ;;
  workers)
    stop_remote_workers
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
