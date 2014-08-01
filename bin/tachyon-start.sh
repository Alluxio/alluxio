#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#start up tachyon

Usage="Usage: tachyon-start.sh [-h] WHAT [MOPT]
Where WHAT is one of:
  all MOPT\t\tStart master and all slaves.
  local\t\t\tStart a master and slave locally
  master\t\tStart the master on this node
  safe\t\t\tScript will run continuously and start the master if it's not running
  worker MOPT\t\tStart a worker on this node
  workers MOPT\t\tStart workers on slaves
  restart_worker\tRestart a failed worker on this node
  restart_workers\tRestart any failed workers on slaves

MOPT is one of:
  Mount\t\t\tMount the configured RamFS
  SudoMount\t\tMount the configured RamFS using sudo
  NoMount\t\tDo not mount the configured RamFS

-h  display this help."

bin=`cd "$( dirname "$0" )"; pwd`

ensure_dirs() {
  if [ ! -d "$TACHYON_LOGS_DIR" ]; then
    echo "TACHYON_LOGS_DIR: $TACHYON_LOGS_DIR"
    mkdir -p $TACHYON_LOGS_DIR
  fi
}

get_env() {
  DEFAULT_LIBEXEC_DIR="$bin"/../libexec
  TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $TACHYON_LIBEXEC_DIR/tachyon-config.sh
}

check_mount_mode() {
  case "${1}" in
    Mount);;
    SudoMount);;
    NoMount);;
    *)
      if [ -z $1 ] ; then
        echo "This command requires a mount mode be specified"
      else
        echo "Invalid mount mode: $1"
      fi
      echo -e "$Usage"
      exit 1
  esac
}

# pass mode as $1
do_mount() {
  MOUNT_FAILED=0
  case "${1}" in
    Mount)
      $bin/tachyon-mount.sh $1
      MOUNT_FAILED=$?
      ;;
    SudoMount)
      $bin/tachyon-mount.sh $1
      MOUNT_FAILED=$?
      ;;
    NoMount)
      ;;
    *)
      echo "This command requires a mount mode be specified"
      echo -e "$Usage"
      exit 1
  esac
}

stop() {
  $bin/tachyon-stop.sh
}


start_master() {
  MASTER_ADDRESS=$TACHYON_MASTER_ADDRESS
  if [ -z $TACHYON_MASTER_ADDRESS ] ; then
    MASTER_ADDRESS=localhost
  fi

  echo "Starting master @ $MASTER_ADDRESS"
  (nohup $JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="MASTER_LOGGER" -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties $TACHYON_MASTER_JAVA_OPTS tachyon.master.TachyonMaster > /dev/null 2>&1) &
}

start_worker() {
  do_mount $1
  if  [ $MOUNT_FAILED -ne 0 ] ; then
    echo "Mount failed, not starting worker"
    exit 1
  fi

  echo "Starting worker @ `hostname -f`"
  (nohup $JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="WORKER_LOGGER" -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties $TACHYON_WORKER_JAVA_OPTS tachyon.worker.TachyonWorker `hostname -f` > /dev/null 2>&1 ) &
}

restart_worker() {
  RUN=`ps -ef | grep "tachyon.worker.TachyonWorker" | grep "java" | wc | cut -d" " -f7`
  if [[ $RUN -eq 0 ]] ; then
    echo "Restarting worker @ `hostname -f`"
    (nohup $JAVA -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="WORKER_LOGGER" -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties $TACHYON_WORKER_JAVA_OPTS tachyon.worker.TachyonWorker `hostname -f` > /dev/null 2>&1) &
  fi
}

run_safe() {
  while [ 1 ]
  do
    RUN=`ps -ef | grep "tachyon.master.TachyonMaster" | grep "java" | wc | cut -d" " -f7`
    if [[ $RUN -eq 0 ]] ; then
      echo "Restarting the system master..."
      start_master
    fi
    echo "Tachyon is running... "
    sleep 2
  done
}

while getopts "h" o; do
  case "${o}" in
    h)
      echo -e "$Usage"
      exit 0
      ;;
    *)
      echo -e "$Usage"
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

WHAT=$1

if [ -z "${WHAT}" ]; then
  echo "Error: no WHAT specified"
  echo -e "$Usage"
  exit 1
fi

# get environment
get_env

# ensure log/data dirs
ensure_dirs

case "${WHAT}" in
  all)
    check_mount_mode $2
    stop $bin
    start_master
    sleep 2
    $bin/tachyon-slaves.sh $bin/tachyon-start.sh worker $2
    ;;
  local)
    stop $bin
    sleep 1
    $bin/tachyon-mount.sh SudoMount
    stat=$?
    if [ $stat -ne 0 ] ; then
      echo "Mount failed, not starting"
      exit 1
    fi
    start_master
    sleep 2
    start_worker NoMount
    ;;
  master)
    start_master
    ;;
  worker)
    check_mount_mode $2
    start_worker $2
    ;;
  safe)
    run_safe
    ;;
  workers)
    check_mount_mode $2
    $bin/tachyon-slaves.sh $bin/tachyon-start.sh worker $2 $TACHYON_MASTER_ADDRESS
    ;;
  restart_worker)
    restart_worker
    ;;
  restart_workers)
    $bin/tachyon-slaves.sh $bin/tachyon-start.sh restart_worker
    ;;
  *)
    echo "Error: Invalid WHAT: $WHAT"
    echo -e "$Usage"
    exit 1
esac
sleep 2
