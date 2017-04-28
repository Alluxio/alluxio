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

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"

get_env () {
  DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}"/../../../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

  ALLUXIO_FUSE_JAR=${SCRIPT_DIR}/../target/alluxio-integration-fuse-${VERSION}-jar-with-dependencies.jar
  FUSE_MAX_WRITE=131072
  CLASSPATH=${CLASSPATH}:${ALLUXIO_FUSE_JAR}
}

check_java_version () {
  local java_mjr_vers=$("${JAVA}" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F'.' '{print $1 $2}')
  if [[ ${java_mjr_vers} -lt 18 ]]; then
    echo "It seems you are running a version of Java which is older then Java8.
     Please, use Java8 to use alluxio-fuse" >&2
    return 1
  else
    return 0
  fi
}

check_tfuse_jar () {
  if ! [[ -f ${ALLUXIO_FUSE_JAR} ]]; then
    echo "Cannot find ${ALLUXIO_FUSE_JAR}. Was alluxio compiled with java8 or more recent?"
    return 1
  else
    return 0
  fi
}

set_java_opt () {
  JAVA_OPTS+="
    -server
    -Xms1G
    -Xmx1G
  "

  ALLUXIO_FUSE_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  ALLUXIO_FUSE_JAVA_OPTS+=" -Dalluxio.logger.type=FUSE_LOGGER"
}

mount_fuse() {
  if fuse_stat > /dev/null ; then
    echo "alluxio-fuse is already running on the local host. Please, stop it first." >&2
    return 1
  fi
  echo "Starting alluxio-fuse on local host."
  local mount_point=$1
  (nohup "${JAVA}" -cp ${CLASSPATH} ${JAVA_OPTS} ${ALLUXIO_FUSE_JAVA_OPTS} \
    alluxio.fuse.AlluxioFuse \
    -m ${mount_point} \
    -o big_writes > ${ALLUXIO_LOGS_DIR}/fuse.out 2>&1) &
  # sleep: workaround to let the bg java process exit on errors, if any
  sleep 2s
  if kill -0 $! > /dev/null 2>&1 ; then
    return 0
  else
    echo "alluxio-fuse not started. See ${ALLUXIO_LOGS_DIR}/fuse.out for details" >&2
    return 1
  fi
}

umount_fuse () {
  local fuse_pid=$(fuse_stat)
  if [[ $? -eq 0 ]]; then
    echo "Stopping alluxio-fuse on local host (PID: ${fuse_pid})."
    kill ${fuse_pid}
    return $?
  else
    echo "alluxio-fuse is not running on local host." >&2
    return 1
  fi
}

fuse_stat() {
  local fuse_pid=$("${JAVA_HOME}/bin/jps" | grep AlluxioFuse | awk -F' ' '{print $1}')
  if [[ -z ${fuse_pid} ]]; then
    if [[ $1 == "-v" ]]; then
      echo "AlluxioFuse: not running"
      return 1
    else
      return 1
    fi
  else
    local fuse_mount=$(mount | grep alluxio-fuse | awk -F' ' '{print $3" "$6}')
    if [[ $1 == "-v" ]]; then
      echo "AlluxioFuse mounted on ${fuse_mount} [PID: ${fuse_pid}]"
      return 0
    else
      echo ${fuse_pid}
      return 0
    fi
  fi
}

USAGE_MSG="Usage:\n\t$0 [mount|umount|stat]"

if [[ $# -lt 1 ]]; then
  echo -e "${USAGE_MSG}" >&2
  exit 1
fi

get_env
check_java_version && check_tfuse_jar
set_java_opt
if [[ $? -ne 0 ]]; then
  exit 1
fi

case $1 in
  mount)
    if [[ $# -ne 2 ]]; then
      echo -e "Usage\n\t$0 mount [mount_point]" >&2
      exit 1
    fi
    mount_fuse $2
    exit $?
    ;;
  umount)
    umount_fuse
    exit $?
    ;;
  stat)
    fuse_stat -v
    ;;
  *)
    echo "${USAGE_MSG}" >&2
    exit 1
    ;;
esac
