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

SCRIPT_DIR="$(cd "$(dirname "$(readlink "$0" || echo "$0")")"; pwd)"

get_env() {
  DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}"/../../../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

  ALLUXIO_FUSE_JAR=${SCRIPT_DIR}/../target/alluxio-integration-fuse-${VERSION}-jar-with-dependencies.jar
  FUSE_MAX_WRITE=131072
  CLASSPATH=${CLASSPATH}:${ALLUXIO_FUSE_JAR}
}

check_java_version() {
  local java_mjr_vers=$("${JAVA}" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F'.' '{print $1 $2}')
  if [[ ${java_mjr_vers} -lt 18 ]]; then
    echo "You are running a version of Java which is older than Java 8.
     Please use Java 8 to use alluxio-fuse" >&2
    return 1
  else
    return 0
  fi
}

check_fuse_jar() {
  if ! [[ -f ${ALLUXIO_FUSE_JAR} ]]; then
    echo "Cannot find ${ALLUXIO_FUSE_JAR}. Please compile alluxio with fuse profile and Java 8"
    return 1
  else
    return 0
  fi
}

set_java_opt() {
  JAVA_OPTS+="
    -server
    -Xms1G
    -Xmx1G
  "

  ALLUXIO_FUSE_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  ALLUXIO_FUSE_JAVA_OPTS+=" -Dalluxio.logger.type=FUSE_LOGGER"
}

mount_fuse() {
  echo "Starting alluxio-fuse on local host."
  local mount_point=$1
  local alluxio_root=$2
  (nohup "${JAVA}" -cp ${CLASSPATH} ${JAVA_OPTS} ${ALLUXIO_FUSE_JAVA_OPTS} \
    alluxio.fuse.AlluxioFuse \
    -o big_writes \
    -m ${mount_point} \
    -r ${alluxio_root} > ${ALLUXIO_LOGS_DIR}/fuse.out 2>&1) &
  # sleep: workaround to let the bg java process exit on errors, if any
  sleep 2s
  if kill -0 $! > /dev/null 2>&1 ; then
    echo "Alluxio-fuse mounted at ${mount_point}. See ${ALLUXIO_LOGS_DIR}/fuse.log for logs"
    return 0
  else
    echo "alluxio-fuse not started. See ${ALLUXIO_LOGS_DIR}/fuse.out for details" >&2
    return 1
  fi
}

umount_fuse() {
  local mount_point=$1  
  local fuse_pid=$(fuse_stat | awk '{print $1,$2}' | grep -w ${mount_point} | awk '{print $1}')  
  if [[ -z ${fuse_pid} ]]; then
    echo "No fuse mounted at ${mount_point}" >&2
    return 1
  else
    echo "Unmount fuse at ${mount_point} (PID: ${fuse_pid})."
    kill ${fuse_pid}
    return $?
  fi
}

fuse_stat() {
  local fuse_info=$("${JAVA_HOME}/bin/jps" | grep AlluxioFuse)
  if [[ -z ${fuse_info} ]]; then    
    echo "AlluxioFuse: not running"
    return 1
  else
    echo -e "pid\tmount_point\talluxio_path"
    echo -e "$(ps aux | grep [A]lluxioFuse | awk -F' ' '{print $2 "\t" $(NF-2) "\t" $NF}')"
    return 0    
  fi
}

USAGE_MSG="Usage:\n\t$0 [mount|umount|stat]"

if [[ $# -lt 1 ]]; then
  echo -e "${USAGE_MSG}" >&2
  exit 1
fi

get_env
check_java_version && check_fuse_jar
set_java_opt
if [[ $? -ne 0 ]]; then
  exit 1
fi

case $1 in
  mount)
    if [[ $# -eq 2 ]]; then
      mount_fuse $2 /
      exit $?
    fi
    if [[ $# -eq 3 ]]; then
      mount_fuse $2 $3
      exit $?
    fi
    echo -e "Usage\n\t$0 mount mount_point [alluxio_path]\n\t alluxio_path is default to root" >&2
    exit 1
    ;;
  umount)    
    if [[ $# -eq 2 ]]; then
      umount_fuse $2
      exit $?
    fi
    echo -e "Usage\n\t$0 umount mount_point\n\tuse mount stat to show mount points" >&2
    exit 1
    ;;
  stat)
    fuse_stat
    ;;
  *)
    echo "${USAGE_MSG}" >&2
    exit 1
    ;;
esac
