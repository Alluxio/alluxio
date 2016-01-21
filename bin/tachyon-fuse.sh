#!/usr/bin/env bash

if [[ -n "$BASH_VERSION" ]]; then
    BIN="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
elif [[ -n "$ZSH_VERSION" ]]; then
    BIN="$( cd "$( dirname "${(%):-%x}" )" && pwd )"
else
    echo "Please, launch your scripts from zsh or bash only." >&2
    exit 1
fi

get_env () {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $TACHYON_LIBEXEC_DIR/tachyon-config.sh

  TACHYON_MASTER_PORT=${TACHYON_MASTER_PORT:-19998}
  TACHYON_FUSE_JAR=${BIN}/../fuse/target/tachyon-fuse-${VERSION}-jar-with-dependencies.jar
  FUSE_MAX_WRITE=131072
}

check_java_version () {
  local java_mjr_vers=$(${JAVA} -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F'.' '{print $1 $2}')
  if [[ ${java_mjr_vers} -lt 18 ]]; then
    echo "It seems you are running a version of Java which is older then Java8. Please, use Java 8 to use tachyon-fuse" >&2
    return 1
  else
    return 0
  fi
}

check_tfuse_jar () {
  if ! [[ -f ${TACHYON_FUSE_JAR} ]]; then
    echo "Cannot find ${TACHYON_FUSE_JAR}. Was tachyon compiled with java8 or more recent?"
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
  
  TACHYON_FUSE_OPTS+="
    -Dtachyon.logger.type=tachyon.fuse
    -Dtachyon.master.port=${TACHYON_MASTER_PORT}
    -Dtachyon.master.hostname=${TACHYON_MASTER_ADDRESS}
    -Dtachyon.logs.dir=$TACHYON_LOGS_DIR
    -Dtachyon.logger.type="FUSE_LOGGER"
    -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties
  "
}

mount_fuse() {
  if fuse_stat > /dev/null ; then
    echo "tachyon-fuse is already running on the local host. Please, stop it first." >&2
    return 1
  fi
  echo "Starting tachyon-fuse on local host."
  local mount_point=$1
  (nohup $JAVA -cp ${TACHYON_FUSE_JAR} ${JAVA_OPTS} ${TACHYON_FUSE_OPTS}\
    tachyon.fuse.TachyonFuse \
    -m ${mount_point} \
    -o big_writes > $TACHYON_LOGS_DIR/fuse.out 2>&1) &
  # sleep: workaround to let the bg java process exit on errors, if any
  sleep 2s
  if kill -0 $! > /dev/null 2>&1 ; then
    return 0
  else
    echo "tachyon-fuse not started. See ${TACHYON_LOGS_DIR}/fuse.out for details" >&2
    return 1
  fi
}

umount_fuse () {
  local fuse_pid=$(fuse_stat)
  if [[ $? -eq 0 ]]; then
    echo "Stopping tachyon-fuse on local host (PID: ${fuse_pid})."
    kill ${fuse_pid}
    return $?
  else 
    echo "tachyon-fuse is not running on local host." >&2
    return 1
  fi
}

fuse_stat() {
  local fuse_pid=$(${JAVA_HOME}/bin/jps | grep TachyonFuse | awk -F' ' '{print $1}')
  if [[ -z ${fuse_pid} ]]; then
    if [[ $1 == "-v" ]]; then
      echo "TachyonFuse: not running"
      return 1
    else 
      return 1
    fi
  else
    local fuse_mount=$(mount | grep tachyon-fuse | awk -F' ' '{print $3" "$6}')
    if [[ $1 == "-v" ]]; then
      echo "TachyonFuse mounted on ${fuse_mount} [PID: ${fuse_pid}]"
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
if [[ $? -ne 0 ]] ; then
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

