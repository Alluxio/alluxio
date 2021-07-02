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

# Included in all the Alluxio scripts with source command should not be executable directly also
# should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
script="$(basename -- "${this}")"
this="${common_bin}/${script}"

# convert relative path to absolute path
config_bin=$(dirname "${this}")
script=$(basename "${this}")
config_bin=$(cd "${config_bin}"; pwd)
this="${config_bin}/${script}"

# This will set the default installation for a tarball installation while os distributors can
# set system installation locations.
VERSION=2.7.0-SNAPSHOT
ALLUXIO_HOME=$(dirname $(dirname "${this}"))
ALLUXIO_ASSEMBLY_CLIENT_JAR="${ALLUXIO_HOME}/assembly/client/target/alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar"
ALLUXIO_ASSEMBLY_SERVER_JAR="${ALLUXIO_HOME}/assembly/server/target/alluxio-assembly-server-${VERSION}-jar-with-dependencies.jar"
ALLUXIO_CONF_DIR="${ALLUXIO_CONF_DIR:-${ALLUXIO_HOME}/conf}"
ALLUXIO_LOGS_DIR="${ALLUXIO_LOGS_DIR:-${ALLUXIO_HOME}/logs}"
ALLUXIO_USER_LOGS_DIR="${ALLUXIO_USER_LOGS_DIR:-${ALLUXIO_LOGS_DIR}/user}"

if [[ -e "${ALLUXIO_CONF_DIR}/alluxio-env.sh" ]]; then
  . "${ALLUXIO_CONF_DIR}/alluxio-env.sh"
fi

# Check if java is found
if [[ -z "${JAVA}" ]]; then
  if [[ -n "${JAVA_HOME}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]];  then
    JAVA="${JAVA_HOME}/bin/java"
  elif [[ -n "$(which java 2>/dev/null)" ]]; then
    JAVA=$(which java)
  else
    echo "Error: Cannot find 'java' on path or under \$JAVA_HOME/bin/. Please set JAVA_HOME in alluxio-env.sh or user bash profile."
    exit 1
  fi
fi

# Check Java version == 1.8 or == 11
JAVA_VERSION=$(${JAVA} -version 2>&1 | awk -F '"' '/version/ {print $2}')
JAVA_MAJORMINOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d%03d",$1,$2);}')
JAVA_MAJOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d",$1);}')
if [[ ${JAVA_MAJORMINOR} != 001008 && ${JAVA_MAJOR} != 011 ]]; then
  echo "Error: Alluxio requires Java 8 or Java 11, currently Java $JAVA_VERSION found."
  exit 1
fi

ALLUXIO_CLIENT_CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_CLASSPATH}:${ALLUXIO_ASSEMBLY_CLIENT_JAR}:${ALLUXIO_HOME}/lib/alluxio-integration-tools-validation-${VERSION}.jar"
ALLUXIO_SERVER_CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_CLASSPATH}:${ALLUXIO_ASSEMBLY_SERVER_JAR}"

if [[ -n "${ALLUXIO_HOME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.home=${ALLUXIO_HOME}"
fi

ALLUXIO_JAVA_OPTS+=" -Dalluxio.conf.dir=${ALLUXIO_CONF_DIR} -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.user.logs.dir=${ALLUXIO_USER_LOGS_DIR}"

if [[ -n "${ALLUXIO_RAM_FOLDER}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.alias=MEM"
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.dirs.path=${ALLUXIO_RAM_FOLDER}"
fi

if [[ -n "${ALLUXIO_MASTER_HOSTNAME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.master.hostname=${ALLUXIO_MASTER_HOSTNAME}"
fi

if [[ -n "${ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.master.mount.table.root.ufs=${ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS}"
elif [[ -n "${ALLUXIO_UNDERFS_ADDRESS}" ]]; then
  echo "Warning: ALLUXIO_UNDERFS_ADDRESS is deprecated by ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS"
  ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="${ALLUXIO_UNDERFS_ADDRESS}"
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.master.mount.table.root.ufs=${ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS}"
fi

# ALLUXIO_WORKER_MEMORY_SIZE is deprecated but kept for compatibility
if [[ -n "${ALLUXIO_WORKER_MEMORY_SIZE}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.ramdisk.size=${ALLUXIO_WORKER_MEMORY_SIZE}"
fi

if [[ -n "${ALLUXIO_WORKER_RAMDISK_SIZE}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.ramdisk.size=${ALLUXIO_WORKER_RAMDISK_SIZE}"
fi

ALLUXIO_JAVA_OPTS+=" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties"
ALLUXIO_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
ALLUXIO_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"
ALLUXIO_JAVA_OPTS+=" -Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false"

ALLUXIO_LOGSERVER_LOGS_DIR="${ALLUXIO_LOGSERVER_LOGS_DIR:-${ALLUXIO_HOME}/logs}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" ]]; then
    ALLUXIO_JAVA_OPTS+=" -Dalluxio.logserver.hostname=${ALLUXIO_LOGSERVER_HOSTNAME}"
fi
if [[ -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_JAVA_OPTS+=" -Dalluxio.logserver.port=${ALLUXIO_LOGSERVER_PORT}"
fi

# Job master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_JOB_MASTER_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_JOB_MASTER_LOGGER:-JOB_MASTER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_JOB_MASTER_JAVA_OPTS_DEFAULT+=" -Dalluxio.remote.logger.type=REMOTE_JOB_MASTER_LOGGER"
fi
ALLUXIO_JOB_MASTER_JAVA_OPTS="${ALLUXIO_JOB_MASTER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_JOB_MASTER_JAVA_OPTS}"

# Job worker specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_JOB_WORKER_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_JOB_WORKER_LOGGER:-JOB_WORKER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_JOB_WORKER_JAVA_OPTS_DEFAULT+=" -Dalluxio.remote.logger.type=REMOTE_JOB_WORKER_LOGGER"
fi
ALLUXIO_JOB_WORKER_JAVA_OPTS="${ALLUXIO_JOB_WORKER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_JOB_WORKER_JAVA_OPTS}"

# Master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_MASTER_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_MASTER_LOGGER:-MASTER_LOGGER}"
ALLUXIO_MASTER_JAVA_OPTS_DEFAULT+=" -Dalluxio.master.audit.logger.type=${ALLUXIO_MASTER_AUDIT_LOGGER:-MASTER_AUDIT_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.remote.logger.type=REMOTE_MASTER_LOGGER"
fi
ALLUXIO_MASTER_JAVA_OPTS="${ALLUXIO_MASTER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_MASTER_JAVA_OPTS}"

# Secondary master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_SECONDARY_MASTER_LOGGER:-SECONDARY_MASTER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_SECONDARY_MASTER_JAVA_OPTS_DEFAULT+=" -Dalluxio.remote.logger.type=REMOTE_SECONDARY_MASTER_LOGGER"
fi
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS="${ALLUXIO_SECONDARY_MASTER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_SECONDARY_MASTER_JAVA_OPTS}"

# Proxy specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_PROXY_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_PROXY_LOGGER:-PROXY_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_PROXY_JAVA_OPTS_DEFAULT+=" -Dalluxio.remote.logger.type=REMOTE_PROXY_LOGGER"
fi
ALLUXIO_PROXY_JAVA_OPTS="${ALLUXIO_PROXY_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_PROXY_JAVA_OPTS}"

# Worker specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_WORKER_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=${ALLUXIO_WORKER_LOGGER:-WORKER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_WORKER_JAVA_OPTS_DEFAULT+=" -Dalluxio.remote.logger.type=REMOTE_WORKER_LOGGER"
fi
ALLUXIO_WORKER_JAVA_OPTS="${ALLUXIO_WORKER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_WORKER_JAVA_OPTS}"

# FUSE specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_FUSE_JAVA_OPTS_DEFAULT=" -Dalluxio.logger.type=FUSE_LOGGER -server -Xms1G -Xmx1G -XX:MaxDirectMemorySize=4g"
ALLUXIO_FUSE_JAVA_OPTS="${ALLUXIO_FUSE_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_FUSE_JAVA_OPTS}"

# Log server specific parameters that will be passed to alluxio log server
ALLUXIO_LOGSERVER_JAVA_OPTS_DEFAULT=" -Dalluxio.logserver.logger.type=LOGSERVER_LOGGER"
ALLUXIO_LOGSERVER_JAVA_OPTS="${ALLUXIO_LOGSERVER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_LOGSERVER_JAVA_OPTS}"

# Client specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_USER_JAVA_OPTS_DEFAULT+=" -Dalluxio.logger.type=USER_LOGGER"
ALLUXIO_USER_JAVA_OPTS="${ALLUXIO_USER_JAVA_OPTS_DEFAULT} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_USER_JAVA_OPTS}"
