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
VERSION=1.8.2-SNAPSHOT
ALLUXIO_HOME=$(dirname $(dirname "${this}"))
ALLUXIO_ASSEMBLY_CLIENT_JAR="${ALLUXIO_HOME}/assembly/client/target/alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar"
ALLUXIO_ASSEMBLY_SERVER_JAR="${ALLUXIO_HOME}/assembly/server/target/alluxio-assembly-server-${VERSION}-jar-with-dependencies.jar"
ALLUXIO_CONF_DIR="${ALLUXIO_CONF_DIR:-${ALLUXIO_HOME}/conf}"
ALLUXIO_LOGS_DIR="${ALLUXIO_LOGS_DIR:-${ALLUXIO_HOME}/logs}"

if [[ -z "$(which java)" ]]; then
  echo "Cannot find the 'java' command."
  exit 1
fi

JAVA_HOME=${JAVA_HOME:-"$(dirname $(which java))/.."}
JAVA=${JAVA:-"${JAVA_HOME}/bin/java"}

if [[ -e "${ALLUXIO_CONF_DIR}/alluxio-env.sh" ]]; then
  . "${ALLUXIO_CONF_DIR}/alluxio-env.sh"
fi

if [[ -n "${ALLUXIO_MASTER_ADDRESS}" ]]; then
  echo "ALLUXIO_MASTER_ADDRESS is deprecated since version 1.1 and will be remove in version 2.0."
  echo "Please use \"ALLUXIO_MASTER_HOSTNAME\" instead."
  ALLUXIO_MASTER_HOSTNAME=${ALLUXIO_MASTER_ADDRESS}
fi

if [[ -n "${ALLUXIO_HOME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.home=${ALLUXIO_HOME}"
fi

ALLUXIO_JAVA_OPTS+=" -Dalluxio.conf.dir=${ALLUXIO_CONF_DIR} -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR}"

if [[ -n "${ALLUXIO_RAM_FOLDER}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.alias=MEM"
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.dirs.path=${ALLUXIO_RAM_FOLDER}"
fi

if [[ -n "${ALLUXIO_MASTER_HOSTNAME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.master.hostname=${ALLUXIO_MASTER_HOSTNAME}"
fi

if [[ -n "${ALLUXIO_UNDERFS_ADDRESS}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.underfs.address=${ALLUXIO_UNDERFS_ADDRESS}"
fi

if [[ -n "${ALLUXIO_WORKER_MEMORY_SIZE}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.memory.size=${ALLUXIO_WORKER_MEMORY_SIZE}"
fi

ALLUXIO_JAVA_OPTS+=" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties"
ALLUXIO_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
ALLUXIO_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"

ALLUXIO_LOGSERVER_LOGS_DIR="${ALLUXIO_LOGSERVER_LOGS_DIR:-${ALLUXIO_HOME}/logs}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" ]]; then
    ALLUXIO_JAVA_OPTS+=" -Dalluxio.logserver.hostname=${ALLUXIO_LOGSERVER_HOSTNAME}"
fi
if [[ -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_JAVA_OPTS+=" -Dalluxio.logserver.port=${ALLUXIO_LOGSERVER_PORT}"
fi

ALLUXIO_CLIENT_CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_CLASSPATH}:${ALLUXIO_ASSEMBLY_CLIENT_JAR}"
ALLUXIO_SERVER_CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_CLASSPATH}:${ALLUXIO_ASSEMBLY_SERVER_JAR}"

# Master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_MASTER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.logger.type=${ALLUXIO_MASTER_LOGGER:-MASTER_LOGGER}"
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.audit.logger.type=${ALLUXIO_MASTER_AUDIT_LOGGER:-MASTER_AUDIT_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.remote.logger.type=REMOTE_MASTER_LOGGER"
fi

# Secondary master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=" -Dalluxio.logger.type=${ALLUXIO_SECONDARY_MASTER_LOGGER:-SECONDARY_MASTER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=" -Dalluxio.remote.logger.type=REMOTE_SECONDARY_MASTER_LOGGER"
fi

# Proxy specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_PROXY_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_PROXY_JAVA_OPTS+=" -Dalluxio.logger.type=${ALLUXIO_PROXY_LOGGER:-PROXY_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_PROXY_JAVA_OPTS+=" -Dalluxio.remote.logger.type=REMOTE_PROXY_LOGGER"
fi

# Worker specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_WORKER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_WORKER_JAVA_OPTS+=" -Dalluxio.logger.type=${ALLUXIO_WORKER_LOGGER:-WORKER_LOGGER}"
if [[ -n "${ALLUXIO_LOGSERVER_HOSTNAME}" && -n "${ALLUXIO_LOGSERVER_PORT}" ]]; then
    ALLUXIO_WORKER_JAVA_OPTS+=" -Dalluxio.remote.logger.type=REMOTE_WORKER_LOGGER"
fi

# Log server specific parameters that will be passed to alluxio log server
ALLUXIO_LOGSERVER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_LOGSERVER_JAVA_OPTS+=" -Dalluxio.logserver.logger.type=LOGSERVER_LOGGER"

# Client specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_USER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_USER_JAVA_OPTS+=" -Dalluxio.logger.type=USER_LOGGER"
