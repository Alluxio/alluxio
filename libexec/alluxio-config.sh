#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the “License”). You may not use this work except in compliance with the License, which is
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
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=$(dirname "$this")
script=$(basename "$this")
config_bin=$(cd "$config_bin"; pwd)
this="$config_bin/$script"

# Allow for a script which overrides the default settings for system integration folks.
[[ -f "$common_bin/alluxio-layout.sh" ]] && . "$common_bin/alluxio-layout.sh"

# This will set the default installation for a tarball installation while os distributors can create
# their own alluxio-layout.sh file to set system installation locations.
if [[ -z "$ALLUXIO_SYSTEM_INSTALLATION" ]]; then
  VERSION=1.1.0-RC1
  ALLUXIO_HOME=$(dirname $(dirname "${this}"))
  ALLUXIO_CONF_DIR="${ALLUXIO_HOME}/conf"
  ALLUXIO_LOGS_DIR="${ALLUXIO_HOME}/logs"
  ALLUXIO_JARS="${ALLUXIO_HOME}/assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar"
fi

JAVA_HOME=${JAVA_HOME:-"$(dirname $(which java))/.."}
JAVA=${JAVA:-"${JAVA_HOME}/bin/java"}

# Make sure alluxio-env.sh exists
if [[ ! -e ${ALLUXIO_CONF_DIR}/alluxio-env.sh ]]; then
  echo "Cannot find ${ALLUXIO_CONF_DIR}/alluxio-env.sh. To proceed, you can"
  echo "(1) create one based on the provided template file ${ALLUXIO_CONF_DIR}/alluxio-env.sh.template, or"
  echo "(2) use a bootstraping tool by running: ${ALLUXIO_HOME}/bin/alluxio bootstrap-conf"
  exit 1
fi

. "${ALLUXIO_CONF_DIR}/alluxio-env.sh"

if [[ -n "${ALLUXIO_MASTER_ADDRESS}" ]]; then
  echo "ALLUXIO_MASTER_ADDRESS is deprecated since version 1.1 and will be remove in version 2.0."
  echo "Please use \"ALLUXIO_MASTER_HOSTNAME\" instead."
  ALLUXIO_MASTER_HOSTNAME=${ALLUXIO_MASTER_ADDRESS}
fi

if [[ -n "${ALLUXIO_HOME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.home=${ALLUXIO_HOME}"
fi

if [[ -n "${ALLUXIO_LOGS_DIR}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR}"
fi

if [[ -n "${ALLUXIO_RAM_FOLDER}" ]]; then
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
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.realm="
ALLUXIO_JAVA_OPTS+=" -Djava.security.krb5.kdc="

# Master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_MASTER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.logger.type=MASTER_LOGGER"

# Worker specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_WORKER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_WORKER_JAVA_OPTS+=" -Dalluxio.logger.type=WORKER_LOGGER"

# Client specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_USER_JAVA_OPTS+=${ALLUXIO_JAVA_OPTS}
ALLUXIO_USER_JAVA_OPTS+=" -Dalluxio.logger.type=USER_LOGGER"

# A developer option to prepend Alluxio jars before ALLUXIO_CLASSPATH jars
if [[ -n "${ALLUXIO_PREPEND_ALLUXIO_CLASSES}" ]]; then
  export CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_JARS}:${ALLUXIO_CLASSPATH}"
else
  export CLASSPATH="${ALLUXIO_CONF_DIR}/:${ALLUXIO_CLASSPATH}:${ALLUXIO_JARS}"
fi
