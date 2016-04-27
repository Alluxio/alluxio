#!/usr/bin/env bash

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
# [ -f "$common_bin/alluxio-layout.sh" ] && . "$common_bin/alluxio-layout.sh"

# This will set the default installation for a tarball installation while os distributors can create
# their own alluxio-layout.sh file to set system installation locations.
if [[ -z "$ALLUXIO_SYSTEM_INSTALLATION" ]]; then
  VERSION=1.1.0-SNAPSHOT
  ALLUXIO_PREFIX=$(dirname $(dirname "$this"))
  ALLUXIO_HOME=${ALLUXIO_PREFIX}
  ALLUXIO_CONF_DIR="$ALLUXIO_HOME/conf"
  ALLUXIO_JARS="$ALLUXIO_HOME/assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar"
fi

export JAVA_HOME=${JAVA_HOME:-"$(dirname $(which java))/../.."}
export JAVA=${JAVA:-"${JAVA_HOME}/bin/java"}

if [[ $(uname -s) == Darwin ]]; then
  # Assuming Mac OS X
  export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-"/Volumes/ramdisk"}
  export ALLUXIO_JAVA_OPTS+="-Djava.security.krb5.realm= -Djava.security.krb5.kdc="
else
  # Assuming Linux
  export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-"/mnt/ramdisk"}
fi


# Make sure alluxio-env.sh exists
if [[ ! -e $ALLUXIO_CONF_DIR/alluxio-env.sh ]]; then
  echo "Cannot find alluxio-env.sh in $ALLUXIO_CONF_DIR."
  echo "Please create one manually or using '$ALLUXIO_HOME/bin/alluxio bootstrap-conf'."
  exit 1
fi

. $ALLUXIO_CONF_DIR/alluxio-env.sh

if [[ -z "${ALLUXIO_HOME}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.home=${ALLUXIO_HOME}"
fi

if [[ -z "${ALLUXIO_LOGS_DIR}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR}"
fi

if [[ -z "${ALLUXIO_RAM_FOLDER}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.dirs.path=${ALLUXIO_RAM_FOLDER}"
fi

if [[ -z "${ALLUXIO_WORKER_MEMORY_SIZE}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.tieredstore.level0.dirs.quota=${ALLUXIO_WORKER_MEMORY_SIZE}"
fi

if [[ -z "${ALLUXIO_MASTER_ADDRESS}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.master.hostname=${ALLUXIO_MASTER_ADDRESS}"
fi

if [[ -z "${ALLUXIO_UNDERFS_ADDRESS}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.underfs.address=${ALLUXIO_UNDERFS_ADDRESS}"
fi

if [[ -z "${ALLUXIO_WORKER_MEMORY_SIZES}" ]]; then
  ALLUXIO_JAVA_OPTS+=" -Dalluxio.worker.memory.size=${ALLUXIO_WORKER_MEMORY_SIZE}"
fi

ALLUXIO_JAVA_OPTS+=" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties"
ALLUXIO_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
ALLUXIO_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"

# Master specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.logger.type=MASTER_LOGGER"

# Worker specific parameters that will be shared to all workers based on ALLUXIO_JAVA_OPTS.
ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
ALLUXIO_WORKER_JAVA_OPTS+=" -Dalluxio.logger.type=WORKER_LOGGER"

# Client specific parameters based on ALLUXIO_JAVA_OPTS.
ALLUXIO_USER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
ALLUXIO_USER_JAVA_OPTS+=" -Dalluxio.logger.type=USER_LOGGER"

# A developer option to prepend Alluxio jars before ALLUXIO_CLASSPATH jars
if [[ -n "$ALLUXIO_PREPEND_ALLUXIO_CLASSES" ]]; then
  export CLASSPATH="$ALLUXIO_CONF_DIR/:$ALLUXIO_JARS:$ALLUXIO_CLASSPATH"
else
  export CLASSPATH="$ALLUXIO_CONF_DIR/:$ALLUXIO_CLASSPATH:$ALLUXIO_JARS"
fi
