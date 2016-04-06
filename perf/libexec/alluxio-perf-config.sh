#!/usr/bin/env bash

# Included in all the Alluxio Perf scripts

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

# Alluxio Perf directories
export ALLUXIO_PERF_PREFIX=`dirname "$this"`/..
export ALLUXIO_PERF_HOME=${ALLUXIO_PERF_PREFIX}
export ALLUXIO_PERF_CONF_DIR="$ALLUXIO_PERF_HOME/conf"
export ALLUXIO_PERF_LOGS_DIR="$ALLUXIO_PERF_HOME/logs"

# load Alluxio libexec
ALLUXIO_LIBEXEC_FILE=$ALLUXIO_PERF_HOME/../libexec/alluxio-config.sh
if [ -e $ALLUXIO_LIBEXEC_FILE ] ; then
  . $ALLUXIO_LIBEXEC_FILE
fi

# Alluxio Perf settings
VERSION=${VERSION:-1.0.2-SNAPSHOT}
export ALLUXIO_PERF_JAR=$ALLUXIO_PERF_HOME/target/alluxio-perf-${VERSION}-jar-with-dependencies.jar

# Make sure alluxio-env.sh exists
if [ ! -e $ALLUXIO_PERF_CONF_DIR/alluxio-perf-env.sh ]; then
  echo "Cannot find alluxio-perf-env.sh in $ALLUXIO_CONF_DIR."
  echo "Please create one manually by copying '$ALLUXIO_PERF_CONF_DIR/alluxio-perf-env.sh.template'"
  exit 1
fi
. $ALLUXIO_PERF_CONF_DIR/alluxio-perf-env.sh

