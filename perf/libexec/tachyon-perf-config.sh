#!/usr/bin/env bash

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

export TACHYON_PERF_PREFIX=`dirname "$this"`/..
export TACHYON_PERF_HOME=${TACHYON_PERF_PREFIX}
export TACHYON_PERF_CONF_DIR="$TACHYON_PERF_HOME/conf"
export TACHYON_PERF_LOGS_DIR="$TACHYON_PERF_HOME/logs"
export TACHYON_PERF_JAR=$TACHYON_PERF_HOME/target/tachyon-perf-0.7.0-SNAPSHOT-jar-with-dependencies.jar
export JAVA="$JAVA_HOME/bin/java"

TACHYON_CONF_FILE=$TACHYON_PERF_HOME/../conf/tachyon-env.sh
if [ -e $TACHYON_CONF_FILE ] ; then
  . $TACHYON_CONF_FILE
fi

if [ -e $TACHYON_PERF_CONF_DIR/tachyon-perf-env.sh ] ; then
  . $TACHYON_PERF_CONF_DIR/tachyon-perf-env.sh
fi
