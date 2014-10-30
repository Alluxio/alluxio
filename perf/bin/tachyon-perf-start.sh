#!/usr/bin/env bash

function printUsage {
  echo "Usage: tachyon-perf-start.sh <NodeName> <TaskId> <TaskType>"
  echo "This is used to start tachyon-perf on each node, see more in ./tachyon-perf"
}

# if less than 3 args specified, show usage
if [ $# -le 2 ]; then
  printUsage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

DEFAULT_PERF_LIBEXEC_DIR="$bin"/../libexec
TACHYON_PERF_LIBEXEC_DIR=${TACHYON_PERF_LIBEXEC_DIR:-$DEFAULT_PERF_LIBEXEC_DIR}
. $TACHYON_PERF_LIBEXEC_DIR/tachyon-perf-config.sh

if [ ! -d "$TACHYON_PERF_LOGS_DIR" ]; then
  echo "TACHYON_PERF_LOGS_DIR: $TACHYON_PERF_LOGS_DIR"
  mkdir -p $TACHYON_PERF_LOGS_DIR
fi

JAVACOMMAND="$JAVA -cp $TACHYON_PERF_CONF_DIR/:$TACHYON_PERF_JAR -Dtachyon.perf.home=$TACHYON_PERF_HOME -Dtachyon.perf.logger.type=PERF_SLAVE_LOGGER -Dlog4j.configuration=file:$TACHYON_PERF_CONF_DIR/log4j.properties $TACHYON_JAVA_OPTS $TACHYON_PERF_JAVA_OPTS tachyon.perf.TachyonPerfSlave"

echo "Starting tachyon-perf task-$2 @ `hostname -f`"
(nohup $JAVACOMMAND $* > /dev/null 2>&1 ) &

sleep 1
