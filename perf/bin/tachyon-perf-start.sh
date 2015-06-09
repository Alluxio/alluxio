#!/usr/bin/env bash

function printUsage {
  echo "Usage: tachyon-perf-start.sh <NodeName> <TaskIdFrom> <TaskIdTo> <TotalTasks> <TestCase>"
  echo "This is used to start tachyon-perf on each node, see more in ./tachyon-perf"
}

# if less than 5 args specified, show usage
if [ $# -le 4 ]; then
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

JAVACOMMAND="$JAVA -cp $TACHYON_PERF_CONF_DIR/:$TACHYON_PERF_JAR -Dtachyon.perf.home=$TACHYON_PERF_HOME -Dtachyon.perf.logger.type=PERF_SLAVE_LOGGER -Dtachyon.perf.slave.id="$2" -Dlog4j.configuration=file:$TACHYON_PERF_CONF_DIR/log4j.properties $TACHYON_JAVA_OPTS $TACHYON_PERF_JAVA_OPTS tachyon.perf.TachyonPerfSlave"

for (( i = $2; i <= $3; i ++))
do
  echo "Starting tachyon-perf task-$i @ `hostname -f`"
  (nohup $JAVACOMMAND $1 $i $4 $5 > /dev/null 2>&1 ) &
done
sleep 1
