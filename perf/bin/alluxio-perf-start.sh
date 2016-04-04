#!/usr/bin/env bash

function printUsage {
  echo "Usage: alluxio-perf-start.sh <NodeName> <TaskIdFrom> <TaskIdTo> <TotalTasks> <TestCase>"
  echo "This is used to start alluxio-perf on each node, see more in ./alluxio-perf"
}

# if less than 5 args specified, show usage
if [ $# -le 4 ]; then
  printUsage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

DEFAULT_PERF_LIBEXEC_DIR="$bin"/../libexec
ALLUXIO_PERF_LIBEXEC_DIR=${ALLUXIO_PERF_LIBEXEC_DIR:-$DEFAULT_PERF_LIBEXEC_DIR}
. $ALLUXIO_PERF_LIBEXEC_DIR/alluxio-perf-config.sh

if [ ! -d "$ALLUXIO_PERF_LOGS_DIR" ]; then
  echo "ALLUXIO_PERF_LOGS_DIR: $ALLUXIO_PERF_LOGS_DIR"
  mkdir -p $ALLUXIO_PERF_LOGS_DIR
fi

JAVACOMMAND="$JAVA -cp $ALLUXIO_PERF_CONF_DIR/:$ALLUXIO_PERF_JAR -Dalluxio.perf.home=$ALLUXIO_PERF_HOME -Dalluxio.perf.logger.type=PERF_SLAVE_LOGGER -Dalluxio.perf.slave.id="$2" -Dlog4j.configuration=file:$ALLUXIO_PERF_CONF_DIR/log4j.properties $ALLUXIO_JAVA_OPTS $ALLUXIO_PERF_JAVA_OPTS alluxio.perf.AlluxioPerfSlave"

for (( i = $2; i <= $3; i ++))
do
  echo "Starting alluxio-perf task-$i @ `hostname -f`"
  (nohup $JAVACOMMAND $1 $i $4 $5 > /dev/null 2>&1 ) &
done
sleep 1
