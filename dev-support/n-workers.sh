#!/usr/bin/env bash

##
## n-workers is a CLI for spinning up multiple workers on the same node.
## The core idea is for ease of testing, being able to spin up multiple
## workers lets you test things very quickly.
##
## Example for how to run
##
## $ ./bin/tachyon format
## $ ./bin/tachyon-start.sh master
## $ ./dev-support/n-workers.sh 10
##

# fail is a statement fails
set -e
# fail if a pipe fails
set -o pipefail

bin=`cd "$( dirname "$0" )"; pwd`

. "$bin"/../libexec/tachyon-config.sh

fail() {
  echo -e "$@" >&2
  exit 1
}

deploy_worker() {
  local port="$1"
  local data_port="$2"

  local worker_dir="/tmp/tachyon/tachyonworker/$port"
  mkdir -p "$worker_dir"

  export TACHYON_WORKER_JAVA_OPTS="
  $TACHYON_WORKER_JAVA_OPTS
  -Dtachyon.worker.port=$port
  -Dtachyon.worker.data.port=$data_port
  -Dtachyon.worker.memory.size=1GB
  -Dtachyon.worker.data.folder=$worker_dir
"

  (nohup $JAVA \
    -cp $CLASSPATH \
    -Dtachyon.home=$TACHYON_HOME \
    -Dtachyon.logger.type="WORKER_LOGGER" \
    $TACHYON_WORKER_JAVA_OPTS \
    tachyon.worker.TachyonWorker > $TACHYON_LOGS_DIR/worker-${port}.out 2>&1 ) &
}

main() {
  local usage="n-workers.sh <num-workers>"
  if [ $# -lt 1 ]; then
    fail "num-workers is required\n$usage"
  fi

  local num_workers="$1"
  
  for i in $(seq 1 $num_workers); do
    #TODO should port be 0 so the system can find a new one?
    # there is benifit in determanistic ports, so keeping this for now
    deploy_worker "29${i}8" "29${i}9"
  done
}

main "$@"
