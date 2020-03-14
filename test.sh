#!/usr/bin/env bash
set -eu

# Run an experiment. Echos the output
# Arguments:
#  $1: The number of files to create
run_experiment() {
  echo "==== EXPERIMENT FOR ${1} ===="
  ./bin/alluxio-stop.sh local > /dev/null
  ./bin/alluxio formatJournal > /dev/null
  ./bin/alluxio formatWorker > /dev/null
  rm -rf logs/master.log
  pushd /home/zac/projects/xio/notepad/hadoop_services/hdfs > /dev/null
  docker-compose down > /dev/null
  docker-compose up -d > /dev/null
  popd > /dev/null
  sleep 5 # Let HDFS start up
  ./bin/alluxio runClass alluxio.cli.ClientProfiler -c hadoop -n "$1" -op createFiles -t 256 --data-size 0
  ./bin/alluxio-start.sh local > /dev/null
  ./bin/alluxio fs startSync / > /dev/null
  until cat logs/master.log | grep "Ended an active full sync of" > /dev/null
  do
    sleep 5
  done

  # 2nd full sync
  echo "Performing 2nd  full sync"
  ./bin/alluxio fs stopSync / > /dev/null
  ./bin/alluxio fs startSync / > /dev/null
  until [[ "$(cat logs/master.log | grep "Ended an active full sync of" | wc -l)" -eq "2" ]]
  do
    sleep 5
  done

  logs="$(cat logs/master.log | grep "TRACING")"
  echo "${logs}"

  echo "==== EXPERIMENT FOR ${1} ===="
}

main() {
  echo ":::: BEGIN EXPERIMENTS ::::" > experiments.log
  run_experiment "256000" >> experiments.log
  echo "`date` - 256k finished"
  run_experiment "512000" >> experiments.log
  echo "`date` - 512k finished"
  run_experiment "1024000" >> experiments.log
  echo "`date` - 1024k finished"
  run_experiment "2048000" >> experiments.log
  echo "`date` - 2048k finished"
  run_experiment "4096000" >> experiments.log
  echo "`date` - 4096k finished"
#  run_experiment "8192000" >> experiments.log
#  echo "`date` - 8192k finished"
  echo ":::: END EXPERIMENTS ::::" >> experiments.log
}

main "$@"


