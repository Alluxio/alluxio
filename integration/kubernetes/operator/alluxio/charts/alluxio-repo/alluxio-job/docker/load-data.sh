#!/bin/bash
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

set -e

export ALLUXIO_HOME=/opt/alluxio

alluxio_env_vars=(
  ALLUXIO_CLASSPATH
  ALLUXIO_HOSTNAME
  ALLUXIO_JARS
  ALLUXIO_JAVA_OPTS
  ALLUXIO_MASTER_JAVA_OPTS
  ALLUXIO_PROXY_JAVA_OPTS
  ALLUXIO_RAM_FOLDER
  ALLUXIO_USER_JAVA_OPTS
  ALLUXIO_WORKER_JAVA_OPTS
  ALLUXIO_JOB_MASTER_JAVA_OPTS
  ALLUXIO_JOB_WORKER_JAVA_OPTS
)

function public::alluxio::ready() {
    local live_workers_str=`$ALLUXIO_HOME/bin/alluxio fsadmin report  summary | grep "Live Workers:" |  awk -F ' ' '{print $NF}'`
    local live_workers=${live_workers_str}
    while ! [[ $live_workers -gt 2 ]]
    do
    	echo "There is $live_workers waiting"
    	sleep 15
    	live_workers_str=`$ALLUXIO_HOME/bin/alluxio fsadmin report  summary | grep "Live Workers:" |  awk -F ' ' '{print $NF}'`
    	live_workers=${live_workers_str}
    done
    echo "There are ${live_workers} ready!"
}

function public::alluxio::load_data() {
	start_time=`date`
	start_time_second=`date +%s`
	set -x


	#cmd=`$ALLUXIO_HOME/bin/alluxio fs load /$DATA`
  #alluxio fs distributedLoad --replication=1 /
  ##alluxio fs distributedLoad --replication=$REPLICA /
	cmd=`$ALLUXIO_HOME/bin/alluxio fs distributedLoad --replication=$REPLICAS $DATA`
	end_time=`date`
	end_time_second=`date +%s`
	runtime=$((end_time_second-start_time_second))
	runtime_minutes=$((runtime/60))
	echo "The start time is $start_time, the end time is $end_time."
	echo "It took $runtime_minutes minutes in total."
}

function public::alluxio::init_conf() {
  local IFS=$'\n' # split by line instead of space
  for keyvaluepair in $(env); do
    # split around the first "="
    key=$(echo ${keyvaluepair} | cut -d= -f1)
    value=$(echo ${keyvaluepair} | cut -d= -f2-)
    if [[ "${alluxio_env_vars[*]}" =~ "${key}" ]]; then
      echo "export ${key}=\"${value}\"" >> $ALLUXIO_HOME/conf/alluxio-env.sh
    fi
  done
}

DATA=$1

if [ -z $DATA ]; then
	DATA='/'
fi

if [[ $DATA == '/'* ]]; then
    echo "use $DATA"
  else
    DATA=/$DATA
fi

THREAD=$2

if [ -z $THREAD ]; then
	THREAD='1'
fi



main() {
	public::alluxio::init_conf
	public::alluxio::ready
	public::alluxio::load_data
}

main
