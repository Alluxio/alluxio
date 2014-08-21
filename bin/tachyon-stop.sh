#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

Usage="Usage: tachyon-stop.sh [-h] [component] 
Where component is one of:
  all\t\t\tStop local master/worker and remote workers. Default.
  master\t\tStop local master.
  worker\t\tStop local worker.
  workers\t\tStop local worker and all remote workers.

-h  display this help."
bin=`cd "$( dirname "$0" )"; pwd`

kill_master() {
  $bin/tachyon killAll tachyon.master.TachyonMaster
}

kill_worker() {
  $bin/tachyon killAll tachyon.worker.TachyonWorker
}

kill_remote_workers() {
  $bin/tachyon-slaves.sh $bin/tachyon killAll tachyon.worker.TachyonWorker
}

WHAT=${1:-all}

case "$WHAT" in
  master)
    kill_master
    ;;
  worker)
    kill_worker
    ;;
  workers)
    kill_worker
    kill_remote_workers
    ;;
  all)
    kill_master
    kill_worker
    kill_remote_workers
    ;;
  -h)
    echo -e "$Usage"
    exit 0
    ;;
  *)
    echo "Error: Invalid component: $WHAT"
    echo -e "$Usage"
    exit 1
    ;;
esac

