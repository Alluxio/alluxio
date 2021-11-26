#!/usr/bin/env bash
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

function prepare {
  local i=$1
  echo "Generating files batch $i"
  ./bin/alluxio runClass alluxio.stress.cli.StressMasterBench \
    --operation CreateFile \
    --create-file-size 8k \
    `# 101 because one file is in the fixed subdirectory` \
    --stop-count 101 \
    `#we dont need fixed path files` \
    --fixed-count 1 \
    --clients 8 \
    --threads 8 \
    --warmup 0 \
    `# preparation will delete files generated from last invocation` \
    `# so skip preparation except the first run` \
    $(if [ $i -ne 1 ]; then echo -n --skip-prepare; fi) \
    `# id is used as the name of the subdirectory of this batch` \
    --id $i > /dev/null
}
export -f prepare

# sem is part of GNU parallel
SEM_CMD=
if which -s sem > /dev/null;
then
  echo "GNU parallel found, preparing in parallel"
  SEM_CMD="sem -j+0"
fi

prepare 1
for i in $(seq 2 100);
do
  $SEM_CMD prepare "$i"
done
if [ -n "$SEM_CMD" ];
then
  sem --wait
fi

echo "Compacting files"
./bin/alluxio runClass alluxio.stress.cli.client.Compaction \
  --source-base alluxio://localhost:19998/stress-master-base/files \
  --output-base alluxio://localhost:19998/stress-master-base/files \
  --output-in-place \
  --threads 4 \
  --compact-ratio 10 \
  --cluster
