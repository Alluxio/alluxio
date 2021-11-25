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

for i in $(seq 1 10);
do
  echo "Generating files batch $i"
  ./bin/alluxio runClass alluxio.stress.cli.StressMasterBench \
    --operation CreateFile \
    --create-file-size 8k \
    --stop-count 100 \
    `#we dont need fixed path files)` \
    --fixed-count 1 \
    --clients 8 \
    --warmup 0 \
    `# preparation will delete files generated from last invocation` \
    `# so skip preparation except the first run` \
    $(if [ $i -ne 1 ]; then echo -n "--skip-prepare"; fi) \
    --id "$i" > /dev/null
done

./bin/alluxio runClass alluxio.stress.cli.client.Compaction \
  --source-base alluxio://localhost:19998/stress-master-base/files \
  --output-base alluxio://localhost:19998/stress-master-base/files \
  --output-in-place \
  --threads 2 \
  --compact-ratio 10
