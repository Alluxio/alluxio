#!/bin/bash -x
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

nproc_per_node=${1}
shift

PRO_TMP_DIR=/tmp/pro_metrics/
rm -rf $PRO_TMP_DIR
mkdir -p $PRO_TMP_DIR
export PROMETHEUS_MULTIPROC_DIR=$PRO_TMP_DIR

WORLD_SIZE=${WORLD_SIZE:-1}
if [[ ${WORLD_SIZE} -eq 1 ]]; then
  python -m torch.distributed.run --nproc_per_node $num_proc $@
  return $?
fi

python -m torch.distributed.launch --master_addr=${MASTER_ADDR} --master_port=${MASTER_PORT} --nnodes=${WORLD_SIZE} --node_rank=${RANK} --nproc_per_node=${nproc_per_node} $@
