#!/bin/bash -x

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
