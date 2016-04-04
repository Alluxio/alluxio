#!/usr/bin/env bash

# The following gives an example:

# The workspace dir in ALLUXIO
export ALLUXIO_PERF_WORKSPACE="/tmp/alluxio-perf-workspace"

# The report output path
export ALLUXIO_PERF_OUT_DIR="$ALLUXIO_PERF_HOME/result"

# The alluxio-perf master service address
ALLUXIO_PERF_MASTER_HOSTNAME="localhost"
ALLUXIO_PERF_MASTER_PORT=23333

# The alluxio master host name
ALLUXIO_MASTER_HOSTNAME="localhost"
ALLUXIO_MASTER_PORT=19998

# The number of threads per worker
ALLUXIO_PERF_THREADS_NUM=1

# The slave is considered to be failed if not register in this time
ALLUXIO_PERF_UNREGISTER_TIMEOUT_MS=10000

# If true, the AlluxioPerfSupervision will print the names of those running and remaining nodes
ALLUXIO_PERF_STATUS_DEBUG="true"

# If true, the test will abort when the number of failed nodes more than the threshold
ALLUXIO_PERF_FAILED_ABORT="true"
ALLUXIO_PERF_FAILED_PERCENTAGE=1

# If true the perf tool is installed on a shared file system visible to all slaves so copying
# and collating configurations either is a no-op or a local copy rather than a scp
ALLUXIO_PERF_SHARED_FS="false"

PERF_CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ALLUXIO_UNDERFS_ADDRESS=hdfs://localhost:9000


# Specify this value and add to the java opts to test against another fs client impl.
# 'HDFS' will use HDFS's native FileSystem client
# 'THCI' will use ALLUXIO's implementation (AbstractTFS) of HDFS's FileSystem interface
# Any other value, including empty, will default to ALLUXIO's native FileSystem client.
#ALLUXIO_PERF_UFS="ALLUXIO"
#export ALLUXIO_PERF_JAVA_OPTS+="-DALLUXIO.perf.ufs=$ALLUXIO_PERF_UFS"

export ALLUXIO_PERF_JAVA_OPTS+="
  -Dlog4j.configuration=file:$PERF_CONF_DIR/log4j.properties
  -Dalluxio.perf.failed.abort=$ALLUXIO_PERF_FAILED_ABORT
  -Dalluxio.perf.failed.percentage=$ALLUXIO_PERF_FAILED_PERCENTAGE
  -Dalluxio.perf.status.debug=$ALLUXIO_PERF_STATUS_DEBUG
  -Dalluxio.perf.master.hostname=$ALLUXIO_PERF_MASTER_HOSTNAME
  -Dalluxio.perf.master.port=$ALLUXIO_PERF_MASTER_PORT
  -Dalluxio.perf.work.dir=$ALLUXIO_PERF_WORKSPACE
  -Dalluxio.perf.out.dir=$ALLUXIO_PERF_OUT_DIR
  -Dalluxio.perf.threads.num=$ALLUXIO_PERF_THREADS_NUM
  -Dalluxio.perf.unregister.timeout.ms=$ALLUXIO_PERF_UNREGISTER_TIMEOUT_MS
  -Dalluxio.master.hostname=$ALLUXIO_MASTER_HOSTNAME
  -Dalluxio.master.port=$ALLUXIO_MASTER_PORT
  -Dalluxio.underfs.address=$ALLUXIO_UNDERFS_ADDRESS
"
