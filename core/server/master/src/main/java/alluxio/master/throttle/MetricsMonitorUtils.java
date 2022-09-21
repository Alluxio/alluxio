/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.throttle;

import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

/**
 * The Metrics monitor utils.
 */
public class MetricsMonitorUtils {
  /**
   * System level gauges.
   */
  public static class MemoryGaugeName {
    public static final String TOTAL_COMMITTED = "total.committed";
    public static final String TOTAL_INIT = "total.init";
    public static final String TOTAL_MAX = "total.max";
    public static final String TOTAL_USED = "total.used";

    public static final String NON_HEAP_COMMITTED = "non-heap.committed";
    public static final String NON_HEAP_INIT = "non-heap.init";
    public static final String NON_HEAP_MAX = "non-heap.max";
    public static final String NON_HEAP_USAGE = "non-heap.usage";
    public static final String NON_HEAP_USED = "non-heap.used";

    public static final String HEAP_COMMITTED = "heap.committed";
    public static final String HEAP_INIT = "heap.init";
    public static final String HEAP_MAX = "heap.max";
    public static final String HEAP_USAGE = "heap.usage";
    public static final String HEAP_USED = "heap.used";
  }

  /**
   * OS gauge names.
   */
  public static class OSGaugeName {
    public static final String OS_CPU_LOAD = "os.cpuLoad";
    public static final String OS_FREE_PHYSICAL_MEMORY = "os.freePhysicalMemory";
    public static final String OS_MAX_FILE_COUNT = "os.maxFileCount";
    public static final String OS_OPEN_FILE_COUNT = "os.openFileCount";
    public static final String OS_TOTAL_PHYSICAL_MEMORY = "os.totalPhysicalMemory";
  }

  /**
   * Server prefix.
   */
  public static class ServerPrefix {
    public static final String MASTER = "Master.";
    public static final String WORKER = "Worker.";
  }

  /**
   * Server gauge names.
   */
  public static class ServerGaugeName {
    public static final String RPC_QUEUE_LENGTH = MetricsSystem.getMetricName("RpcQueueLength");

    // JVM total pause time
    public static final String TOTAL_EXTRA_TIME
        = MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName());
  }

  /**
   * Filesystem gauge name.
   */
  public static class FileSystemGaugeName {
    public static final String MASTER_EDGE_CACHE_EVICTIONS
        = MetricKey.MASTER_EDGE_CACHE_EVICTIONS.getName();
    public static final String MASTER_EDGE_CACHE_HITS
        = MetricKey.MASTER_EDGE_CACHE_HITS.getName();
    public static final String MASTER_EDGE_CACHE_LOAD_TIMES
        = MetricKey.MASTER_EDGE_CACHE_LOAD_TIMES.getName();
    public static final String MASTER_EDGE_CACHE_MISSES
        = MetricKey.MASTER_EDGE_CACHE_MISSES.getName();
    public static final String MASTER_INODE_CACHE_EVICTIONS
        = MetricKey.MASTER_INODE_CACHE_EVICTIONS.getName();
    public static final String MASTER_INODE_CACHE_HIT_RATIO
        = MetricKey.MASTER_INODE_CACHE_HIT_RATIO.getName();
    public static final String MASTER_INODE_CACHE_LOAD_TIMES
        = MetricKey.MASTER_INODE_CACHE_LOAD_TIMES.getName();
    public static final String MASTER_INODE_CACHE_MISSES
        = MetricKey.MASTER_INODE_CACHE_MISSES.getName();
  }

  /**
   * Filesystem counter names.
   */
  public static class FileSystemCounterName {
    public static final String MASTER_COMPLETE_FILE_OPS
        = MetricKey.MASTER_COMPLETE_FILE_OPS.getName();
    public static final String MASTER_COMPLETED_OPERATION_RETRY_COUNT
        = MetricKey.MASTER_COMPLETED_OPERATION_RETRY_COUNT.getName();
    public static final String MASTER_CREATE_DIRECTORIES_OPS
        = MetricKey.MASTER_CREATE_DIRECTORIES_OPS.getName();
    public static final String MASTER_CREATE_FILE_OPS
        = MetricKey.MASTER_CREATE_FILES_OPS.getName();
    public static final String MASTER_DELETE_PATH_OPS
        = MetricKey.MASTER_DELETE_PATHS_OPS.getName();
    public static final String MASTER_DIRECTORIES_CREATED
        = MetricKey.MASTER_DIRECTORIES_CREATED.getName();
    public static final String MASTER_FILE_BLOCK_INFOS_GOT
        = MetricKey.MASTER_FILE_BLOCK_INFOS_GOT.getName();
    public static final String MASTER_FILE_INFOS_GOT
        = MetricKey.MASTER_FILE_INFOS_GOT.getName();
    public static final String MASTER_FILES_COMPLETED
        = MetricKey.MASTER_FILES_COMPLETED.getName();
    public static final String MASTER_FILES_CREATED
        = MetricKey.MASTER_FILES_CREATED.getName();
    public static final String MASTER_FILES_FREED
        = MetricKey.MASTER_FILES_FREED.getName();
    public static final String MASTER_FILES_PERSISTED
        = MetricKey.MASTER_FILES_PERSISTED.getName();
    public static final String MASTER_FREE_FILE_OPS
        = MetricKey.MASTER_FREE_FILE_OPS.getName();
    public static final String MASTER_GET_FILE_BLOCK_INFO_OPS
        = MetricKey.MASTER_GET_FILE_BLOCK_INFO_OPS.getName();
    public static final String MASTER_GET_FILE_INFO_OPS
        = MetricKey.MASTER_GET_FILE_INFO_OPS.getName();
    public static final String MASTER_GET_NEW_BLOCK_OPS
        = MetricKey.MASTER_GET_NEW_BLOCK_OPS.getName();
    public static final String MASTER_LISTING_CACHE_EVICTIONS
        = MetricKey.MASTER_LISTING_CACHE_EVICTIONS.getName();
    public static final String MASTER_LISTING_CACHE_HITS
        = MetricKey.MASTER_LISTING_CACHE_HITS.getName();
    public static final String MASTER_LISTING_CACHE_LOAD_TIMES
        = MetricKey.MASTER_LISTING_CACHE_LOAD_TIMES.getName();
    public static final String MASTER_LISTING_CACHE_MISSES
        = MetricKey.MASTER_LISTING_CACHE_MISSES.getName();
    public static final String MASTER_METADATA_SYNC_ACTIVE_PATHS
        = MetricKey.MASTER_METADATA_SYNC_ACTIVE_PATHS.getName();
    public static final String MASTER_METADATA_SYNC_FAIL
        = MetricKey.MASTER_METADATA_SYNC_FAIL.getName();
    public static final String MASTER_METADATA_SYNC_NO_CHANGE
        = MetricKey.MASTER_METADATA_SYNC_NO_CHANGE.getName();

    // This is the concerns
    public static final String MASTER_METADATA_SYNC_OPS_COUNT
        = MetricKey.MASTER_METADATA_SYNC_OPS_COUNT.getName();
    public static final String MASTER_METADATA_SYNC_PATHS_CANCEL
        = MetricKey.MASTER_METADATA_SYNC_PATHS_CANCEL.getName();
    public static final String MASTER_METADATA_SYNC_PATHS_FAIL
        = MetricKey.MASTER_METADATA_SYNC_PATHS_FAIL.getName();
    public static final String MASTER_METADATA_SYNC_PATHS_SUCCESS
        = MetricKey.MASTER_METADATA_SYNC_PATHS_SUCCESS.getName();
    public static final String MASTER_METADATA_SYNC_PENDING_PATHS
        = MetricKey.MASTER_METADATA_SYNC_PENDING_PATHS.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_CANCEL
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_CANCEL.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_FAIL
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_FAIL.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_OPS_COUNT
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_OPS_COUNT.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_PATHS
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_PATHS.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_RETRIES
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_RETRIES.getName();
    public static final String MASTER_METADATA_SYNC_PREFETCH_SUCCESS
        = MetricKey.MASTER_METADATA_SYNC_PREFETCH_SUCCESS.getName();
    public static final String MASTER_METADATA_SYNC_SKIPPED
        = MetricKey.MASTER_METADATA_SYNC_SKIPPED.getName();
    public static final String MASTER_METADATA_SYNC_SUCCESS
        = MetricKey.MASTER_METADATA_SYNC_SUCCESS.getName();
    public static final String MASTER_METADATA_SYNC_TIME_MS
        = MetricKey.MASTER_METADATA_SYNC_TIME_MS.getName();
    public static final String MASTER_MOUNT_OPS
        = MetricKey.MASTER_MOUNT_OPS.getName();
    public static final String MASTER_NEW_BLOCKS_GOT
        = MetricKey.MASTER_NEW_BLOCKS_GOT.getName();
    public static final String MASTER_PATHS_DELETED
        = MetricKey.MASTER_PATHS_DELETED.getName();
    public static final String MASTER_PATHS_MOUNTED
        = MetricKey.MASTER_PATHS_MOUNTED.getName();
    public static final String MASTER_PATHS_RENAMED
        = MetricKey.MASTER_PATHS_RENAMED.getName();
    public static final String MASTER_PATHS_UNMOUNTED
        = MetricKey.MASTER_PATHS_UNMOUNTED.getName();
    public static final String MASTER_RENAME_PATH_OPS
        = MetricKey.MASTER_RENAME_PATH_OPS.getName();
    public static final String MASTER_SET_ACL_OPS
        = MetricKey.MASTER_SET_ACL_OPS.getName();
    public static final String MASTER_SET_ATTRIBUTE_OPS
        = MetricKey.MASTER_SET_ATTRIBUTE_OPS.getName();
    public static final String MASTER_UFS_STATUS_CACHE_CHILDREN_SIZE
        = MetricKey.MASTER_UFS_STATUS_CACHE_CHILDREN_SIZE.getName();
    public static final String MASTER_UFS_STATUS_CACHE_SIZE
        = MetricKey.MASTER_UFS_STATUS_CACHE_SIZE.getName();
    public static final String MASTER_UNMOUNT_OPS
        = MetricKey.MASTER_UNMOUNT_OPS.getName();
    public static final String MASTER_GET_CONFIG_HASH_IN_PROGRESS
        = "Master.getConfigHashInProgress";
    public static final String MASTER_GET_CONFIGURATION_IN_PROGRESS
        = "Master.getConfigurationInProgress";
    public static final String MASTER_REGISTER_WORKER_START_IN_PROGRESS
        = "Master.registerWorkerStartInProgress";
  }
}
