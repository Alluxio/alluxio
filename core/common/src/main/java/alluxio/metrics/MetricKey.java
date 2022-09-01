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

package alluxio.metrics;

import alluxio.conf.PropertyKey;
import alluxio.grpc.MetricType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Metric keys. This class provides a set of pre-defined Alluxio metric keys.
 */
@ThreadSafe
public final class MetricKey implements Comparable<MetricKey> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricKey.class);

  /**
   * A map from default metric key's string name to the metric.
   * This map must be the first to initialize within this file.
   */
  private static final Map<String, MetricKey> METRIC_KEYS_MAP = new ConcurrentHashMap<>();

  /** Metric name. */
  private final String mName;
  /** Metrics name without instance prefix. */
  private final String mMetricName;

  /** Metric key description. */
  private final String mDescription;

  /** The type of this metric. */
  private final MetricType mMetricType;

  /** Whether the metric can be aggregated at cluster level. */
  private final boolean mIsClusterAggregated;

  /**
   * @param name name of this metric
   * @param description description of this metric
   * @param metricType the metric type of this metric
   * @param isClusterAggregated whether this metric can be aggregated at cluster level
   */
  private MetricKey(String name, String description,
      MetricType metricType, boolean isClusterAggregated) {
    mName = Preconditions.checkNotNull(name, "name");
    mDescription = Strings.isNullOrEmpty(description) ? "N/A" : description;
    mMetricType = metricType;
    mIsClusterAggregated = isClusterAggregated;
    mMetricName = extractMetricName();
  }

  /**
   * @param name String of this property
   */
  private MetricKey(String name) {
    this(name, null, MetricType.GAUGE, false);
  }

  /**
   * @param name name of a metric
   * @return whether the given name is a valid Metric name
   */
  public static boolean isValid(String name) {
    return METRIC_KEYS_MAP.containsKey(name);
  }

  /**
   * Parses a given name and return its corresponding {@link MetricKey},
   * throwing exception if no such a Metric can be found.
   *
   * @param name name of the Metric key
   * @return corresponding Metric
   */
  public static MetricKey fromString(String name) {
    MetricKey key = METRIC_KEYS_MAP.get(name);
    if (key != null) {
      return key;
    }
    throw new IllegalArgumentException(MessageFormat.format("Invalid metric key {0}", name));
  }

  /**
   * @return all pre-defined Alluxio metric keys
   */
  public static Collection<? extends MetricKey> allMetricKeys() {
    return Sets.newHashSet(METRIC_KEYS_MAP.values());
  }

  /**
   * Gets all the metric keys that belong to the given instance type
   * and should be reported.
   *
   * @param instanceType the instance type that should be reported
   * @return all pre-defined Alluxio metric keys
   */
  public static Set<MetricKey> allShouldReportMetricKeys(MetricsSystem.InstanceType instanceType) {
    Set<MetricKey> shouldReportMetrics = new HashSet<>();
    for (Map.Entry<String, MetricKey> entry : METRIC_KEYS_MAP.entrySet()) {
      if (entry.getKey().startsWith(instanceType.toString())
          && entry.getValue().isClusterAggregated()) {
        shouldReportMetrics.add(entry.getValue());
      }
    }
    return shouldReportMetrics;
  }

  /**
   * @return the name of the Metric
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the name of the Metric without instance prefix
   */
  public String getMetricName() {
    return mMetricName;
  }

  private String extractMetricName() {
    String[] pieces = mName.split("\\.");
    if (pieces.length <= 1) {
      return mName;
    }
    return pieces[1];
  }

  /**
   * @return the description of a Metric
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the metric type of a Metric
   */
  public MetricType getMetricType() {
    return mMetricType;
  }

  /**
   * @return true if this metrics can be aggregated at cluster level
   */
  public boolean isClusterAggregated() {
    return mIsClusterAggregated;
  }

  /**
   * Builder to create {@link MetricKey} instances. Note that, <code>Builder.build()</code> will
   * throw exception if there is an existing Metric built with the same name.
   */
  public static final class Builder {
    private String mName;
    private String mDescription;
    private boolean mIsClusterAggregated;
    private MetricType mMetricType = MetricType.GAUGE;

    /**
     * @param name name of the Metric
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param name name for the Metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param description of the Metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * @param isClusterAggregated whether this metric can be aggregated at cluster level
     * @return the updated builder instance
     */
    public MetricKey.Builder setIsClusterAggregated(boolean isClusterAggregated) {
      mIsClusterAggregated = isClusterAggregated;
      return this;
    }

    /**
     * @param metricType the metric type of this metric
     * @return the updated builder instance
     */
    public MetricKey.Builder setMetricType(MetricType metricType) {
      mMetricType = metricType;
      return this;
    }

    /**
     * Creates and registers the Metric key.
     *
     * @return the created Metric key instance
     */
    public MetricKey build() {
      MetricKey key = new MetricKey(mName, mDescription, mMetricType, mIsClusterAggregated);
      Preconditions.checkState(MetricKey.register(key), "Cannot register existing metric \"%s\"",
          mName);
      return key;
    }
  }

  private static final String EXECUTOR_STRING = "%1$s.submitted is a meter of the tasks submitted"
      + " to the executor. %1$s.completed is a meter of the tasks completed by the executor."
      + " %1$s.activeTaskQueue is exponentially-decaying random reservoir of the number of"
      + " active tasks (running or submitted) at the executor calculated each time a new"
      + " task is added to the executor. The max value is the maximum number of active"
      + " tasks at any time during execution. %1$s.running is the number of tasks actively"
      + " being run by the executor. %1$s.idle is the time spent idling by the submitted"
      + " tasks (i.e. waiting the the queue before being executed)."
      + " %1$s.duration is the time spent running the submitted tasks."
      + " If the executor is a thread pool executor then %1$s.queueSize is"
      + " the size of the task queue.";

  // Master metrics
  // Absent cache stats
  public static final MetricKey MASTER_ABSENT_CACHE_HITS =
      new Builder("Master.AbsentCacheHits")
          .setDescription("Number of cache hits on the absent cache")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ABSENT_CACHE_MISSES =
      new Builder("Master.AbsentCacheMisses")
          .setDescription("Number of cache misses on the absent cache")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ABSENT_CACHE_SIZE =
      new Builder("Master.AbsentCacheSize")
          .setDescription("Size of the absent cache")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ABSENT_PATH_CACHE_QUEUE_SIZE =
      new Builder("Master.AbsentPathCacheQueueSize")
          .setDescription("Alluxio maintains a cache of absent UFS paths. "
              + "This is the number of UFS paths being processed.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Edge cache stats
  public static final MetricKey MASTER_EDGE_CACHE_EVICTIONS =
      new Builder("Master.EdgeCacheEvictions")
          .setDescription("Total number of edges (inode metadata) that was evicted from cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_HITS =
      new Builder("Master.EdgeCacheHits")
          .setDescription("Total number of hits in the edge (inode metadata) cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_LOAD_TIMES =
      new Builder("Master.EdgeCacheLoadTimes")
          .setDescription("Total load times in the edge (inode metadata) cache "
              + "that resulted from a cache miss. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_MISSES =
      new Builder("Master.EdgeCacheMisses")
          .setDescription("Total number of misses in the edge (inode metadata) cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_SIZE =
      new Builder("Master.EdgeCacheSize")
          .setDescription("Total number of edges (inode metadata) cached. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Master file statistics
  public static final MetricKey MASTER_FILES_PINNED =
      new Builder("Master.FilesPinned")
          .setDescription("Total number of currently pinned files")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_FILES_TO_PERSIST =
      new Builder("Master.FilesToBePersisted")
          .setDescription("Total number of currently to be persisted files")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_FILE_SIZE =
      new Builder("Master.FileSize")
          .setDescription("File size distribution")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_EVICTIONS =
      new Builder("Master.InodeCacheEvictions")
          .setDescription("Total number of inodes that was evicted from the cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_HITS =
      new Builder("Master.InodeCacheHits")
          .setDescription("Total number of hits in the inodes (inode metadata) cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_HIT_RATIO =
      new Builder("Master.InodeCacheHitRatio")
          .setDescription("Inode Cache hit ratio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_LOAD_TIMES =
      new Builder("Master.InodeCacheLoadTimes")
          .setDescription("Total load times in the inodes (inode metadata) cache "
              + "that resulted from a cache miss.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_MISSES =
      new Builder("Master.InodeCacheMisses")
          .setDescription("Total number of misses in the inodes (inode metadata) cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_SIZE =
      new Builder("Master.InodeCacheSize")
          .setDescription("Total number of inodes (inode metadata) cached.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_SPACE_FREE_BYTES =
      new Builder("Master.JournalFreeBytes")
          .setDescription("Bytes left on the journal disk(s) for an Alluxio master. "
              + "This metric is only valid on Linux and when embedded journal is used. "
              + "Use this metric to monitor whether your journal is running out of disk space.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_SPACE_FREE_PERCENT =
      new Builder("Master.JournalFreePercent")
          .setDescription("Percentage of free space left on the journal disk(s) "
              + "for an Alluxio master."
              + "This metric is only valid on Linux and when embedded journal is used. "
              + "Use this metric to monitor whether your journal is running out of disk space.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LOST_BLOCK_COUNT =
      new Builder("Master.LostBlockCount")
          .setDescription("Count of lost unique blocks")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_TO_REMOVE_BLOCK_COUNT =
      new Builder("Master.ToRemoveBlockCount")
          .setDescription("Count of block replicas to be removed from the workers. "
              + "If 1 block is to be removed from 2 workers, 2 will be counted here.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LOST_FILE_COUNT =
      new Builder("Master.LostFileCount")
          .setDescription("Count of lost files. This number is cached and may not be in sync with "
              + String.format("%s", MetricKey.MASTER_LOST_BLOCK_COUNT.getName()))
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_TOTAL_PATHS =
      new Builder("Master.TotalPaths")
          .setDescription("Total number of files and directory in Alluxio namespace")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_UNIQUE_BLOCKS =
      new Builder("Master.UniqueBlocks")
          .setDescription("Total number of unique blocks in Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_TOTAL_RPCS =
      new Builder("Master.TotalRpcs")
          .setDescription("Throughput of master RPC calls. This metrics indicates how busy the"
              + " master is serving client and worker requests")
          .setMetricType(MetricType.TIMER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_TOTAL_BLOCK_REPLICA_COUNT =
      new Builder("Master.BlockReplicaCount")
          .setDescription("Total number of block replicas in Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_HEAP_SIZE =
      new Builder("Master.InodeHeapSize")
          .setDescription("An estimate of the inode heap size")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_BLOCK_HEAP_SIZE =
      new Builder("Master.BlockHeapSize")
          .setDescription("An estimate of the blocks heap size")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_RPC_QUEUE_LENGTH =
      new Builder("Master.RpcQueueLength")
          .setDescription("Length of the master rpc queue. "
              + "Use this metric to monitor the RPC pressure on master.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_REPLICA_MGMT_ACTIVE_JOB_SIZE =
      new Builder("Master.ReplicaMgmtActiveJobSize")
          .setDescription("Number of active block replication/eviction jobs. "
              + "These jobs are created by the master to maintain the block replica factor. "
              + "The value is an estimate with lag. ")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  // Backup Restore
  public static final MetricKey MASTER_LAST_BACKUP_ENTRIES_COUNT =
      new Builder("Master.LastBackupEntriesCount")
          .setDescription("The total number of entries written "
              + "in the last leading master metadata backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_TIME_MS =
      new Builder("Master.LastBackupTimeMs")
          .setDescription("The process time of the last backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_RESTORE_COUNT =
      new Builder("Master.LastBackupRestoreCount")
          .setDescription("The total number of entries restored from backup "
              + "when a leading master initializes its metadata")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_RESTORE_TIME_MS =
      new Builder("Master.LastBackupRestoreTimeMs")
          .setDescription("The process time of the last restore from backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Logical operations and results
  public static final MetricKey MASTER_DIRECTORIES_CREATED =
      new Builder("Master.DirectoriesCreated")
          .setDescription("Total number of the succeed CreateDirectory operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILE_BLOCK_INFOS_GOT =
      new Builder("Master.FileBlockInfosGot")
          .setDescription("Total number of succeed GetFileBlockInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILE_INFOS_GOT =
      new Builder("Master.FileInfosGot")
          .setDescription("Total number of the succeed GetFileInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_COMPLETED =
      new Builder("Master.FilesCompleted")
          .setDescription("Total number of the succeed CompleteFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_CREATED =
      new Builder("Master.FilesCreated")
          .setDescription("Total number of the succeed CreateFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_FREED =
      new Builder("Master.FilesFreed")
          .setDescription("Total number of succeed FreeFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_PERSISTED =
      new Builder("Master.FilesPersisted")
          .setDescription("Total number of successfully persisted files")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_NEW_BLOCKS_GOT =
      new Builder("Master.NewBlocksGot")
          .setDescription("Total number of the succeed GetNewBlock operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_DELETED =
      new Builder("Master.PathsDeleted")
          .setDescription("Total number of the succeed Delete operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_MOUNTED =
      new Builder("Master.PathsMounted")
          .setDescription("Total number of succeed Mount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_RENAMED =
      new Builder("Master.PathsRenamed")
          .setDescription("Total number of succeed Rename operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_UNMOUNTED =
      new Builder("Master.PathsUnmounted")
          .setDescription("Total number of succeed Unmount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_COMPLETED_OPERATION_RETRY_COUNT =
      new Builder("Master.CompletedOperationRetryCount")
          .setDescription("Total number of completed operations that has been retried by client.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_COMPLETE_FILE_OPS =
      new Builder("Master.CompleteFileOps")
          .setDescription("Total number of the CompleteFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_CREATE_DIRECTORIES_OPS =
      new Builder("Master.CreateDirectoryOps")
          .setDescription("Total number of the CreateDirectory operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_CREATE_FILES_OPS =
      new Builder("Master.CreateFileOps")
          .setDescription("Total number of the CreateFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_DELETE_PATHS_OPS =
      new Builder("Master.DeletePathOps")
          .setDescription("Total number of the Delete operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FREE_FILE_OPS =
      new Builder("Master.FreeFileOps")
          .setDescription("Total number of FreeFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_FILE_BLOCK_INFO_OPS =
      new Builder("Master.GetFileBlockInfoOps")
          .setDescription("Total number of GetFileBlockInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_FILE_INFO_OPS =
      new Builder("Master.GetFileInfoOps")
          .setDescription("Total number of the GetFileInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_NEW_BLOCK_OPS =
      new Builder("Master.GetNewBlockOps")
          .setDescription("Total number of the GetNewBlock operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_EVICTIONS =
      new Builder("Master.ListingCacheEvictions")
          .setDescription("The total number of evictions in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_HITS =
      new Builder("Master.ListingCacheHits")
          .setDescription("The total number of hits in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_LOAD_TIMES =
      new Builder("Master.ListingCacheLoadTimes")
          .setDescription("The total load time (in nanoseconds) in master listing cache "
              + "that resulted from a cache miss.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_MISSES =
      new Builder("Master.ListingCacheMisses")
          .setDescription("The total number of misses in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_SIZE =
      new Builder("Master.ListingCacheSize")
          .setDescription("The size of master listing cache")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_MOUNT_OPS =
      new Builder("Master.MountOps")
          .setDescription("Total number of Mount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_RENAME_PATH_OPS =
      new Builder("Master.RenamePathOps")
          .setDescription("Total number of Rename operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_SET_ACL_OPS =
      new Builder("Master.SetAclOps")
          .setDescription("Total number of SetAcl operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_SET_ATTRIBUTE_OPS =
      new Builder("Master.SetAttributeOps")
          .setDescription("Total number of SetAttribute operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_UNMOUNT_OPS =
      new Builder("Master.UnmountOps")
          .setDescription("Total number of Unmount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_INODE_LOCK_POOL_SIZE =
      new Builder("Master.InodeLockPoolSize")
          .setDescription("The size of master inode lock pool")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_LOCK_POOL_SIZE =
      new Builder("Master.EdgeLockPoolSize")
          .setDescription("The size of master edge lock pool")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_AUDIT_LOG_ENTRIES_SIZE =
      new Builder("Master.AuditLogEntriesSize")
          .setDescription("The size of the audit log entries blocking queue")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  public static final MetricKey PROXY_AUDIT_LOG_ENTRIES_SIZE =
      new Builder("Proxy.AuditLogEntriesSize")
          .setDescription("The size of the audit log entries blocking queue")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Metadata sync metrics
  public static final MetricKey MASTER_METADATA_SYNC_OPS_COUNT =
      new Builder("Master.MetadataSyncOpsCount")
          .setDescription("The number of metadata sync operations. "
              + "Each sync operation corresponds to one InodeSyncStream instance.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_TIME_MS =
      new Builder("Master.MetadataSyncTimeMs")
          .setDescription("The total time elapsed in all InodeSyncStream instances")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_SKIPPED =
      new Builder("Master.MetadataSyncSkipped")
          .setDescription("The number of InodeSyncStream that are skipped because "
              + "the Alluxio metadata is fresher than "
              + PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL)
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_NO_CHANGE =
      new Builder("Master.MetadataSyncNoChange")
          .setDescription("The number of InodeSyncStream that finished with no change to inodes.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_SUCCESS =
      new Builder("Master.MetadataSyncSuccess")
          .setDescription("The number of InodeSyncStream that succeeded")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_FAIL =
      new Builder("Master.MetadataSyncFail")
          .setDescription("The number of InodeSyncStream that failed, either partially or fully")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PENDING_PATHS =
      new Builder("Master.MetadataSyncPendingPaths")
          .setDescription("The number of pending paths from all active InodeSyncStream instances,"
              + "waiting for metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_ACTIVE_PATHS =
      new Builder("Master.MetadataSyncActivePaths")
          .setDescription("The number of in-progress paths from all InodeSyncStream instances")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PATHS_CANCEL =
      new Builder("Master.MetadataSyncPathsCancel")
          .setDescription("The number of pending paths from all InodeSyncStream instances that "
              + "are ignored in the end instead of processed")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PATHS_SUCCESS =
      new Builder("Master.MetadataSyncPathsSuccess")
          .setDescription("The number of paths sync-ed from all InodeSyncStream instances")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PATHS_FAIL =
      new Builder("Master.MetadataSyncPathsFail")
          .setDescription("The number of paths that failed during metadata sync"
              + " from all InodeSyncStream instances")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_OPS_COUNT =
      new Builder("Master.MetadataSyncPrefetchOpsCount")
          .setDescription("The number of prefetch operations handled by the prefetch thread pool")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_SUCCESS =
      new Builder("Master.MetadataSyncPrefetchSuccess")
          .setDescription("Number of successful prefetch jobs from metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_FAIL =
      new Builder("Master.MetadataSyncPrefetchFail")
          .setDescription("Number of failed prefetch jobs from metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_CANCEL =
      new Builder("Master.MetadataSyncPrefetchCancel")
          .setDescription("Number of cancelled prefetch jobs from metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_RETRIES =
      new Builder("Master.MetadataSyncPrefetchRetries")
          .setDescription("Number of retries to get from prefetch jobs from metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_PATHS =
      new Builder("Master.MetadataSyncPrefetchPaths")
          .setDescription("Total number of UFS paths fetched by prefetch jobs from metadata sync")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_UFS_STATUS_CACHE_SIZE =
      new Builder("Master.UfsStatusCacheSize")
          .setDescription("Total number of Alluxio paths being processed by the "
              + "metadata sync prefetch thread pool.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_UFS_STATUS_CACHE_CHILDREN_SIZE =
      new Builder("Master.UfsStatusCacheChildrenSize")
          .setDescription("Total number of UFS file metadata cached."
              + " The cache is used during metadata sync.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_EXECUTOR =
      new Builder("Master.MetadataSyncPrefetchExecutor")
          .setDescription(String.format("Metrics concerning the master metadata sync prefetch"
              + "executor threads. " + EXECUTOR_STRING, "Master.MetadataSyncPrefetchExecutor"))
          .setMetricType(MetricType.EXECUTOR_SERVICE)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_PREFETCH_EXECUTOR_QUEUE_SIZE =
      new Builder("Master.MetadataSyncPrefetchExecutorQueueSize")
          .setDescription("The number of queuing prefetch tasks in the metadata sync thread pool"
              + " controlled by " + PropertyKey.MASTER_METADATA_SYNC_UFS_PREFETCH_POOL_SIZE)
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_EXECUTOR =
      new Builder("Master.MetadataSyncExecutor")
          .setDescription(String.format("Metrics concerning the master metadata sync "
              + "executor threads. " + EXECUTOR_STRING, "Master.MetadataSyncExecutor"))
          .setMetricType(MetricType.EXECUTOR_SERVICE)
          .build();
  public static final MetricKey MASTER_METADATA_SYNC_EXECUTOR_QUEUE_SIZE =
      new Builder("Master.MetadataSyncExecutorQueueSize")
          .setDescription("The number of queuing sync tasks in the metadata sync thread pool"
              + " controlled by " + PropertyKey.MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE)
          .setMetricType(MetricType.GAUGE)
          .build();

  // Journal metrics
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_GENERATE_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotGenerateTimer")
          .setDescription("Describes the amount of time taken to generate local journal snapshots"
              + " on this master. Only valid when using the embedded journal. Use this metric to "
              + "measure the performance of Alluxio's snapshot generation.")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotDownloadGenerate")
          .setDescription("Describes the amount of time taken to download journal snapshots from "
              + "other masters in the cluster. Only valid when using the embedded journal. Use "
              + "this metric to determine if there are potential communication bottlenecks "
              + "between Alluxio masters.")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_INSTALL_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotInstallTimer")
          .setDescription("Describes the amount of time taken to install a downloaded journal "
              + "snapshot from another master. Only valid only when using the embedded journal. "
              + "Use this metric to determine the performance of Alluxio when installing "
              + "snapshots from the leader. Higher numbers may indicate a slow disk or CPU "
              + "contention.")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLAY_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotReplayTimer")
          .setDescription("Describes the amount of time taken to replay a journal snapshot onto "
              + "the master's state machine. Only valid only when using the embedded journal. Use"
              + " this metric to determine the performance of Alluxio when replaying journal "
              + "snapshot file. Higher numbers may indicate a slow disk or CPU contention")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_LAST_INDEX =
      new Builder("Master.EmbeddedJournalSnapshotLastIndex")
          .setDescription("Represents the latest journal index that was recorded by this master "
              + "in the most recent local snapshot or from a snapshot downloaded from another "
              + "master in the cluster. Only valid when using the embedded journal.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROLE_ID =
      new Builder("Master.RoleId")
          .setDescription("Display master role id")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_FLUSH_FAILURE =
      new Builder("Master.JournalFlushFailure")
          .setDescription("Total number of failed journal flush")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOURNAL_FLUSH_TIMER =
      new Builder("Master.JournalFlushTimer")
          .setDescription("The timer statistics of journal flush")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_JOURNAL_SEQUENCE_NUMBER =
      new Builder("Master.JournalSequenceNumber")
          .setDescription("Current journal sequence number")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_LAST_CHECKPOINT_TIME =
      new Builder("Master.JournalLastCheckPointTime")
          .setDescription("Last Journal Checkpoint Time")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_ENTRIES_SINCE_CHECKPOINT =
      new Builder("Master.JournalEntriesSinceCheckPoint")
          .setDescription("Journal entries since last checkpoint")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_LAST_APPLIED_COMMIT_INDEX =
      new Builder("Master.JournalLastAppliedCommitIndex")
          .setDescription("The last raft log index which was applied to the state machine")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_CHECKPOINT_WARN =
      new Builder("Master.JournalCheckpointWarn")
          .setDescription(String.format("If the raft log index exceeds %s, and the "
              + "last checkpoint exceeds %s, it returns 1 to indicate that a warning"
              + " is required, otherwise it returns 0",
              PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES.getName(),
              PropertyKey.MASTER_WEB_JOURNAL_CHECKPOINT_WARNING_THRESHOLD_TIME.getName()))
         .setMetricType(MetricType.GAUGE)
         .build();
  public static final MetricKey MASTER_JOURNAL_GAIN_PRIMACY_TIMER =
      new Builder("Master.JournalGainPrimacyTimer")
          .setDescription("The timer statistics of journal gain primacy")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_CATCHUP_TIMER =
      new Builder("Master.UfsJournalCatchupTimer")
          .setDescription("The timer statistics of journal catchup"
              + "Only valid when ufs journal is used. "
              + "This provides a summary of how long a standby master"
              + " takes to catch up with primary master,"
              + " and should be monitored if master transition takes too long")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER =
      new Builder("Master.UfsJournalFailureRecoverTimer")
          .setDescription("The timer statistics of ufs journal failure recover")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS =
      new Builder("Master.UfsJournalInitialReplayTimeMs")
          .setDescription("The process time of the ufs journal initial replay."
              + "Only valid when ufs journal is used."
              + " It records the time it took for the very first journal replay. "
              + "Use this metric to monitor when your master boot-up time is high。")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Job metrics
  public static final MetricKey MASTER_JOB_CANCELED =
      new Builder("Master.JobCanceled")
          .setDescription("The number of canceled status job")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_COMPLETED =
      new Builder("Master.JobCompleted")
          .setDescription("The number of completed status job")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_COUNT =
      new Builder("Master.JobCount")
          .setDescription("The number of all status job")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOB_CREATED =
      new Builder("Master.JobCreated")
          .setDescription("The number of created status job")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_FAILED =
      new Builder("Master.JobFailed")
          .setDescription("The number of failed status job")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_RUNNING =
      new Builder("Master.JobRunning")
          .setDescription("The number of running status job")
          .setMetricType(MetricType.COUNTER)
          .build();

  // Distributed command related metrics
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_SUCCESS =
      new Builder("Master.JobDistributedLoadSuccess")
          .setDescription("The number of successful DistributedLoad operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_FAIL =
      new Builder("Master.JobDistributedLoadFail")
          .setDescription("The number of failed DistributedLoad operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_CANCEL =
      new Builder("Master.JobDistributedLoadCancel")
          .setDescription("The number of cancelled DistributedLoad operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_FILE_COUNT =
      new Builder("Master.JobDistributedLoadFileCount")
          .setDescription("The number of files by DistributedLoad operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_FILE_SIZE =
      new Builder("Master.JobDistributedLoadFileSizes")
          .setDescription("The total file size by DistributedLoad operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOB_DISTRIBUTED_LOAD_RATE =
      new Builder("Master.JobDistributedLoadRate")
          .setDescription("The average DistributedLoad loading rate")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey MASTER_MIGRATE_JOB_SUCCESS =
      new Builder("Master.MigrateJobSuccess")
          .setDescription("The number of successful MigrateJob operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_MIGRATE_JOB_FAIL =
      new Builder("Master.MigrateJobFail")
          .setDescription("The number of failed MigrateJob operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_MIGRATE_JOB_CANCEL =
      new Builder("Master.MigrateJobCancel")
          .setDescription("The number of cancelled MigrateJob operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_MIGRATE_JOB_FILE_COUNT =
      new Builder("Master.MigrateJobFileCount")
          .setDescription("The number of MigrateJob files")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_MIGRATE_JOB_FILE_SIZE =
      new Builder("Master.MigrateJobFileSize")
          .setDescription("The total size of MigrateJob files")
          .setMetricType(MetricType.COUNTER)
          .build();

  public static final MetricKey MASTER_ASYNC_PERSIST_SUCCESS =
      new Builder("Master.AsyncPersistSuccess")
          .setDescription("The number of successful AsyncPersist operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ASYNC_PERSIST_FAIL =
      new Builder("Master.AsyncPersistFail")
          .setDescription("The number of failed AsyncPersist operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ASYNC_PERSIST_CANCEL =
      new Builder("Master.AsyncPersistCancel")
          .setDescription("The number of cancelled AsyncPersist operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ASYNC_PERSIST_FILE_COUNT =
      new Builder("Master.AsyncPersistFileCount")
          .setDescription("The number of files created by AsyncPersist operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ASYNC_PERSIST_FILE_SIZE =
      new Builder("Master.AsyncPersistFileSize")
          .setDescription("The total size of files created by AsyncPersist operations")
          .setMetricType(MetricType.COUNTER)
          .build();

  // Rocks Block metrics
  public static final MetricKey MASTER_ROCKS_BLOCK_BACKGROUND_ERRORS =
      new Builder("Master.RocksBlockBackgroundErrors")
          .setDescription("RocksDB block table. Accumulated number of background errors.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_BLOCK_CACHE_CAPACITY =
      new Builder("Master.RocksBlockBlockCacheCapacity")
          .setDescription("RocksDB block table. Block cache capacity.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_BLOCK_CACHE_PINNED_USAGE =
      new Builder("Master.RocksBlockBlockCachePinnedUsage")
          .setDescription("RocksDB block table. Memory size for the entries being pinned.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_BLOCK_CACHE_USAGE =
      new Builder("Master.RocksBlockBlockCacheUsage")
          .setDescription("RocksDB block table. Memory size for the entries residing in block "
              + "cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_COMPACTION_PENDING =
      new Builder("Master.RocksBlockCompactionPending")
          .setDescription("RocksDB block table. This metric 1 if at least one compaction "
              + "is pending; otherwise, the metric reports 0.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_CUR_SIZE_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksBlockCurSizeActiveMemTable")
          .setDescription("RocksDB block table. Approximate size of active memtable in bytes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_CUR_SIZE_ALL_MEM_TABLES =
      new Builder("Master.RocksBlockCurSizeAllMemTables")
          .setDescription("RocksDB block table. Approximate size of active, unflushed immutable, "
              + "and pinned immutable memtables in bytes. Pinned immutable memtables are flushed "
              + "memtables that are kept in memory to maintain write history in memory.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_ESTIMATE_NUM_KEYS =
      new Builder("Master.RocksBlockEstimateNumKeys")
          .setDescription("RocksDB block table. Estimated number of total keys in the active and "
              + "unflushed immutable memtables and storage.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_ESTIMATE_PENDING_COMPACTION_BYTES =
      new Builder("Master.RocksBlockEstimatePendingCompactionBytes")
          .setDescription("RocksDB block table. Estimated total number of bytes a compaction needs "
              + "to rewrite on disk to get all levels down to under target size. In other words, "
              + "this metrics relates to the write amplification in level compaction. "
              + "Thus, this metric is not valid for compactions other than level-based.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_ESTIMATE_TABLE_READERS_MEM =
      new Builder("Master.RocksBlockEstimateTableReadersMem")
          .setDescription("RocksDB inode table. Estimated memory in bytes used for reading SST "
              + "tables, excluding memory used in block cache (e.g., filter and index blocks). "
              + "This metric records the memory used by iterators as well as filters and indices "
              + "if the filters and indices are not maintained in the block cache. Basically this "
              + "metric reports the memory used outside the block cache to read data.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_LIVE_SST_FILES_SIZE =
      new Builder("Master.RocksBlockLiveSstFilesSize")
          .setDescription("RocksDB block table. Total size in bytes of all SST files that belong "
              + "to the latest LSM tree.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_MEM_TABLE_FLUSH_PENDING =
      new Builder("Master.RocksBlockMemTableFlushPending")
          .setDescription("RocksDB block table. This metric returns 1 if a memtable flush "
              + "is pending; otherwhise it returns 0.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_DELETES_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksBlockNumDeletesActiveMemTable")
          .setDescription("RocksDB block table. Total number of delete entries in the active "
              + "memtable.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_DELETES_IMM_MEM_TABLES =
      new Builder("Master.RocksBlockNumDeletesImmMemTables")
          .setDescription("RocksDB block table. Total number of delete entries in the unflushed "
              + "immutable memtables.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_ENTRIES_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksBlockNumEntriesActiveMemTable")
          .setDescription("RocksDB block table. Total number of entries in the active memtable.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_ENTRIES_IMM_MEM_TABLES =
      new Builder("Master.RocksBlockNumEntriesImmMemTables")
          .setDescription("RocksDB block table. Total number of entries in the unflushed "
              + "immutable memtables.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_IMMUTABLE_MEM_TABLE =
      new Builder("Master.RocksBlockNumImmutableMemTable")
          .setDescription("RocksDB block table. Number of immutable memtables that have not yet "
              + "been flushed.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_LIVE_VERSIONS =
      new Builder("Master.RocksBlockNumLiveVersions")
          .setDescription("RocksDB inode table. Number of live versions. More live versions often "
              + "mean more SST files are held from being deleted, by iterators or unfinished "
              + "compactions.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_RUNNING_COMPACTIONS =
      new Builder("Master.RocksBlockNumRunningCompactions")
          .setDescription("RocksDB block table. Number of currently running compactions.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_NUM_RUNNING_FLUSHES =
      new Builder("Master.RocksBlockNumRunningFlushes")
          .setDescription("RocksDB block table. Number of currently running flushes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_SIZE_ALL_MEM_TABLES =
      new Builder("Master.RocksBlockSizeAllMemTables")
          .setDescription("RocksDB block table. Size all mem tables.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_TOTAL_SST_FILES_SIZE =
      new Builder("Master.RocksBlockTotalSstFilesSize")
          .setDescription("RocksDB block table. Total size in bytes of all SST files.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_BLOCK_ESTIMATED_MEM_USAGE =
      new Builder("Master.RocksBlockEstimatedMemUsage")
          .setDescription("RocksDB block table. This metric estimates the memory usage of the "
              + "RockDB Block table by aggregating the values of "
              + "Master.RocksBlockBlockCacheUsage, Master.RocksBlockEstimateTableReadersMem, "
              + "Master.RocksBlockCurSizeAllMemTables, and Master.RocksBlockBlockCachePinnedUsage")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Rocks Inode metrics
  public static final MetricKey MASTER_ROCKS_INODE_BACKGROUND_ERRORS =
      new Builder("Master.RocksInodeBackgroundErrors")
          .setDescription("RocksDB inode table. Accumulated number of background errors.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_BLOCK_CACHE_CAPACITY =
      new Builder("Master.RocksInodeBlockCacheCapacity")
          .setDescription("RocksDB inode table. Block cache capacity.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_BLOCK_CACHE_PINNED_USAGE =
      new Builder("Master.RocksInodeBlockCachePinnedUsage")
          .setDescription("RocksDB inode table. Memory size for the entries being pinned.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_BLOCK_CACHE_USAGE =
      new Builder("Master.RocksInodeBlockCacheUsage")
          .setDescription("RocksDB inode table. Memory size for the entries residing in block "
              + "cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_COMPACTION_PENDING =
      new Builder("Master.RocksInodeCompactionPending")
          .setDescription("RocksDB inode table. This metric 1 if at least one compaction is "
              + "pending; otherwise, the metric reports 0.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_CUR_SIZE_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksInodeCurSizeActiveMemTable")
          .setDescription("RocksDB inode table. Approximate size of active memtable in bytes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_CUR_SIZE_ALL_MEM_TABLES =
      new Builder("Master.RocksInodeCurSizeAllMemTables")
          .setDescription("RocksDB inode table. Approximate size of active and unflushed "
              + "immutable memtable in bytes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_ESTIMATE_NUM_KEYS =
      new Builder("Master.RocksInodeEstimateNumKeys")
          .setDescription("RocksDB inode table. Estimated number of total keys in the active and "
              + "unflushed immutable memtables and storage.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_ESTIMATE_PENDING_COMPACTION_BYTES =
      new Builder("Master.RocksInodeEstimatePendingCompactionBytes")
          .setDescription("RocksDB block table. Estimated total number of bytes a compaction needs "
              + "to rewrite on disk to get all levels down to under target size. In other words, "
              + "this metrics relates to the write amplification in level compaction. "
              + "Thus, this metric is not valid for compactions other than level-based.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_ESTIMATE_TABLE_READERS_MEM =
      new Builder("Master.RocksInodeEstimateTableReadersMem")
          .setDescription("RocksDB inode table. Estimated memory in bytes used for reading SST "
              + "tables, excluding memory used in block cache (e.g., filter and index blocks). "
              + "This metric records the memory used by iterators as well as filters and indices "
              + "if the filters and indices are not maintained in the block cache. Basically this "
              + "metric reports the memory used outside the block cache to read data.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_LIVE_SST_FILES_SIZE =
      new Builder("Master.RocksInodeLiveSstFilesSize")
          .setDescription("RocksDB inode table. Total size in bytes of all SST files that belong "
              + "to the latest LSM tree.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_MEM_TABLE_FLUSH_PENDING =
      new Builder("Master.RocksInodeMemTableFlushPending")
          .setDescription("RocksDB inode table. This metric returns 1 if a memtable flush "
              + "is pending; otherwhise it returns 0.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_DELETES_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksInodeNumDeletesActiveMemTable")
          .setDescription("RocksDB inode table. Total number of delete entries in the active "
              + "memtable.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_DELETES_IMM_MEM_TABLES =
      new Builder("Master.RocksInodeNumDeletesImmMemTables")
          .setDescription("RocksDB inode table. Total number of delete entries in the unflushed "
              + "immutable memtables.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_ENTRIES_ACTIVE_MEM_TABLE =
      new Builder("Master.RocksInodeNumEntriesActiveMemTable")
          .setDescription("RocksDB inode table. Total number of entries in the active memtable.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_ENTRIES_IMM_MEM_TABLES =
      new Builder("Master.RocksInodeNumEntriesImmMemTables")
          .setDescription("RocksDB inode table. Total number of entries in the unflushed "
              + "immutable memtables.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_IMMUTABLE_MEM_TABLE =
      new Builder("Master.RocksInodeNumImmutableMemTable")
          .setDescription("RocksDB inode table. Number of immutable memtables that have not yet "
              + "been flushed.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_LIVE_VERSIONS =
      new Builder("Master.RocksInodeNumLiveVersions")
          .setDescription("RocksDB inode table. Number of live versions. More live versions often "
              + "mean more SST files are held from being deleted, by iterators or unfinished "
              + "compactions.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_RUNNING_COMPACTIONS =
      new Builder("Master.RocksInodeNumRunningCompactions")
          .setDescription("RocksDB inode table. Number of currently running compactions.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_NUM_RUNNING_FLUSHES =
      new Builder("Master.RocksInodeNumRunningFlushes")
          .setDescription("RocksDB inode table. Number of currently running flushes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_SIZE_ALL_MEM_TABLES =
      new Builder("Master.RocksInodeSizeAllMemTables")
          .setDescription("RocksDB inode table. Approximate size of active, unflushed immutable, "
              + "and pinned immutable memtables in bytes. Pinned immutable memtables are flushed "
              + "memtables that are kept in memory to maintain write history in memory.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_TOTAL_SST_FILES_SIZE =
      new Builder("Master.RocksInodeTotalSstFilesSize")
          .setDescription("RocksDB inode table. Total size in bytes of all SST files.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_INODE_ESTIMATED_MEM_USAGE =
      new Builder("Master.RocksInodeEstimatedMemUsage")
          .setDescription("RocksDB block table. This metric estimates the memory usage of the "
              + "RockDB Inode table by aggregating the values of "
              + "Master.RocksInodeBlockCacheUsage, Master.RocksInodeEstimateTableReadersMem, "
              + "Master.RocksInodeCurSizeAllMemTables, and Master.RocksInodeBlockCachePinnedUsage")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_ROCKS_TOTAL_ESTIMATED_MEM_USAGE =
      new Builder("Master.RocksTotalEstimatedMemUsage")
          .setDescription("This metric gives an estimate of the total memory used by RocksDB by "
              + "aggregating the values of Master.RocksBlockEstimatedMemUsage and "
              + "Master.RocksInodeEstimatedMemUsage")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Cluster metrics
  public static final MetricKey CLUSTER_ACTIVE_RPC_READ_COUNT =
      new Builder("Cluster.ActiveRpcReadCount")
          .setDescription("The number of active read-RPCs managed by workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_ACTIVE_RPC_WRITE_COUNT =
      new Builder("Cluster.ActiveRpcWriteCount")
          .setDescription("The number of active write-RPCs managed by workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DIRECT =
      new Builder("Cluster.BytesReadDirect")
          .setDescription("Total number of bytes read from all workers "
              + "without external RPC involved. Data exists in worker storage "
              + "or is fetched by workers from UFSes. This records data read "
              + "by worker internal calls (e.g. clients embedded in workers).")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DIRECT_THROUGHPUT =
      new Builder("Cluster.BytesReadDirectThroughput")
          .setDescription("Total number of bytes read from all workers "
              + "without external RPC involved. Data exists in worker storage "
              + "or is fetched by workers from UFSes. This records data read "
              + "by worker internal calls (e.g. clients embedded in workers).")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE =
      new Builder("Cluster.BytesReadRemote")
          .setDescription("Total number of bytes read from all workers via network (RPC). "
              + "Data exists in worker storage or is fetched by workers from UFSes. "
              + "This does not include short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder("Cluster.BytesReadRemoteThroughput")
          .setDescription("Bytes read per minute throughput from all workers "
              + "via network (RPC calls). Data exists in worker storage "
              + "or is fetched by workers from UFSes. This does not include "
              + "short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN =
      new Builder("Cluster.BytesReadDomain")
          .setDescription("Total number of bytes read from all works "
              + "via domain socket")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder("Cluster.BytesReadDomainThroughput")
          .setDescription("Bytes read per minute throughput from all workers "
              + "via domain socket")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL =
      new Builder("Cluster.BytesReadLocal")
          .setDescription("Total number of bytes short-circuit read from local worker data storage "
              + "by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL_THROUGHPUT =
      new Builder("Cluster.BytesReadLocalThroughput")
          .setDescription("Bytes per minute throughput "
              + "short-circuit read from local worker data storage by all clients")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS =
      new Builder("Cluster.BytesReadPerUfs")
          .setDescription("Total number of bytes read from a specific UFS by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_ALL =
      new Builder("Cluster.BytesReadUfsAll")
          .setDescription("Total number of bytes read from all Alluxio UFSes by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_THROUGHPUT =
      new Builder("Cluster.BytesReadUfsThroughput")
          .setDescription("Bytes read per minute throughput from all Alluxio UFSes by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE =
      new Builder("Cluster.BytesWrittenRemote")
          .setDescription("Total number of bytes written to workers via network (RPC). "
              + "Data is written to worker storage or is written by workers to underlying UFSes. "
              + "This does not include short-circuit local writes and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder("Cluster.BytesWrittenRemoteThroughput")
          .setDescription("Bytes write per minute throughput to workers via network (RPC). "
              + "Data is written to worker storage or is written by workers to underlying UFSes. "
              + "This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN =
      new Builder("Cluster.BytesWrittenDomain")
          .setDescription("Total number of bytes written to all workers "
              + "via domain socket")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder("Cluster.BytesWrittenDomainThroughput")
          .setDescription("Throughput of bytes written per minute to all workers "
              + "via domain socket")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL =
      new Builder("Cluster.BytesWrittenLocal")
          .setDescription("Total number of bytes short-circuit written to "
              + "local worker data storage by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT =
      new Builder("Cluster.BytesWrittenLocalThroughput")
          .setDescription("Bytes per minute throughput written to "
              + "local worker data storage by all clients")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS =
      new Builder("Cluster.BytesWrittenPerUfs")
          .setDescription("Total number of bytes written to a specific Alluxio UFS by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS_ALL =
      new Builder("Cluster.BytesWrittenUfsAll")
          .setDescription("Total number of bytes written to all Alluxio UFSes by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT =
      new Builder("Cluster.BytesWrittenUfsThroughput")
          .setDescription("Bytes write per minute throughput to all Alluxio UFSes by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CACHE_HIT_RATE =
      new Builder("Cluster.CacheHitRate")
          .setDescription("Cache hit rate: (# bytes read from cache) / (# bytes requested)")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_TOTAL =
      new Builder("Cluster.CapacityTotal")
          .setDescription("Total capacity (in bytes) on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_USED =
      new Builder("Cluster.CapacityUsed")
          .setDescription("Total used bytes on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_FREE =
      new Builder("Cluster.CapacityFree")
          .setDescription("Total free bytes on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_TOTAL =
      new Builder("Cluster.RootUfsCapacityTotal")
          .setDescription("Total capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_USED =
      new Builder("Cluster.RootUfsCapacityUsed")
          .setDescription("Used capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_FREE =
      new Builder("Cluster.RootUfsCapacityFree")
          .setDescription("Free capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_WORKERS =
      new Builder("Cluster.Workers")
          .setDescription("Total number of active workers inside the cluster")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_LOST_WORKERS =
      new Builder("Cluster.LostWorkers")
          .setDescription("Total number of lost workers inside the cluster")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_LEADER_INDEX =
      new Builder("Cluster.LeaderIndex")
          .setDescription("Index of current leader")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_LEADER_ID =
      new Builder("Cluster.LeaderId")
          .setDescription("Display current leader id")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Server metrics shared by Master, Worker and other Alluxio servers
  public static final MetricKey TOTAL_EXTRA_TIME =
      new Builder(Name.TOTAL_EXTRA_TIME)
          .setDescription("The total time that JVM slept and didn't do GC")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey INFO_TIME_EXCEEDED =
      new Builder(Name.INFO_TIME_EXCEEDED)
          .setDescription(String.format("The total number of times that JVM slept and the sleep"
                  + " period is larger than the info level threshold defined by %s",
              PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS.getName()))
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WARN_TIME_EXCEEDED =
      new Builder(Name.WARN_TIME_EXCEEDED)
          .setDescription(String.format("The total number of times that JVM slept and the sleep"
                  + " period is larger than the warn level threshold defined by %s",
              PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS.getName()))
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Worker metrics
  public static final MetricKey WORKER_ACTIVE_RPC_READ_COUNT =
      new Builder("Worker.ActiveRpcReadCount")
          .setDescription("The number of active read-RPCs managed by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_ACTIVE_RPC_WRITE_COUNT =
      new Builder("Worker.ActiveRpcWriteCount")
          .setDescription("The number of active write-RPCs managed by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_ACTIVE_CLIENTS =
      new Builder("Worker.ActiveClients")
          .setDescription("The number of clients actively reading from or writing to this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BLOCKS_ACCESSED =
      new Builder("Worker.BlocksAccessed")
          .setDescription("Total number of times any one of the blocks in this worker is accessed.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_CACHED =
      new Builder("Worker.BlocksCached")
          .setDescription("Total number of blocks used for caching data in an Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_CANCELLED =
      new Builder("Worker.BlocksCancelled")
          .setDescription("Total number of aborted temporary blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_DELETED =
      new Builder("Worker.BlocksDeleted")
          .setDescription("Total number of deleted blocks in this worker by external requests.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_EVICTED =
      new Builder("Worker.BlocksEvicted")
          .setDescription("Total number of evicted blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_EVICTION_RATE =
      new Builder("Worker.BlocksEvictionRate")
          .setDescription("Block eviction rate in this worker.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BLOCKS_LOST =
      new Builder("Worker.BlocksLost")
          .setDescription("Total number of lost blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_PROMOTED =
      new Builder("Worker.BlocksPromoted")
          .setDescription("Total number of times any one of the blocks in this worker "
              + "moved to a new tier.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_READ_LOCAL =
      new Builder("Worker.BlocksReadLocal")
          .setDescription("Total number of local blocks read by this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_READ_REMOTE =
      new Builder("Worker.BlocksReadRemote")
          .setDescription("Total number of a remote blocks read by this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_READ_UFS =
      new Builder("Worker.BlocksReadUfs")
          .setDescription("Total number of a UFS blocks read by this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DIRECT =
      new Builder("Worker.BytesReadDirect")
          .setDescription("Total number of bytes read from the this worker "
              + "without external RPC involved. Data exists in worker storage "
              + "or is fetched by this worker from underlying UFSes. "
              + "This records data read by worker internal calls "
              + "(e.g. a client embedded in this worker).")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DIRECT_THROUGHPUT =
      new Builder("Worker.BytesReadDirectThroughput")
          .setDescription("Throughput of bytes read from the this worker "
              + "without external RPC involved. Data exists in worker storage "
              + "or is fetched by this worker from underlying UFSes. "
              + "This records data read by worker internal calls "
              + "(e.g. a client embedded in this worker).")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE =
      new Builder("Worker.BytesReadRemote")
          .setDescription("Total number of bytes read from the this worker via network (RPC). "
              + "Data exists in worker storage or is fetched by this worker from underlying UFSes. "
              + "This does not include short-circuit local reads and domain socket reads.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder("Worker.BytesReadRemoteThroughput")
          .setDescription("Throughput of bytes read from the this worker via network (RPC). "
              + "Data exists in worker storage or is fetched by this worker from underlying UFSes. "
              + "This does not include short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN =
      new Builder("Worker.BytesReadDomain")
          .setDescription("Total number of bytes read from the current worker via domain socket")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder("Worker.BytesReadDomainThroughput")
          .setDescription("Bytes read throughput from the current worker "
              + "via domain socket")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS =
      new Builder("Worker.BytesReadPerUfs")
          .setDescription("Total number of bytes read from a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS_THROUGHPUT =
      new Builder("Worker.BytesReadUfsThroughput")
          .setDescription("Bytes read throughput from all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DIRECT =
      new Builder("Worker.BytesWrittenDirect")
          .setDescription("Total number of bytes written to this worker "
              + "without external RPC involved. Data is written to worker storage "
              + "or is written by this worker to underlying UFSes. "
              + "This records data written by worker internal calls "
              + "(e.g. a client embedded in this worker).")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DIRECT_THROUGHPUT =
      new Builder("Worker.BytesWrittenDirectThroughput")
          .setDescription("Total number of bytes written to this worker "
              + "without external RPC involved. Data is written to worker storage "
              + "or is written by this worker to underlying UFSes. This records data written "
              + "by worker internal calls (e.g. a client embedded in this worker).")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE =
      new Builder("Worker.BytesWrittenRemote")
          .setDescription("Total number of bytes written to this worker via network (RPC). "
              + "Data is written to worker storage or is written by this worker "
              + "to underlying UFSes. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder("Worker.BytesWrittenRemoteThroughput")
          .setDescription("Bytes write throughput to this worker via network (RPC). "
              + "Data is written to worker storage or is written by this worker "
              + "to underlying UFSes. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN =
      new Builder("Worker.BytesWrittenDomain")
          .setDescription("Total number of bytes written to this worker via domain socket")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder("Worker.BytesWrittenDomainThroughput")
          .setDescription("Throughput of bytes written to this worker "
              + "via domain socketr")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS =
      new Builder("Worker.BytesWrittenPerUfs")
          .setDescription("Total number of bytes written to a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS_THROUGHPUT =
      new Builder("Worker.BytesWrittenUfsThroughput")
          .setDescription("Bytes write throughput to all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_TOTAL =
      new Builder("Worker.CapacityTotal")
          .setDescription("Total capacity (in bytes) on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_USED =
      new Builder("Worker.CapacityUsed")
          .setDescription("Total used bytes on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_FREE =
      new Builder("Worker.CapacityFree")
          .setDescription("Total free bytes on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_FAILED_BLOCKS =
      new Builder("Worker.CacheFailedBlocks")
          .setDescription("Total number of failed cache blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_REMOTE_BLOCKS =
      new Builder("Worker.CacheRemoteBlocks")
          .setDescription("Total number of blocks that need to be cached from remote source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_REQUESTS =
      new Builder("Worker.CacheRequests")
          .setDescription("Total number of cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_REQUESTS_ASYNC =
      new Builder("Worker.CacheRequestsAsync")
          .setDescription("Total number of async cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_REQUESTS_SYNC =
      new Builder("Worker.CacheRequestsSync")
          .setDescription("Total number of sync cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_SUCCEEDED_BLOCKS =
      new Builder("Worker.CacheSucceededBlocks")
          .setDescription("Total number of cache succeeded blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_UFS_BLOCKS =
      new Builder("Worker.CacheUfsBlocks")
          .setDescription("Total number of blocks that need to be cached from local source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_BLOCKS_SIZE =
      new Builder("Worker.CacheBlocksSize")
          .setDescription("Total number of bytes that being cached through cache requests")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT =
      new Builder("Worker.BlockRemoverTryRemoveCount")
          .setDescription("The total number of blocks this worker attempted to remove "
              + "with asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVED_COUNT =
      new Builder("Worker.BlockRemoverBlocksRemovedCount")
          .setDescription("The total number of blocks successfully removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE =
      new Builder("Worker.BlockRemoverTryRemoveBlocksSize")
          .setDescription("The number of blocks to be removed from this worker at a moment "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE =
      new Builder("Worker.BlockRemoverRemovingBlocksSize")
          .setDescription("The size of blocks is being removed from this worker at a moment "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_MANAGER_THREAD_ACTIVE_COUNT =
      new Builder("Worker.CacheManagerThreadActiveCount")
          .setDescription("The approximate number of block cache "
              + "threads that are actively executing tasks in the cache manager thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_MANAGER_THREAD_CURRENT_COUNT =
      new Builder("Worker.CacheManagerThreadCurrentCount")
          .setDescription("The current number of cache threads in the cache manager thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_MANAGER_THREAD_QUEUE_WAITING_TASK_COUNT =
      new Builder("Worker.CacheManagerThreadQueueWaitingTaskCount")
          .setDescription("The current number of tasks waiting in the work queue "
              + "in the cache manager thread pool, bounded by "
              + PropertyKey.WORKER_NETWORK_ASYNC_CACHE_MANAGER_QUEUE_MAX)
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_MANAGER_THREAD_MAX_COUNT =
      new Builder("Worker.CacheManagerThreadMaxCount")
          .setDescription("The maximum allowed number of block cache "
              + "thread in the cache manager thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CACHE_MANAGER_COMPLETED_TASK_COUNT =
      new Builder("Worker.CacheManagerCompleteTaskCount")
          .setDescription("The approximate total number of block cache tasks "
              + "that have completed execution")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_READER_THREAD_ACTIVE_COUNT =
      new Builder("Worker.BlockReaderThreadActiveCount")
          .setDescription("The approximate number of block read "
              + "threads that are actively executing tasks in reader thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_READER_THREAD_CURRENT_COUNT =
      new Builder("Worker.BlockReaderThreadCurrentCount")
          .setDescription("The current number of read threads in the reader thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_READER_THREAD_MAX_COUNT =
      new Builder("Worker.BlockReaderThreadMaxCount")
          .setDescription("The maximum allowed number of block read "
              + "thread in the reader thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_READER_COMPLETED_TASK_COUNT =
      new Builder("Worker.BlockReaderCompleteTaskCount")
          .setDescription("The approximate total number of block read tasks "
              + "that have completed execution")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_WRITER_THREAD_ACTIVE_COUNT =
      new Builder("Worker.BlockWriterThreadActiveCount")
          .setDescription("The approximate number of block write "
              + "threads that are actively executing tasks in writer thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_WRITER_THREAD_CURRENT_COUNT =
      new Builder("Worker.BlockWriterThreadCurrentCount")
          .setDescription("The current number of write threads in the writer thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_WRITER_THREAD_MAX_COUNT =
      new Builder("Worker.BlockWriterThreadMaxCount")
          .setDescription("The maximum allowed number of block write "
              + "thread in the writer thread pool")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_WRITER_COMPLETED_TASK_COUNT =
      new Builder("Worker.BlockWriterCompleteTaskCount")
          .setDescription("The approximate total number of block write tasks "
              + "that have completed execution")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_RPC_QUEUE_LENGTH =
      new Builder("Worker.RpcQueueLength")
          .setDescription("Length of the worker rpc queue. "
              + "Use this metric to monitor the RPC pressure on worker.")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Client metrics
  public static final MetricKey CLIENT_BLOCK_READ_CHUNK_REMOTE =
      new Builder("Client.BlockReadChunkRemote")
          .setDescription(String.format("The timer statistics of reading block data in chunks "
              + "from remote Alluxio workers via RPC framework. "
              + "This metrics will only be recorded when %s is set to true",
              PropertyKey.USER_BLOCK_READ_METRICS_ENABLED.getName()))
          .setMetricType(MetricType.TIMER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BYTES_READ_LOCAL =
      new Builder("Client.BytesReadLocal")
          .setDescription("Total number of bytes short-circuit read from local storage "
              + "by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_BYTES_READ_LOCAL_THROUGHPUT =
      new Builder("Client.BytesReadLocalThroughput")
          .setDescription("Bytes throughput short-circuit read from local storage by this client")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_LOCAL =
      new Builder("Client.BytesWrittenLocal")
          .setDescription("Total number of bytes short-circuit written to local storage "
              + "by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT =
      new Builder("Client.BytesWrittenLocalThroughput")
          .setDescription("Bytes throughput short-circuit written to local storage by this client")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_UFS =
      new Builder("Client.BytesWrittenUfs")
          .setDescription("Total number of bytes write to Alluxio UFS by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  // Client cache metrics
  public static final MetricKey CLIENT_CACHE_BYTES_READ_CACHE =
      new Builder("Client.CacheBytesReadCache")
          .setDescription("Total number of bytes read from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_READ_IN_STREAM_BUFFER =
      new Builder("Client.CacheBytesReadInStreamBuffer")
          .setDescription("Total number of bytes read from the client cache's in stream buffer.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_READ_EXTERNAL =
      new Builder("Client.CacheBytesReadExternal")
          .setDescription("Total number of bytes read from external storage due to a cache miss "
              + "on the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL =
      new Builder("Client.CacheBytesRequestedExternal")
          .setDescription("Total number of bytes the user requested to read which resulted in a "
              + "cache miss. This number may be smaller than "
              + "Client.CacheBytesReadExternal" + " due to chunk reads.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGE_READ_CACHE_TIME_NS =
      new Builder("Client.CachePageReadCacheTimeNanos")
          .setDescription("Time in nanoseconds taken to read a page from the client cache "
              + "when the cache hits.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGE_READ_EXTERNAL_TIME_NS =
      new Builder("Client.CachePageReadExternalTimeNanos")
          .setDescription("Time in nanoseconds taken to read a page from external source "
              + "when the cache misses.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_DISCARDED =
      new Builder("Client.CacheBytesDiscarded")
          .setDescription("Total number of bytes discarded when restoring the page store.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_EVICTED =
      new Builder("Client.CacheBytesEvicted")
          .setDescription("Total number of bytes evicted from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGES =
      new Builder("Client.CachePages")
          .setDescription("Total number of pages in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGES_DISCARDED =
      new Builder("Client.CachePagesDiscarded")
          .setDescription("Total number of pages discarded when restoring the page store.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGES_EVICTED =
      new Builder("Client.CachePagesEvicted")
          .setDescription("Total number of pages evicted from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_WRITTEN_CACHE =
      new Builder("Client.CacheBytesWrittenCache")
          .setDescription("Total number of bytes written to the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_HIT_RATE =
      new Builder("Client.CacheHitRate")
          .setDescription("Cache hit rate: (# bytes read from cache) / (# bytes requested).")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_SPACE_AVAILABLE =
      new Builder("Client.CacheSpaceAvailable")
          .setDescription("Amount of bytes available in the client cache.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_SPACE_USED =
      new Builder("Client.CacheSpaceUsed")
          .setDescription("Amount of bytes used by the client cache.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_BYTES =
      new Builder("Client.CacheShadowCacheBytes")
          .setDescription("Amount of bytes in the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_BYTES_HIT =
      new Builder("Client.CacheShadowCacheBytesHit")
          .setDescription("Total number of bytes hit the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_BYTES_READ =
      new Builder("Client.CacheShadowCacheBytesRead")
          .setDescription("Total number of bytes read from the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_FALSE_POSITIVE_RATIO =
      new Builder("Client.CacheShadowCacheFalsePositiveRatio")
          .setDescription("Probability that the working set bloom filter makes an error. "
              + "The value is 0-100. If too high, need to allocate more space")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_PAGES =
      new Builder("Client.CacheShadowCachePages")
          .setDescription("Amount of pages in the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_PAGES_HIT =
      new Builder("Client.CacheShadowCachePagesHit")
          .setDescription("Total number of pages hit the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();
  public static final MetricKey CLIENT_CACHE_SHADOW_CACHE_PAGES_READ =
      new Builder("Client.CacheShadowCachePagesRead")
          .setDescription("Total number of pages read from the client shadow cache.")
          .setMetricType(MetricType.COUNTER).setIsClusterAggregated(false).build();

  // Counter versions of gauges, these may be removed in the future without notice
  public static final MetricKey CLIENT_CACHE_SPACE_USED_COUNT =
      new Builder("Client.CacheSpaceUsedCount")
          .setDescription("Amount of bytes used by the client cache as a counter.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_CLEAN_ERRORS =
      new Builder("Client.CacheCleanErrors")
          .setDescription("Number of failures when cleaning out the existing cache directory "
              + "to initialize a new cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();

  public static final MetricKey CLIENT_CACHE_CREATE_ERRORS =
      new Builder("Client.CacheCreateErrors")
          .setDescription("Number of failures when creating a cache in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_ERRORS =
      new Builder("Client.CacheDeleteErrors")
          .setDescription("Number of failures when deleting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS =
      new Builder("Client.CacheDeleteNonExistingPageErrors")
          .setDescription("Number of failures when deleting pages due to absence.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_NOT_READY_ERRORS =
      new Builder("Client.CacheDeleteNotReadyErrors")
          .setDescription("Number of failures when cache is not ready to delete pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_FROM_STORE_ERRORS =
      new Builder("Client.CacheDeleteFromStoreErrors")
          .setDescription("Number of failures when deleting pages from page "
              + "stores.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_ERRORS =
      new Builder("Client.CacheGetErrors")
          .setDescription("Number of failures when getting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_NOT_READY_ERRORS =
      new Builder("Client.CacheGetNotReadyErrors")
          .setDescription("Number of failures when cache is not ready to get pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_STORE_READ_ERRORS =
      new Builder("Client.CacheGetStoreReadErrors")
          .setDescription("Number of failures when getting cached data in the client cache due to "
              + "failed read from page stores.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_CLEANUP_GET_ERRORS =
      new Builder("Client.CacheCleanupGetErrors")
          .setDescription("Number of failures when cleaning up a failed cache read.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_CLEANUP_PUT_ERRORS =
      new Builder("Client.CacheCleanupPutErrors")
          .setDescription("Number of failures when cleaning up a failed cache write.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_ERRORS =
      new Builder("Client.CachePutErrors")
          .setDescription("Number of failures when putting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS =
      new Builder("Client.CachePutAsyncRejectionErrors")
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed injection to async write queue.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_EVICTION_ERRORS =
      new Builder("Client.CachePutEvictionErrors")
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed eviction.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS =
      new Builder("Client.CachePutInsufficientSpaceErrors")
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " insufficient space made after eviction.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS =
      new Builder("Client.CachePutBenignRacingErrors")
          .setDescription("Number of failures when adding pages due to racing eviction. This error"
              + " is benign.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_NOT_READY_ERRORS =
      new Builder("Client.CachePutNotReadyErrors")
          .setDescription("Number of failures when cache is not ready to add pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_STORE_DELETE_ERRORS =
      new Builder("Client.CachePutStoreDeleteErrors")
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed deletes in page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_STORE_WRITE_ERRORS =
      new Builder("Client.CachePutStoreWriteErrors")
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed writes to page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_STORE_WRITE_NO_SPACE_ERRORS =
      new Builder("Client.CachePutStoreWriteNoSpaceErrors")
          .setDescription("Number of failures when putting cached data in the client cache but"
              + " getting disk is full while cache capacity is not achieved. This can happen if"
              + " the storage overhead ratio to write data is underestimated.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_DELETE_TIMEOUT =
      new Builder("Client.CacheStoreDeleteTimeout")
          .setDescription("Number of timeouts when deleting pages from page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_GET_TIMEOUT =
      new Builder("Client.CacheStoreGetTimeout")
          .setDescription("Number of timeouts when reading pages from page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_PUT_TIMEOUT =
      new Builder("Client.CacheStorePutTimeout")
          .setDescription("Number of timeouts when writing new pages to page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_THREADS_REJECTED =
      new Builder("Client.CacheStoreThreadsRejected")
          .setDescription("Number of rejection of I/O threads on submitting tasks to thread pool, "
              + "likely due to unresponsive local file system.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STATE =
      new Builder("Client.CacheState")
          .setDescription("State of the cache: 0 (NOT_IN_USE), 1 (READ_ONLY) and 2 (READ_WRITE)")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_META_DATA_CACHE_SIZE =
      new Builder("Client.MetadataCacheSize")
          .setDescription("The total number of files and directories whose metadata is cached "
              + "on the client-side. Only valid if the filesystem is "
              + "alluxio.client.file.MetadataCachingBaseFileSystem.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_FILE_SYSTEM_MASTER_CLIENT_COUNT =
      new Builder("Client.FileSystemMasterClientCount")
          .setDescription("Number of instances in the FileSystemMasterClientPool.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BLOCK_MASTER_CLIENT_COUNT =
      new Builder("Client.BlockMasterClientCount")
          .setDescription("Number of instances in the BlockMasterClientPool.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BLOCK_WORKER_CLIENT_COUNT =
      new Builder("Client.BlockWorkerClientCount")
          .setDescription("Number of instances in the BlockWorkerClientPool.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_DEFAULT_HIVE_CLIENT_COUNT =
      new Builder("Client.DefaultHiveClientCount")
          .setDescription("Number of instances in the DefaultHiveClientPool.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();

  // Fuse operation timer and failure counter metrics are added dynamically.
  // Other Fuse related metrics are added here
  public static final MetricKey FUSE_TOTAL_CALLS =
      new Builder("Fuse.TotalCalls")
          .setDescription("Throughput of JNI FUSE operation calls. "
              + "This metrics indicates how busy the Alluxio Fuse application is serving requests")
          .setMetricType(MetricType.TIMER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey FUSE_READ_WRITE_FILE_COUNT =
      new Builder("Fuse.ReadWriteFileCount")
          .setDescription("Total number of files being opened for reading or writing concurrently.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey FUSE_CACHED_PATH_COUNT =
      new Builder("Fuse.CachedPathCount")
          .setDescription(String
              .format("Total number of FUSE-to-Alluxio path mappings being cached. "
                      + "This value will be smaller or equal to %s",
              PropertyKey.FUSE_CACHED_PATHS_MAX.getName()))
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  /**
   * A nested class to hold named string constants for their corresponding metrics.
   */
  @ThreadSafe
  public static final class Name {
    public static final String TOTAL_EXTRA_TIME = "Server.JvmPauseMonitorTotalExtraTime";
    public static final String INFO_TIME_EXCEEDED = "Server.JvmPauseMonitorInfoTimeExceeded";
    public static final String WARN_TIME_EXCEEDED = "Server.JvmPauseMonitorWarnTimeExceeded";
  }

  /**
   * Registers the given key to the global key map.
   *
   * @param key the Metric key
   * @return whether the Metric key is successfully registered
   */
  @VisibleForTesting
  public static boolean register(MetricKey key) {
    String name = key.getName();
    if (METRIC_KEYS_MAP.containsKey(name)) {
      return false;
    }

    METRIC_KEYS_MAP.put(name, key);
    return true;
  }

  /**
   * Unregisters the given key from the global key map.
   *
   * @param key the Metric to unregister
   */
  @VisibleForTesting
  public static void unregister(MetricKey key) {
    METRIC_KEYS_MAP.remove(key.getName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetricKey)) {
      return false;
    }
    MetricKey that = (MetricKey) o;
    return Objects.equal(mName, that.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName);
  }

  @Override
  public String toString() {
    return mName;
  }

  @Override
  public int compareTo(MetricKey o) {
    return mName.compareTo(o.mName);
  }
}
