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
import alluxio.exception.ExceptionMessage;
import alluxio.grpc.MetricType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    throw new IllegalArgumentException(ExceptionMessage.INVALID_METRIC_KEY.getMessage(name));
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

  // Master metrics
  // Absent cache stats
  public static final MetricKey MASTER_ABSENT_CACHE_HITS =
      new Builder("Master.AbsentCacheHits")
          .setDescription("Number of cache hits on the absent cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ABSENT_CACHE_MISSES =
      new Builder("Master.AbsentCacheMisses")
          .setDescription("Number of cache misses on the absent cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ABSENT_CACHE_INVALIDATIONS =
      new Builder("Master.AbsentCacheInvalidations")
          .setDescription("Number of invalidations on the absent cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_ABSENT_CACHE_SIZE =
      new Builder("Master.AbsentCacheSize")
          .setDescription("Size of the absent cache")
          .setMetricType(MetricType.GAUGE)
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
  public static final MetricKey MASTER_FILES_PINNED =
      new Builder("Master.FilesPinned")
          .setDescription("Total number of currently pinned files")
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
          .setDescription("Bytes left on the journal disk(s) for an Alluxio master")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_JOURNAL_SPACE_FREE_PERCENT =
      new Builder("Master.JournalFreePercent")
          .setDescription(
              "Percentage of free space left on the journal disk(s) for an Alluxio master")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_TOTAL_PATHS =
      new Builder("Master.TotalPaths")
          .setDescription("Total number of files and directory in Alluxio namespace")
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
  // Journal metrics
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_GENERATE_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotGenerateTimer")
          .setDescription("The timer statistics of journal snapshot generation by this master")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_DOWNLOAD_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotDownloadGenerate")
          .setDescription("The timer statistics of journal snapshot download from other masters")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_INSTALL_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotInstallTimer")
          .setDescription("The timer statistics of journal snapshot install")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_REPLAY_TIMER =
      new Builder("Master.EmbeddedJournalSnapshotReplayTimer")
          .setDescription("The timer statistics of journal snapshot replay")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_EMBEDDED_JOURNAL_SNAPSHOT_LAST_INDEX =
      new Builder("Master.EmbeddedJournalSnapshotLastIndex")
          .setDescription("The last index of the latest journal snapshot "
              + "created by this master or downloaded from other masters")
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
  public static final MetricKey MASTER_JOURNAL_GAIN_PRIMACY_TIMER =
      new Builder("Master.JournalGainPrimacyTimer")
          .setDescription("The timer statistics of journal gain primacy")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_CATCHUP_TIMER =
      new Builder("Master.UfsJournalCatchupTimer")
          .setDescription("The timer statistics of journal catchup")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER =
      new Builder("Master.UfsJournalFailureRecoverTimer")
          .setDescription("The timer statistics of ufs journal failure recover")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS =
      new Builder("Master.UfsJournalInitialReplayTimeMs")
          .setDescription("The process time of the ufs journal initial replay")
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
          .setDescription("Total number of bytes read from Alluxio storage managed by workers "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "without external RPC involved. This records data read "
              + "by worker internal calls (e.g. clients embedded in workers).")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DIRECT_THROUGHPUT =
      new Builder("Cluster.BytesReadDirectThroughput")
          .setDescription("Total number of bytes read from Alluxio storage managed by workers "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "without external RPC involved. This records data read "
              + "by worker internal calls (e.g. clients embedded in workers).")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE =
      new Builder("Cluster.BytesReadRemote")
          .setDescription("Total number of bytes read from Alluxio storage "
              + "or underlying UFS if data does not exist in Alluxio storage "
              + "reported by all workers. This does not include "
              + "short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder("Cluster.BytesReadRemoteThroughput")
          .setDescription("Bytes read per minute throughput from Alluxio storage "
              + "or underlying UFS if data does not exist in Alluxio storage "
              + "reported by all workers. This does not include "
              + "short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN =
      new Builder("Cluster.BytesReadDomain")
          .setDescription("Total number of bytes read from Alluxio storage "
              + "via domain socket reported by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder("Cluster.BytesReadDomainThroughput")
          .setDescription("Bytes read per minute throughput from Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL =
      new Builder("Cluster.BytesReadLocal")
          .setDescription("Total number of bytes short-circuit read from local storage "
              + "by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL_THROUGHPUT =
      new Builder("Cluster.BytesReadLocalThroughput")
          .setDescription("Bytes per minute throughput "
              + "short-circuit read from local storage by all clients")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS =
      new Builder("Cluster.BytesReadPerUfs")
          .setDescription("Total number of bytes read from a specific UFS by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_ALL =
      new Builder("Cluster.BytesReadUfsAll")
          .setDescription("Total number of bytes read from a all Alluxio UFSes by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_THROUGHPUT =
      new Builder("Cluster.BytesReadUfsThroughput")
          .setDescription("Bytes read per minute throughput from all Alluxio UFSes by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE =
      new Builder("Cluster.BytesWrittenRemote")
          .setDescription("Total number of bytes written to Alluxio storage in all workers "
              + "or the underlying UFS. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder("Cluster.BytesWrittenRemoteThroughput")
          .setDescription("Bytes write per minute throughput to Alluxio storage in all workers "
              + "or the underlying UFS. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN =
      new Builder("Cluster.BytesWrittenDomain")
          .setDescription("Total number of bytes written to Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder("Cluster.BytesWrittenDomainThroughput")
          .setDescription("Throughput of bytes written per minute to Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL =
      new Builder("Cluster.BytesWrittenLocal")
          .setDescription("Total number of bytes short-circuit written to local storage "
              + "by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT =
      new Builder("Cluster.BytesWrittenLocalThroughput")
          .setDescription("Bytes per minute throughput written to local storage by all clients")
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
  public static final MetricKey WORKER_ASYNC_CACHE_DUPLICATE_REQUESTS =
      new Builder("Worker.AsyncCacheDuplicateRequests")
          .setDescription("Total number of duplicated async cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_FAILED_BLOCKS =
      new Builder("Worker.AsyncCacheFailedBlocks")
          .setDescription("Total number of async cache failed blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_REMOTE_BLOCKS =
      new Builder("Worker.AsyncCacheRemoteBlocks")
          .setDescription("Total number of blocks that need to be async cached from remote source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_REQUESTS =
      new Builder("Worker.AsyncCacheRequests")
          .setDescription("Total number of async cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_SUCCEEDED_BLOCKS =
      new Builder("Worker.AsyncCacheSucceededBlocks")
          .setDescription("Total number of async cache succeeded blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_UFS_BLOCKS =
      new Builder("Worker.AsyncCacheUfsBlocks")
          .setDescription("Total number of blocks that need to be async cached from local source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
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
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "without external RPC involved. This records data read "
              + "by worker internal calls (e.g. a client embedded in this worker).")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DIRECT_THROUGHPUT =
      new Builder("Worker.BytesReadDirectThroughput")
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "without external RPC involved. This records data read "
              + "by worker internal calls (e.g. a client embedded in this worker).")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE =
      new Builder("Worker.BytesReadRemote")
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "via external RPC channel. This does not include "
              + "short-circuit local reads and domain socket reads.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder("Worker.BytesReadRemoteThroughput")
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage "
              + "via external RPC channel. This does not include "
              + "short-circuit local reads and domain socket reads.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN =
      new Builder("Worker.BytesReadDomain")
          .setDescription("Total number of bytes read from Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder("Worker.BytesReadDomainThroughput")
          .setDescription("Bytes read throughput from Alluxio storage "
              + "via domain socket by this worker")
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
      new MetricKey.Builder("Worker.BytesReadUfsThroughput")
          .setDescription("Bytes read throughput from all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DIRECT =
      new Builder("Worker.BytesWrittenDirect")
          .setDescription("Total number of bytes written to Alluxio storage managed by this worker "
              + "without external RPC involved. This records data written "
              + "by worker internal calls (e.g. a client embedded in this worker).")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DIRECT_THROUGHPUT =
      new Builder("Worker.BytesWrittenDirectThroughput")
          .setDescription("Total number of bytes written to Alluxio storage managed by this worker "
              + "without external RPC involved. This records data written "
              + "by worker internal calls (e.g. a client embedded in this worker).")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE =
      new Builder("Worker.BytesWrittenRemote")
          .setDescription("Total number of bytes written to Alluxio storage "
              + "or the underlying UFS by this worker. "
              + "This does not include short-circuit local writes and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder("Worker.BytesWrittenRemoteThroughput")
          .setDescription("Bytes write throughput to Alluxio storage "
              + "or the underlying UFS by this worker"
              + "This does not include short-circuit local writes and domain socket writes.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN =
      new Builder("Worker.BytesWrittenDomain")
          .setDescription("Total number of bytes written to Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder("Worker.BytesWrittenDomainThroughput")
          .setDescription("Throughput of bytes written to Alluxio storage "
              + "via domain socket by this worker")
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
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT =
      new Builder("Worker.BlockRemoverTryRemoveCount")
          .setDescription("The total number of blocks tried to be removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVED_COUNT =
      new Builder("Worker.BlockRemoverBlocksToRemovedCount")
          .setDescription("The total number of blocks removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE =
      new Builder("Worker.BlockRemoverTryRemoveBlocksSize")
          .setDescription("The size of blocks to be removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE =
      new Builder("Worker.BlockRemoverRemovingBlocksSize")
          .setDescription("The size of blocks is removing from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Client metrics
  public static final MetricKey CLIENT_BLOCK_READ_CHUNK =
      new Builder("Client.BlockReadDataChunk")
          .setDescription(String.format("The timer statistics of reading block data in chunks "
              + "from Alluxio workers. This metrics will only be recorded when %s is set to true",
              PropertyKey.USER_BLOCK_READ_METRICS_ENABLED.getName()))
          .setMetricType(MetricType.TIMER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BLOCK_READ_FROM_CHUNK =
      new Builder("Client.BlockReadDataFromChunk")
          .setDescription(String.format("The timer statistics of reading data from data chunks "
              + "which have already fetched from Alluxio workers. "
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

  // Counter versions of gauges, these may be removed in the future without notice
  public static final MetricKey CLIENT_CACHE_SPACE_USED_COUNT =
      new Builder("Client.CacheSpaceUsedCount")
          .setDescription("Amount of bytes used by the client cache as a counter.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_UNREMOVABLE_FILES =
      new Builder("Client.CacheUnremovableFiles")
          .setDescription("Amount of bytes unusable managed by the client cache.")
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
          .setDescription("Number of failures when  when cache is not ready to delete pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS =
      new Builder("Client.CacheDeleteStoreDeleteErrors")
          .setDescription("Number of failures when deleting pages due to failed delete in page "
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

  // Fuse operation timer and failure counter metrics are added dynamically.
  // Other Fuse related metrics are added here
  public static final MetricKey FUSE_BYTES_TO_READ =
      new Builder("Fuse.BytesToRead")
          .setDescription("Total number of bytes requested by Fuse.read() operations.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey FUSE_BYTES_READ =
      new Builder("Fuse.BytesRead")
          .setDescription("Total number of bytes read through Fuse.read() operations.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();

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
