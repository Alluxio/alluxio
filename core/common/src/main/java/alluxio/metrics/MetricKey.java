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
  // Master status
  public static final MetricKey MASTER_EDGE_CACHE_EVICTIONS =
      new Builder(Name.MASTER_EDGE_CACHE_EVICTIONS)
          .setDescription("Total number of edges (inode metadata) that was evicted from cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_HITS =
      new Builder(Name.MASTER_EDGE_CACHE_HITS)
          .setDescription("Total number of hits in the edge (inode metadata) cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_LOAD_TIMES =
      new Builder(Name.MASTER_EDGE_CACHE_LOAD_TIMES)
          .setDescription("Total load times in the edge (inode metadata) cache "
              + "that resulted from a cache miss. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_MISSES =
      new Builder(Name.MASTER_EDGE_CACHE_MISSES)
          .setDescription("Total number of misses in the edge (inode metadata) cache. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_CACHE_SIZE =
      new Builder(Name.MASTER_EDGE_CACHE_SIZE)
          .setDescription("Total number of edges (inode metadata) cached. "
              + "The edge cache is responsible for managing the mapping "
              + "from (parentId, childName) to childId.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_FILES_PINNED =
      new Builder(Name.MASTER_FILES_PINNED)
          .setDescription("Total number of currently pinned files")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_EVICTIONS =
      new Builder(Name.MASTER_INODE_CACHE_EVICTIONS)
          .setDescription("Total number of inodes that was evicted from the cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_HITS =
      new Builder(Name.MASTER_INODE_CACHE_HITS)
          .setDescription("Total number of hits in the inodes (inode metadata) cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_LOAD_TIMES =
      new Builder(Name.MASTER_INODE_CACHE_LOAD_TIMES)
          .setDescription("Total load times in the inodes (inode metadata) cache "
              + "that resulted from a cache miss.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_MISSES =
      new Builder(Name.MASTER_INODE_CACHE_MISSES)
          .setDescription("Total number of misses in the inodes (inode metadata) cache.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_INODE_CACHE_SIZE =
      new Builder(Name.MASTER_INODE_CACHE_SIZE)
          .setDescription("Total number of inodes (inode metadata) cached.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_TOTAL_PATHS =
      new Builder(Name.MASTER_TOTAL_PATHS)
          .setDescription("Total number of files and directory in Alluxio namespace")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Backup Restore
  public static final MetricKey MASTER_LAST_BACKUP_ENTRIES_COUNT =
      new Builder(Name.MASTER_LAST_BACKUP_ENTRIES_COUNT)
          .setDescription("The total number of entries written "
              + "in the last leading master metadata backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_TIME_MS =
      new Builder(Name.MASTER_LAST_BACKUP_TIME_MS)
          .setDescription("The process time of the last backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_RESTORE_COUNT =
      new Builder(Name.MASTER_LAST_BACKUP_RESTORE_COUNT)
          .setDescription("The total number of entries restored from backup "
              + "when a leading master initializes its metadata")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_LAST_BACKUP_RESTORE_TIME_MS =
      new Builder(Name.MASTER_LAST_BACKUP_RESTORE_TIME_MS)
          .setDescription("The process time of the last restore from backup")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Logical operations and results
  public static final MetricKey MASTER_DIRECTORIES_CREATED =
      new Builder(Name.MASTER_DIRECTORIES_CREATED)
          .setDescription("Total number of the succeed CreateDirectory operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILE_BLOCK_INFOS_GOT =
      new Builder(Name.MASTER_FILE_BLOCK_INFOS_GOT)
          .setDescription("Total number of succeed GetFileBlockInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILE_INFOS_GOT =
      new Builder(Name.MASTER_FILE_INFOS_GOT)
          .setDescription("Total number of the succeed GetFileInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_COMPLETED =
      new Builder(Name.MASTER_FILES_COMPLETED)
          .setDescription("Total number of the succeed CompleteFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_CREATED =
      new Builder(Name.MASTER_FILES_CREATED)
          .setDescription("Total number of the succeed CreateFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_FREED =
      new Builder(Name.MASTER_FILES_FREED)
          .setDescription("Total number of succeed FreeFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FILES_PERSISTED =
      new Builder(Name.MASTER_FILES_PERSISTED)
          .setDescription("Total number of successfully persisted files")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_NEW_BLOCKS_GOT =
      new Builder(Name.MASTER_NEW_BLOCKS_GOT)
          .setDescription("Total number of the succeed GetNewBlock operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_DELETED =
      new Builder(Name.MASTER_PATHS_DELETED)
          .setDescription("Total number of the succeed Delete operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_MOUNTED =
      new Builder(Name.MASTER_PATHS_MOUNTED)
          .setDescription("Total number of succeed Mount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_RENAMED =
      new Builder(Name.MASTER_PATHS_RENAMED)
          .setDescription("Total number of succeed Rename operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_PATHS_UNMOUNTED =
      new Builder(Name.MASTER_PATHS_UNMOUNTED)
          .setDescription("Total number of succeed Unmount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_COMPLETE_FILE_OPS =
      new Builder(Name.MASTER_COMPLETE_FILE_OPS)
          .setDescription("Total number of the CompleteFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_CREATE_DIRECTORIES_OPS =
      new Builder(Name.MASTER_CREATE_DIRECTORIES_OPS)
          .setDescription("Total number of the CreateDirectory operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_CREATE_FILES_OPS =
      new Builder(Name.MASTER_CREATE_FILES_OPS)
          .setDescription("Total number of the CreateFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_DELETE_PATHS_OPS =
      new Builder(Name.MASTER_DELETE_PATHS_OPS)
          .setDescription("Total number of the Delete operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_FREE_FILE_OPS =
      new Builder(Name.MASTER_FREE_FILE_OPS)
          .setDescription("Total number of FreeFile operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_FILE_BLOCK_INFO_OPS =
      new Builder(Name.MASTER_GET_FILE_BLOCK_INFO_OPS)
          .setDescription("Total number of GetFileBlockInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_FILE_INFO_OPS =
      new Builder(Name.MASTER_GET_FILE_INFO_OPS)
          .setDescription("Total number of the GetFileInfo operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_GET_NEW_BLOCK_OPS =
      new Builder(Name.MASTER_GET_NEW_BLOCK_OPS)
          .setDescription("Total number of the GetNewBlock operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_EVICTIONS =
      new Builder(Name.MASTER_LISTING_CACHE_EVICTIONS)
          .setDescription("The total number of evictions in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_HITS =
      new Builder(Name.MASTER_LISTING_CACHE_HITS)
          .setDescription("The total number of hits in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_LOAD_TIMES =
      new Builder(Name.MASTER_LISTING_CACHE_LOAD_TIMES)
          .setDescription("The total load time (in nanoseconds) in master listing cache "
              + "that resulted from a cache miss.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_MISSES =
      new Builder(Name.MASTER_LISTING_CACHE_MISSES)
          .setDescription("The total number of misses in master listing cache")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_LISTING_CACHE_SIZE =
      new Builder(Name.MASTER_LISTING_CACHE_SIZE)
          .setDescription("The size of master listing cache")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_MOUNT_OPS =
      new Builder(Name.MASTER_MOUNT_OPS)
          .setDescription("Total number of Mount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_RENAME_PATH_OPS =
      new Builder(Name.MASTER_RENAME_PATH_OPS)
          .setDescription("Total number of Rename operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_SET_ACL_OPS =
      new Builder(Name.MASTER_SET_ACL_OPS)
          .setDescription("Total number of SetAcl operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_SET_ATTRIBUTE_OPS =
      new Builder(Name.MASTER_SET_ATTRIBUTE_OPS)
          .setDescription("Total number of SetAttribute operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_UNMOUNT_OPS =
      new Builder(Name.MASTER_UNMOUNT_OPS)
          .setDescription("Total number of Unmount operations")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_INODE_LOCK_POOL_SIZE =
      new Builder(Name.MASTER_INODE_LOCK_POOL_SIZE)
          .setDescription("The size of master inode lock pool")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey MASTER_EDGE_LOCK_POOL_SIZE =
      new Builder(Name.MASTER_EDGE_LOCK_POOL_SIZE)
          .setDescription("The size of master edge lock pool")
          .setMetricType(MetricType.GAUGE)
          .build();
  // Journal metrics
  public static final MetricKey MASTER_JOURNAL_FLUSH_FAILURE =
      new Builder(Name.MASTER_JOURNAL_FLUSH_FAILURE)
          .setDescription("Total number of failed journal flush")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey MASTER_JOURNAL_FLUSH_TIMER =
      new Builder(Name.MASTER_JOURNAL_FLUSH_TIMER)
          .setDescription("The timer statistics of journal flush")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_JOURNAL_GAIN_PRIMACY_TIMER =
      new Builder(Name.MASTER_JOURNAL_GAIN_PRIMACY_TIMER)
          .setDescription("The timer statistics of journal gain primacy")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_CATCHUP_TIMER =
      new Builder(Name.MASTER_UFS_JOURNAL_CATCHUP_TIMER)
          .setDescription("The timer statistics of journal catchup")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER =
      new Builder(Name.MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER)
          .setDescription("The timer statistics of ufs journal failure recover")
          .setMetricType(MetricType.TIMER)
          .build();
  public static final MetricKey MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS =
      new Builder(Name.MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS)
          .setDescription("The process time of the ufs journal initial replay")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Cluster metrics
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE =
      new Builder(Name.CLUSTER_BYTES_READ_REMOTE)
          .setDescription("Total number of bytes read from Alluxio storage "
              + "or underlying UFS if data does not exist in Alluxio storage "
              + "reported by all workers. This does not include "
              + "short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_READ_REMOTE_THROUGHPUT)
          .setDescription("Bytes read per minute throughput from Alluxio storage "
              + "or underlying UFS if data does not exist in Alluxio storage "
              + "reported by all workers. This does not include "
              + "short-circuit local reads and domain socket reads")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN =
      new Builder(Name.CLUSTER_BYTES_READ_DOMAIN)
          .setDescription("Total number of bytes read from Alluxio storage "
              + "via domain socket reported by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT)
          .setDescription("Bytes read per minute throughput from Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL =
      new Builder(Name.CLUSTER_BYTES_READ_LOCAL)
          .setDescription("Total number of bytes short-circuit read from local storage "
              + "by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_LOCAL_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_READ_LOCAL_THROUGHPUT)
          .setDescription("Bytes per minute throughput "
              + "short-circuit read from local storage by all clients")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS =
      new Builder(Name.CLUSTER_BYTES_READ_UFS)
          .setDescription("Total number of bytes read from a specific UFS by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_ALL =
      new Builder(Name.CLUSTER_BYTES_READ_UFS_ALL)
          .setDescription("Total number of bytes read from a all Alluxio UFSes by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_READ_UFS_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_READ_UFS_THROUGHPUT)
          .setDescription("Bytes read per minute throughput from all Alluxio UFSes by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_REMOTE)
          .setDescription("Total number of bytes written to Alluxio storage in all workers "
              + "or the underlying UFS. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT)
          .setDescription("Bytes write per minute throughput to Alluxio storage in all workers "
              + "or the underlying UFS. This does not include short-circuit local writes "
              + "and domain socket writes.")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_DOMAIN)
          .setDescription("Total number of bytes written to Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT)
          .setDescription("Throughput of bytes written per minute to Alluxio storage "
              + "via domain socket by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_LOCAL)
          .setDescription("Total number of bytes short-circuit written to local storage "
              + "by all clients")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT)
          .setDescription("Bytes per minute throughput written to local storage by all clients")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes written to a specific Alluxio UFS by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS_ALL =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_UFS_ALL)
          .setDescription("Total number of bytes written to all Alluxio UFSes by all workers")
          .setMetricType(MetricType.COUNTER)
          .build();
  public static final MetricKey CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT =
      new Builder(Name.CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT)
          .setDescription("Bytes write per minute throughput to all Alluxio UFSes by all workers")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_TOTAL =
      new Builder(Name.CLUSTER_CAPACITY_TOTAL)
          .setDescription("Total capacity (in bytes) on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_USED =
      new Builder(Name.CLUSTER_CAPACITY_USED)
          .setDescription("Total used bytes on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_CAPACITY_FREE =
      new Builder(Name.CLUSTER_CAPACITY_FREE)
          .setDescription("Total free bytes on all tiers, on all workers of Alluxio")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_TOTAL =
      new Builder(Name.CLUSTER_ROOT_UFS_CAPACITY_TOTAL)
          .setDescription("Total capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_USED =
      new Builder(Name.CLUSTER_ROOT_UFS_CAPACITY_USED)
          .setDescription("Used capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_ROOT_UFS_CAPACITY_FREE =
      new Builder(Name.CLUSTER_ROOT_UFS_CAPACITY_FREE)
          .setDescription("Free capacity of the Alluxio root UFS in bytes")
          .setMetricType(MetricType.GAUGE)
          .build();
  public static final MetricKey CLUSTER_WORKERS =
      new Builder(Name.CLUSTER_WORKERS)
          .setDescription("Total number of active workers inside the cluster")
          .setMetricType(MetricType.GAUGE)
          .build();

  // Worker metrics
  public static final MetricKey WORKER_ASYNC_CACHE_DUPLICATE_REQUESTS =
      new Builder(Name.WORKER_ASYNC_CACHE_DUPLICATE_REQUESTS)
          .setDescription("Total number of duplicated async cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_FAILED_BLOCKS =
      new Builder(Name.WORKER_ASYNC_CACHE_FAILED_BLOCKS)
          .setDescription("Total number of async cache failed blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_REMOTE_BLOCKS =
      new Builder(Name.WORKER_ASYNC_CACHE_REMOTE_BLOCKS)
          .setDescription("Total number of blocks that need to be async cached from remote source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_REQUESTS =
      new Builder(Name.WORKER_ASYNC_CACHE_REQUESTS)
          .setDescription("Total number of async cache request received by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_SUCCEEDED_BLOCKS =
      new Builder(Name.WORKER_ASYNC_CACHE_SUCCEEDED_BLOCKS)
          .setDescription("Total number of async cache succeeded blocks in this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_ASYNC_CACHE_UFS_BLOCKS =
      new Builder(Name.WORKER_ASYNC_CACHE_UFS_BLOCKS)
          .setDescription("Total number of blocks that need to be async cached from local source")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_ACCESSED =
      new Builder(Name.WORKER_BLOCKS_ACCESSED)
          .setDescription("Total number of times any one of the blocks in this worker is accessed.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_CACHED =
      new Builder(Name.WORKER_BLOCKS_CACHED)
          .setDescription("Total number of blocks used for caching data in an Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_CANCELLED =
      new Builder(Name.WORKER_BLOCKS_CANCELLED)
          .setDescription("Total number of aborted temporary blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_DELETED =
      new Builder(Name.WORKER_BLOCKS_DELETED)
          .setDescription("Total number of deleted blocks in this worker by external requests.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_EVICTED =
      new Builder(Name.WORKER_BLOCKS_EVICTED)
          .setDescription("Total number of evicted blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_LOST =
      new Builder(Name.WORKER_BLOCKS_LOST)
          .setDescription("Total number of lost blocks in this worker.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCKS_PROMOTED =
      new Builder(Name.WORKER_BLOCKS_PROMOTED)
          .setDescription("Total number of times any one of the blocks in this worker "
              + "moved to a new tier.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE =
      new Builder(Name.WORKER_BYTES_READ_REMOTE)
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage. "
              + "This does not include short-circuit local reads and domain socket reads.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_REMOTE_THROUGHPUT =
      new Builder(Name.WORKER_BYTES_READ_REMOTE_THROUGHPUT)
          .setDescription("Total number of bytes read from Alluxio storage managed by this worker "
              + "and underlying UFS if data cannot be found in the Alluxio storage. "
              + "This does not include short-circuit local reads and domain socket reads.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN =
      new Builder(Name.WORKER_BYTES_READ_DOMAIN)
          .setDescription("Total number of bytes read from Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_DOMAIN_THROUGHPUT =
      new Builder(Name.WORKER_BYTES_READ_DOMAIN_THROUGHPUT)
          .setDescription("Bytes read throughput from Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS =
      new Builder(Name.WORKER_BYTES_READ_UFS)
          .setDescription("Total number of bytes read from a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_READ_UFS_THROUGHPUT =
      new MetricKey.Builder(Name.WORKER_BYTES_READ_UFS_THROUGHPUT)
          .setDescription("Bytes read throughput from all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE =
      new Builder(Name.WORKER_BYTES_WRITTEN_REMOTE)
          .setDescription("Total number of bytes written to Alluxio storage "
              + "or the underlying UFS by this worker. "
              + "This does not include short-circuit local writes and domain socket writes.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT =
      new Builder(Name.WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT)
          .setDescription("Bytes write throughput to Alluxio storage "
              + "or the underlying UFS by this worker"
              + "This does not include short-circuit local writes and domain socket writes.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN =
      new Builder(Name.WORKER_BYTES_WRITTEN_DOMAIN)
          .setDescription("Total number of bytes written to Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT =
      new Builder(Name.WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT)
          .setDescription("Throughput of bytes written to Alluxio storage "
              + "via domain socket by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS =
      new Builder(Name.WORKER_BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes written to a specific Alluxio UFS by this worker")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey WORKER_BYTES_WRITTEN_UFS_THROUGHPUT =
      new Builder(Name.WORKER_BYTES_WRITTEN_UFS_THROUGHPUT)
          .setDescription("Bytes write throughput to all Alluxio UFSes by this worker")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_TOTAL =
      new Builder(Name.WORKER_CAPACITY_TOTAL)
          .setDescription("Total capacity (in bytes) on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_USED =
      new Builder(Name.WORKER_CAPACITY_USED)
          .setDescription("Total used bytes on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_CAPACITY_FREE =
      new Builder(Name.WORKER_CAPACITY_FREE)
          .setDescription("Total free bytes on all tiers of a specific Alluxio worker")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT =
      new Builder(Name.WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT)
          .setDescription("The total number of blocks tried to be removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVED_COUNT =
      new Builder(Name.WORKER_BLOCK_REMOVER_REMOVED_COUNT)
          .setDescription("The total number of blocks removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE =
      new Builder(Name.WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE)
          .setDescription("The size of blocks to be removed from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE =
      new Builder(Name.WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE)
          .setDescription("The size of blocks is removing from this worker "
              + "by asynchronous block remover.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Client metrics
  public static final MetricKey CLIENT_BYTES_READ_LOCAL =
      new Builder(Name.CLIENT_BYTES_READ_LOCAL)
          .setDescription("Total number of bytes short-circuit read from local storage "
              + "by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_BYTES_READ_LOCAL_THROUGHPUT =
      new Builder(Name.CLIENT_BYTES_READ_LOCAL_THROUGHPUT)
          .setDescription("Bytes throughput short-circuit read from local storage by this client")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_LOCAL =
      new Builder(Name.CLIENT_BYTES_WRITTEN_LOCAL)
          .setDescription("Total number of bytes short-circuit written to local storage "
              + "by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT =
      new Builder(Name.CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT)
          .setDescription("Bytes throughput short-circuit written to local storage by this client")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_BYTES_WRITTEN_UFS =
      new Builder(Name.CLIENT_BYTES_WRITTEN_UFS)
          .setDescription("Total number of bytes write to Alluxio UFS by this client")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(true)
          .build();
  // Client cache metrics
  public static final MetricKey CLIENT_CACHE_BYTES_READ_CACHE =
      new Builder(Name.CLIENT_CACHE_BYTES_READ_CACHE)
          .setDescription("Total number of bytes read from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_READ_EXTERNAL =
      new Builder(Name.CLIENT_CACHE_BYTES_READ_EXTERNAL)
          .setDescription("Total number of bytes read from external storage due to a cache miss "
              + "on the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL =
      new Builder(Name.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL)
          .setDescription("Total number of bytes the user requested to read which resulted in a "
              + "cache miss. This number may be smaller than "
              + Name.CLIENT_CACHE_BYTES_READ_EXTERNAL + " due to chunk reads.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_EVICTED =
      new Builder(Name.CLIENT_CACHE_BYTES_EVICTED)
          .setDescription("Total number of bytes evicted from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGES =
      new Builder(Name.CLIENT_CACHE_PAGES)
          .setDescription("Total number of pages in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PAGES_EVICTED =
      new Builder(Name.CLIENT_CACHE_PAGES_EVICTED)
          .setDescription("Total number of pages evicted from the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_BYTES_WRITTEN_CACHE =
      new Builder(Name.CLIENT_CACHE_BYTES_WRITTEN_CACHE)
          .setDescription("Total number of bytes written to the client cache.")
          .setMetricType(MetricType.METER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_HIT_RATE =
      new Builder(Name.CLIENT_CACHE_HIT_RATE)
          .setDescription("Cache hit rate: (# bytes read from cache) / (# bytes requested).")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_SPACE_AVAILABLE =
      new Builder(Name.CLIENT_CACHE_SPACE_AVAILABLE)
          .setDescription("Amount of bytes available in the client cache.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_SPACE_USED =
      new Builder(Name.CLIENT_CACHE_SPACE_USED)
          .setDescription("Amount of bytes used by the client cache.")
          .setMetricType(MetricType.GAUGE)
          .setIsClusterAggregated(false)
          .build();

  // Counter versions of gauges, these may be removed in the future without notice
  public static final MetricKey CLIENT_CACHE_SPACE_USED_COUNT =
      new Builder(Name.CLIENT_CACHE_SPACE_USED_COUNT)
          .setDescription("Amount of bytes used by the client cache as a counter.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_UNREMOVABLE_FILES =
      new Builder(Name.CLIENT_CACHE_UNREMOVABLE_FILES)
          .setDescription("Amount of bytes unusable managed by the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();

  public static final MetricKey CLIENT_CACHE_CREATE_ERRORS =
      new Builder(Name.CLIENT_CACHE_CREATE_ERRORS)
          .setDescription("Number of failures when creating a cache in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_ERRORS =
      new Builder(Name.CLIENT_CACHE_DELETE_ERRORS)
          .setDescription("Number of failures when deleting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS =
      new Builder(Name.CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS)
          .setDescription("Number of failures when deleting pages due to absence.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_NOT_READY_ERRORS =
      new Builder(Name.CLIENT_CACHE_DELETE_NOT_READY_ERRORS)
          .setDescription("Number of failures when  when cache is not ready to delete pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS =
      new Builder(Name.CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS)
          .setDescription("Number of failures when deleting pages due to failed delete in page "
              + "stores.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_ERRORS =
      new Builder(Name.CLIENT_CACHE_GET_ERRORS)
          .setDescription("Number of failures when getting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_NOT_READY_ERRORS =
      new Builder(Name.CLIENT_CACHE_GET_NOT_READY_ERRORS)
          .setDescription("Number of failures when cache is not ready to get pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_GET_STORE_READ_ERRORS =
      new Builder(Name.CLIENT_CACHE_GET_STORE_READ_ERRORS)
          .setDescription("Number of failures when getting cached data in the client cache due to "
              + "failed read from page stores.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_CLEANUP_GET_ERRORS =
      new Builder(Name.CLIENT_CACHE_CLEANUP_GET_ERRORS)
          .setDescription("Number of failures when cleaning up a failed cache read.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_CLEANUP_PUT_ERRORS =
      new Builder(Name.CLIENT_CACHE_CLEANUP_PUT_ERRORS)
          .setDescription("Number of failures when cleaning up a failed cache write.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed injection to async write queue.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_EVICTION_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_EVICTION_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed eviction.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " insufficient space made after eviction.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS)
          .setDescription("Number of failures when adding pages due to racing eviction. This error"
              + " is benign.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_NOT_READY_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_NOT_READY_ERRORS)
          .setDescription("Number of failures when cache is not ready to add pages.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_STORE_DELETE_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_STORE_DELETE_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed deletes in page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_PUT_STORE_WRITE_ERRORS =
      new Builder(Name.CLIENT_CACHE_PUT_STORE_WRITE_ERRORS)
          .setDescription("Number of failures when putting cached data in the client cache due to"
              + " failed writes to page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_DELETE_TIMEOUT =
      new Builder(Name.CLIENT_CACHE_STORE_DELETE_TIMEOUT)
          .setDescription("Number of timeouts when deleting pages from page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_GET_TIMEOUT =
      new Builder(Name.CLIENT_CACHE_STORE_GET_TIMEOUT)
          .setDescription("Number of timeouts when reading pages from page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_PUT_TIMEOUT =
      new Builder(Name.CLIENT_CACHE_STORE_PUT_TIMEOUT)
          .setDescription("Number of timeouts when writing new pages to page store.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STORE_THREADS_REJECTED =
      new Builder(Name.CLIENT_CACHE_STORE_THREADS_REJECTED)
          .setDescription("Number of rejection of I/O threads on submitting tasks to thread pool, "
              + "likely due to unresponsive local file system.")
          .setMetricType(MetricType.COUNTER)
          .setIsClusterAggregated(false)
          .build();
  public static final MetricKey CLIENT_CACHE_STATE =
      new Builder(Name.CLIENT_CACHE_STATE)
          .setDescription("State of the cache: 0 (NOT_IN_USE), 1 (READ_ONLY) and 2 (READ_WRITE)")
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

  /**
   * A nested class to hold named string constants for their corresponding metrics.
   */
  @ThreadSafe
  public static final class Name {
    // Master metrics
    // metrics names for master status
    public static final String MASTER_EDGE_CACHE_EVICTIONS = "Master.EdgeCacheEvictions";
    public static final String MASTER_EDGE_CACHE_HITS = "Master.EdgeCacheHits";
    public static final String MASTER_EDGE_CACHE_LOAD_TIMES = "Master.EdgeCacheLoadTimes";
    public static final String MASTER_EDGE_CACHE_MISSES = "Master.EdgeCacheMisses";
    public static final String MASTER_EDGE_CACHE_SIZE = "Master.EdgeCacheSize";

    public static final String MASTER_FILES_PINNED = "Master.FilesPinned";

    public static final String MASTER_INODE_CACHE_EVICTIONS = "Master.InodeCacheEvictions";
    public static final String MASTER_INODE_CACHE_HITS = "Master.InodeCacheHits";
    public static final String MASTER_INODE_CACHE_LOAD_TIMES = "Master.InodeCacheLoadTimes";
    public static final String MASTER_INODE_CACHE_MISSES = "Master.InodeCacheMisses";
    public static final String MASTER_INODE_CACHE_SIZE = "Master.InodeCacheSize";

    public static final String MASTER_TOTAL_PATHS = "Master.TotalPaths";

    // metrics names for BackupManager
    public static final String MASTER_LAST_BACKUP_ENTRIES_COUNT = "Master.LastBackupEntriesCount";
    public static final String MASTER_LAST_BACKUP_RESTORE_COUNT = "Master.LastBackupRestoreCount";
    public static final String MASTER_LAST_BACKUP_TIME_MS
        = "Master.LastBackupTimeMs";
    public static final String MASTER_LAST_BACKUP_RESTORE_TIME_MS
        = "Master.LastBackupRestoreTimeMs";

    // metrics names for FileSystemMaster
    public static final String MASTER_DIRECTORIES_CREATED = "Master.DirectoriesCreated";
    public static final String MASTER_FILE_BLOCK_INFOS_GOT = "Master.FileBlockInfosGot";
    public static final String MASTER_FILE_INFOS_GOT = "Master.FileInfosGot";
    public static final String MASTER_FILES_COMPLETED = "Master.FilesCompleted";
    public static final String MASTER_FILES_CREATED = "Master.FilesCreated";
    public static final String MASTER_FILES_FREED = "Master.FilesFreed";
    public static final String MASTER_FILES_PERSISTED = "Master.FilesPersisted";
    public static final String MASTER_NEW_BLOCKS_GOT = "Master.NewBlocksGot";
    public static final String MASTER_PATHS_DELETED = "Master.PathsDeleted";
    public static final String MASTER_PATHS_MOUNTED = "Master.PathsMounted";
    public static final String MASTER_PATHS_RENAMED = "Master.PathsRenamed";
    public static final String MASTER_PATHS_UNMOUNTED = "Master.PathsUnmounted";
    public static final String MASTER_COMPLETE_FILE_OPS = "Master.CompleteFileOps";
    public static final String MASTER_CREATE_DIRECTORIES_OPS = "Master.CreateDirectoryOps";
    public static final String MASTER_CREATE_FILES_OPS = "Master.CreateFileOps";
    public static final String MASTER_DELETE_PATHS_OPS = "Master.DeletePathOps";
    public static final String MASTER_FREE_FILE_OPS = "Master.FreeFileOps";
    public static final String MASTER_GET_FILE_BLOCK_INFO_OPS = "Master.GetFileBlockInfoOps";
    public static final String MASTER_GET_FILE_INFO_OPS = "Master.GetFileInfoOps";
    public static final String MASTER_GET_NEW_BLOCK_OPS = "Master.GetNewBlockOps";
    public static final String MASTER_LISTING_CACHE_EVICTIONS = "Master.ListingCacheEvictions";
    public static final String MASTER_LISTING_CACHE_HITS = "Master.ListingCacheHits";
    public static final String MASTER_LISTING_CACHE_LOAD_TIMES = "Master.ListingCacheLoadTimes";
    public static final String MASTER_LISTING_CACHE_MISSES = "Master.ListingCacheMisses";
    public static final String MASTER_LISTING_CACHE_SIZE = "Master.ListingCacheSize";
    public static final String MASTER_MOUNT_OPS = "Master.MountOps";
    public static final String MASTER_RENAME_PATH_OPS = "Master.RenamePathOps";
    public static final String MASTER_SET_ACL_OPS = "Master.SetAclOps";
    public static final String MASTER_SET_ATTRIBUTE_OPS = "Master.SetAttributeOps";
    public static final String MASTER_UNMOUNT_OPS = "Master.UnmountOps";
    public static final String MASTER_INODE_LOCK_POOL_SIZE = "Master.InodeLockPoolSize";
    public static final String MASTER_EDGE_LOCK_POOL_SIZE = "Master.EdgeLockPoolSize";

    // metrics names for journal
    public static final String MASTER_JOURNAL_FLUSH_FAILURE = "Master.JournalFlushFailure";
    public static final String MASTER_JOURNAL_FLUSH_TIMER = "Master.JournalFlushTimer";
    public static final String MASTER_JOURNAL_GAIN_PRIMACY_TIMER = "Master.JournalGainPrimacyTimer";
    public static final String MASTER_UFS_JOURNAL_CATCHUP_TIMER
        = "Master.UfsJournalCatchupTimer";
    public static final String MASTER_UFS_JOURNAL_FAILURE_RECOVER_TIMER
        = "Master.UfsJournalFailureRecoverTimer";
    public static final String MASTER_UFS_JOURNAL_INITIAL_REPLAY_TIME_MS
        = "Master.UfsJournalInitialReplayTimeMs";

    // Cluster metrics
    public static final String CLUSTER_BYTES_READ_LOCAL = "Cluster.BytesReadLocal";
    public static final String CLUSTER_BYTES_READ_LOCAL_THROUGHPUT
        = "Cluster.BytesReadLocalThroughput";
    public static final String CLUSTER_BYTES_READ_REMOTE = "Cluster.BytesReadRemote";
    public static final String CLUSTER_BYTES_READ_REMOTE_THROUGHPUT
        = "Cluster.BytesReadRemoteThroughput";
    public static final String CLUSTER_BYTES_READ_DOMAIN = "Cluster.BytesReadDomain";
    public static final String CLUSTER_BYTES_READ_DOMAIN_THROUGHPUT
        = "Cluster.BytesReadDomainThroughput";
    public static final String CLUSTER_BYTES_READ_UFS = "Cluster.BytesReadPerUfs";
    public static final String CLUSTER_BYTES_READ_UFS_ALL = "Cluster.BytesReadUfsAll";
    public static final String CLUSTER_BYTES_READ_UFS_THROUGHPUT
        = "Cluster.BytesReadUfsThroughput";
    public static final String CLUSTER_BYTES_WRITTEN_REMOTE = "Cluster.BytesWrittenRemote";
    public static final String CLUSTER_BYTES_WRITTEN_REMOTE_THROUGHPUT
        = "Cluster.BytesWrittenRemoteThroughput";
    public static final String CLUSTER_BYTES_WRITTEN_DOMAIN = "Cluster.BytesWrittenDomain";
    public static final String CLUSTER_BYTES_WRITTEN_DOMAIN_THROUGHPUT
        = "Cluster.BytesWrittenDomainThroughput";
    public static final String CLUSTER_BYTES_WRITTEN_LOCAL = "Cluster.BytesWrittenLocal";
    public static final String CLUSTER_BYTES_WRITTEN_LOCAL_THROUGHPUT
        = "Cluster.BytesWrittenLocalThroughput";
    public static final String CLUSTER_BYTES_WRITTEN_UFS = "Cluster.BytesWrittenPerUfs";
    public static final String CLUSTER_BYTES_WRITTEN_UFS_ALL = "Cluster.BytesWrittenUfsAll";
    public static final String CLUSTER_BYTES_WRITTEN_UFS_THROUGHPUT
        = "Cluster.BytesWrittenUfsThroughput";
    public static final String CLUSTER_CAPACITY_TOTAL = "Cluster.CapacityTotal";
    public static final String CLUSTER_CAPACITY_USED = "Cluster.CapacityUsed";
    public static final String CLUSTER_CAPACITY_FREE = "Cluster.CapacityFree";
    public static final String CLUSTER_ROOT_UFS_CAPACITY_TOTAL = "Cluster.RootUfsCapacityTotal";
    public static final String CLUSTER_ROOT_UFS_CAPACITY_USED = "Cluster.RootUfsCapacityUsed";
    public static final String CLUSTER_ROOT_UFS_CAPACITY_FREE = "Cluster.RootUfsCapacityFree";
    public static final String CLUSTER_WORKERS = "Cluster.Workers";

    // Worker metrics
    public static final String WORKER_ASYNC_CACHE_DUPLICATE_REQUESTS
        = "Worker.AsyncCacheDuplicateRequests";
    public static final String WORKER_ASYNC_CACHE_FAILED_BLOCKS = "Worker.AsyncCacheFailedBlocks";
    public static final String WORKER_ASYNC_CACHE_REMOTE_BLOCKS = "Worker.AsyncCacheRemoteBlocks";
    public static final String WORKER_ASYNC_CACHE_REQUESTS = "Worker.AsyncCacheRequests";
    public static final String WORKER_ASYNC_CACHE_SUCCEEDED_BLOCKS
        = "Worker.AsyncCacheSucceededBlocks";
    public static final String WORKER_ASYNC_CACHE_UFS_BLOCKS = "Worker.AsyncCacheUfsBlocks";
    public static final String WORKER_BLOCKS_ACCESSED = "Worker.BlocksAccessed";
    public static final String WORKER_BLOCKS_CACHED = "Worker.BlocksCached";
    public static final String WORKER_BLOCKS_CANCELLED = "Worker.BlocksCancelled";
    public static final String WORKER_BLOCKS_DELETED = "Worker.BlocksDeleted";
    public static final String WORKER_BLOCKS_EVICTED = "Worker.BlocksEvicted";
    public static final String WORKER_BLOCKS_LOST = "Worker.BlocksLost";
    public static final String WORKER_BLOCKS_PROMOTED = "Worker.BlocksPromoted";
    public static final String WORKER_BYTES_READ_REMOTE = "Worker.BytesReadRemote";
    public static final String WORKER_BYTES_READ_REMOTE_THROUGHPUT
        = "Worker.BytesReadRemoteThroughput";
    public static final String WORKER_BYTES_WRITTEN_REMOTE = "Worker.BytesWrittenRemote";
    public static final String WORKER_BYTES_WRITTEN_REMOTE_THROUGHPUT
        = "Worker.BytesWrittenRemoteThroughput";
    public static final String WORKER_BYTES_READ_DOMAIN = "Worker.BytesReadDomain";
    public static final String WORKER_BYTES_READ_DOMAIN_THROUGHPUT
        = "Worker.BytesReadDomainThroughput";
    public static final String WORKER_BYTES_WRITTEN_DOMAIN = "Worker.BytesWrittenDomain";
    public static final String WORKER_BYTES_WRITTEN_DOMAIN_THROUGHPUT
        = "Worker.BytesWrittenDomainThroughput";

    public static final String WORKER_BYTES_READ_UFS = "Worker.BytesReadPerUfs";
    public static final String WORKER_BYTES_READ_UFS_THROUGHPUT = "Worker.BytesReadUfsThroughput";
    public static final String WORKER_BYTES_WRITTEN_UFS = "Worker.BytesWrittenPerUfs";
    public static final String WORKER_BYTES_WRITTEN_UFS_THROUGHPUT
        = "Worker.BytesWrittenUfsThroughput";
    public static final String WORKER_CAPACITY_TOTAL = "Worker.CapacityTotal";
    public static final String WORKER_CAPACITY_USED = "Worker.CapacityUsed";
    public static final String WORKER_CAPACITY_FREE = "Worker.CapacityFree";
    public static final String WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT
        = "Worker.BlockRemoverTryRemoveCount";
    public static final String WORKER_BLOCK_REMOVER_REMOVED_COUNT
        = "Worker.BlockRemoverBlocksToRemovedCount";
    public static final String WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE
        = "Worker.BlockRemoverTryRemoveBlocksSize";
    public static final String WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE
        = "Worker.BlockRemoverRemovingBlocksSize";

    // Client metrics
    public static final String CLIENT_BYTES_READ_LOCAL = "Client.BytesReadLocal";
    public static final String CLIENT_BYTES_READ_LOCAL_THROUGHPUT
        = "Client.BytesReadLocalThroughput";
    public static final String CLIENT_BYTES_WRITTEN_LOCAL = "Client.BytesWrittenLocal";
    public static final String CLIENT_BYTES_WRITTEN_LOCAL_THROUGHPUT
        = "Client.BytesWrittenLocalThroughput";
    public static final String CLIENT_BYTES_WRITTEN_UFS = "Client.BytesWrittenUfs";

    // Client local cache metrics
    public static final String CLIENT_CACHE_BYTES_READ_CACHE = "Client.CacheBytesReadCache";
    public static final String CLIENT_CACHE_BYTES_READ_EXTERNAL = "Client.CacheBytesReadExternal";
    public static final String CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL
        = "Client.CacheBytesRequestedExternal";
    public static final String CLIENT_CACHE_BYTES_EVICTED = "Client.CacheBytesEvicted";
    public static final String CLIENT_CACHE_PAGES = "Client.CachePages";
    public static final String CLIENT_CACHE_PAGES_EVICTED = "Client.CachePagesEvicted";
    public static final String CLIENT_CACHE_BYTES_WRITTEN_CACHE
        = "Client.CacheBytesWrittenCache";
    public static final String CLIENT_CACHE_HIT_RATE = "Client.CacheHitRate";
    public static final String CLIENT_CACHE_SPACE_AVAILABLE = "Client.CacheSpaceAvailable";
    public static final String CLIENT_CACHE_SPACE_USED = "Client.CacheSpaceUsed";
    public static final String CLIENT_CACHE_SPACE_USED_COUNT = "Client.CacheSpaceUsedCount";
    public static final String CLIENT_CACHE_CLEANUP_GET_ERRORS =
        "Client.CacheCleanupGetErrors";
    public static final String CLIENT_CACHE_CLEANUP_PUT_ERRORS =
        "Client.CacheCleanupPutErrors";
    public static final String CLIENT_CACHE_CREATE_ERRORS = "Client.CacheCreateErrors";
    public static final String CLIENT_CACHE_DELETE_ERRORS = "Client.CacheDeleteErrors";
    public static final String CLIENT_CACHE_DELETE_NON_EXISTING_PAGE_ERRORS =
        "Client.CacheDeleteNonExistingPageErrors";
    public static final String CLIENT_CACHE_DELETE_NOT_READY_ERRORS =
        "Client.CacheDeleteNotReadyErrors";
    public static final String CLIENT_CACHE_DELETE_STORE_DELETE_ERRORS =
        "Client.CacheDeleteStoreDeleteErrors";
    public static final String CLIENT_CACHE_GET_ERRORS = "Client.CacheGetErrors";
    public static final String CLIENT_CACHE_GET_NOT_READY_ERRORS = "Client.CacheGetNotReadyErrors";
    public static final String CLIENT_CACHE_GET_STORE_READ_ERRORS =
        "Client.CacheGetStoreReadErrors";
    public static final String CLIENT_CACHE_PUT_ERRORS = "Client.CachePutErrors";
    public static final String CLIENT_CACHE_PUT_ASYNC_REJECTION_ERRORS =
        "Client.CachePutAsyncRejectionErrors";
    public static final String CLIENT_CACHE_PUT_EVICTION_ERRORS =
        "Client.CachePutEvictionErrors";
    public static final String CLIENT_CACHE_PUT_BENIGN_RACING_ERRORS =
        "Client.CachePutBenignRacingErrors";
    public static final String CLIENT_CACHE_PUT_INSUFFICIENT_SPACE_ERRORS =
        "Client.CachePutInsufficientSpaceErrors";
    public static final String CLIENT_CACHE_PUT_NOT_READY_ERRORS =
        "Client.CachePutNotReadyErrors";
    public static final String CLIENT_CACHE_PUT_STORE_DELETE_ERRORS =
        "Client.CachePutStoreDeleteErrors";
    public static final String CLIENT_CACHE_PUT_STORE_WRITE_ERRORS =
        "Client.CachePutStoreWriteErrors";
    public static final String CLIENT_CACHE_STORE_DELETE_TIMEOUT =
        "Client.CacheStoreDeleteTimeout";
    public static final String CLIENT_CACHE_STORE_GET_TIMEOUT =
        "Client.CacheStoreGetTimeout";
    public static final String CLIENT_CACHE_STORE_PUT_TIMEOUT =
        "Client.CacheStorePutTimeout";
    public static final String CLIENT_CACHE_STORE_THREADS_REJECTED =
        "Client.CacheStoreThreadsRejected";
    public static final String CLIENT_CACHE_STATE = "Client.CacheState";
    public static final String CLIENT_CACHE_UNREMOVABLE_FILES = "Client.CacheUnremovableFiles";

    private Name() {} // prevent instantiation
  }
}
