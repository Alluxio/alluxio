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

import alluxio.metrics.MetricsSystem;

import com.google.common.base.MoreObjects;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The filesystem indicator object.
 */
public class FileSystemIndicator {
  // The observed counter names
  private static final List<String> OBSERVED_MASTER_COUNTER = new LinkedList<>();

  static {
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_COMPLETE_FILE_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_COMPLETED_OPERATION_RETRY_COUNT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_CREATE_FILE_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_CREATE_DIRECTORIES_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_DELETE_PATH_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_DIRECTORIES_CREATED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILE_BLOCK_INFOS_GOT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILE_INFOS_GOT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILES_COMPLETED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILES_CREATED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILES_FREED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FILES_PERSISTED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_FREE_FILE_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_GET_FILE_BLOCK_INFO_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_GET_FILE_INFO_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_GET_NEW_BLOCK_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_LISTING_CACHE_EVICTIONS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_LISTING_CACHE_HITS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_LISTING_CACHE_LOAD_TIMES);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_LISTING_CACHE_MISSES);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_ACTIVE_PATHS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_FAIL);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_NO_CHANGE);

    // This is the concerns
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_OPS_COUNT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PATHS_CANCEL);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PATHS_FAIL);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PATHS_SUCCESS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PENDING_PATHS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_CANCEL);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_FAIL);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_OPS_COUNT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_PATHS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_RETRIES);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_PREFETCH_SUCCESS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_SKIPPED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_SUCCESS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_METADATA_SYNC_TIME_MS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_MOUNT_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_NEW_BLOCKS_GOT);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_PATHS_DELETED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_PATHS_MOUNTED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_PATHS_RENAMED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_PATHS_UNMOUNTED);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_RENAME_PATH_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_SET_ACL_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_SET_ATTRIBUTE_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_UFS_STATUS_CACHE_CHILDREN_SIZE);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_UFS_STATUS_CACHE_SIZE);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_UNMOUNT_OPS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_GET_CONFIG_HASH_IN_PROGRESS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_GET_CONFIGURATION_IN_PROGRESS);
    OBSERVED_MASTER_COUNTER
        .add(MetricsMonitorUtils.FileSystemCounterName.MASTER_REGISTER_WORKER_START_IN_PROGRESS);
  }

  private Map<String, Long> mMasterIndicators;
  private long mPitTimeMS;

  /**
   * Filesystem indicator Constructor.
   */
  public FileSystemIndicator() {
    mMasterIndicators = new HashMap<>();
    for (String countName : OBSERVED_MASTER_COUNTER) {
      mMasterIndicators.put(countName, MetricsSystem.METRIC_REGISTRY
          .counter(countName).getCount());
    }
    mPitTimeMS = System.currentTimeMillis();
  }

  /**
   * Filesystem indicator constructor.
   *
   * @param fileSystemIndicator the filesystem indicator
   */
  public FileSystemIndicator(FileSystemIndicator fileSystemIndicator) {
    mMasterIndicators = new HashMap<>();
    mMasterIndicators.putAll(fileSystemIndicator.mMasterIndicators);
    mPitTimeMS = fileSystemIndicator.mPitTimeMS;
  }

  /**
   * @param name the counter name
   * @return the counter
   */
  public Optional<Long> getCounter(String name) {
    if (mMasterIndicators.containsKey(name)) {
      return Optional.of(mMasterIndicators.get(name));
    }
    return Optional.empty();
  }

  /**
   * @return the point in time
   */
  public long getPitTimeMS() {
    return mPitTimeMS;
  }

  /**
   * Sets the counter.
   *
   * @param name the counter name
   * @param value the counter value
   */
  public void setCounter(String name, long value) {
    mMasterIndicators.put(name, value);
  }

  /**
   * Sets the pit time.
   *
   * @param pitTimeMS the pit
   */
  public void setPitTimeMS(long pitTimeMS) {
    mPitTimeMS = pitTimeMS;
  }

  /**
   * Calculates the delta.
   *
   * @param baselineIndicators the baseline indicator object
   */
  public void deltaTo(FileSystemIndicator baselineIndicators) {
    for (String name : OBSERVED_MASTER_COUNTER) {
      mMasterIndicators.computeIfPresent(name,
          (key, value)
              -> { return value - baselineIndicators.mMasterIndicators.getOrDefault(key, 0L); });
    }
  }

  /**
   * @return the string of the indicators
   */
  public String toString() {
    MoreObjects.ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
    for (Map.Entry entry : mMasterIndicators.entrySet()) {
      toStringHelper.add((String) (entry.getKey()), entry.getValue());
    }
    return toStringHelper.toString();
  }
}

