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

package alluxio.master.block.meta;

import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.grpc.StorageList;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Metadata for an Alluxio worker. This class is not thread safe, so external locking is required.
 */
@NotThreadSafe
public final class MasterWorkerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWorkerInfo.class);
  private static final String LIVE_WORKER_STATE = "In Service";
  private static final String LOST_WORKER_STATE = "Out of Service";
  private static final int BLOCK_SIZE_LIMIT = 100;

  /** Worker's address. */
  private final WorkerNetAddress mWorkerAddress;
  /** The id of the worker. */
  private final long mId;
  /** Start time of the worker in ms. */
  private final long mStartTimeMs;
  /** Capacity of worker in bytes. */
  private long mCapacityBytes;
  /** Worker's used bytes. */
  private long mUsedBytes;
  /** Worker's last updated time in ms. */
  private long mLastUpdatedTimeMs;
  /** If true, the worker is considered registered. */
  private boolean mIsRegistered;
  /** Worker-specific mapping between storage tier alias and storage tier ordinal. */
  private StorageTierAssoc mStorageTierAssoc;
  /** Mapping from storage tier alias to total bytes. */
  private Map<String, Long> mTotalBytesOnTiers;
  /** Mapping from storage tier alias to used bytes. */
  private Map<String, Long> mUsedBytesOnTiers;

  /** ids of blocks the worker contains. */
  private Set<Long> mBlocks;
  /** ids of blocks the worker should remove. */
  private Set<Long> mToRemoveBlocks;
  /** Mapping from tier alias to lost storage paths. */
  private Map<String, List<String>> mLostStorage;

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, WorkerNetAddress address) {
    mWorkerAddress = Preconditions.checkNotNull(address, "address");
    mId = id;
    mStartTimeMs = System.currentTimeMillis();
    mLastUpdatedTimeMs = System.currentTimeMillis();
    mIsRegistered = false;
    mStorageTierAssoc = null;
    mTotalBytesOnTiers = new HashMap<>();
    mUsedBytesOnTiers = new HashMap<>();
    mBlocks = new HashSet<>();
    mToRemoveBlocks = new HashSet<>();
    mLostStorage = new HashMap<>();
  }

  /**
   * Marks the worker as registered, while updating all of its metadata.
   *
   * @param globalStorageTierAssoc global mapping between storage aliases and ordinal position
   * @param storageTierAliases list of storage tier aliases in order of their position in the
   *        hierarchy
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used byes
   * @param blocks set of block ids on this worker
   * @return A Set of blocks removed (or lost) from this worker
   */
  public Set<Long> register(final StorageTierAssoc globalStorageTierAssoc,
      final List<String> storageTierAliases, final Map<String, Long> totalBytesOnTiers,
      final Map<String, Long> usedBytesOnTiers, final Set<Long> blocks) {
    // If the storage aliases do not have strictly increasing ordinal value based on the total
    // ordering, throw an error
    for (int i = 0; i < storageTierAliases.size() - 1; i++) {
      if (globalStorageTierAssoc.getOrdinal(storageTierAliases.get(i)) >= globalStorageTierAssoc
          .getOrdinal(storageTierAliases.get(i + 1))) {
        throw new IllegalArgumentException(
            "Worker cannot place storage tier " + storageTierAliases.get(i) + " above "
                + storageTierAliases.get(i + 1) + " in the hierarchy");
      }
    }
    mStorageTierAssoc = new WorkerStorageTierAssoc(storageTierAliases);
    // validate the number of tiers
    if (mStorageTierAssoc.size() != totalBytesOnTiers.size()
        || mStorageTierAssoc.size() != usedBytesOnTiers.size()) {
      throw new IllegalArgumentException(
          "totalBytesOnTiers and usedBytesOnTiers should have the same number of tiers as "
              + "storageTierAliases, but storageTierAliases has " + mStorageTierAssoc.size()
              + " tiers, while totalBytesOnTiers has " + totalBytesOnTiers.size()
              + " tiers and usedBytesOnTiers has " + usedBytesOnTiers.size() + " tiers");
    }

    // defensive copy
    mTotalBytesOnTiers = new HashMap<>(totalBytesOnTiers);
    mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    mCapacityBytes = 0;
    for (long bytes : mTotalBytesOnTiers.values()) {
      mCapacityBytes += bytes;
    }
    mUsedBytes = 0;
    for (long bytes : mUsedBytesOnTiers.values()) {
      mUsedBytes += bytes;
    }

    Set<Long> removedBlocks;
    if (mIsRegistered) {
      // This is a re-register of an existing worker. Assume the new block ownership data is more
      // up-to-date and update the existing block information.
      LOG.info("re-registering an existing workerId: {}", mId);

      // Compute the difference between the existing block data, and the new data.
      removedBlocks = Sets.difference(mBlocks, blocks);
    } else {
      removedBlocks = Collections.emptySet();
    }

    // Set the new block information.
    mBlocks = blocks;

    mIsRegistered = true;
    return removedBlocks;
  }

  /**
   * Adds a block to the worker.
   *
   * @param blockId the id of the block to be added
   */
  public void addBlock(long blockId) {
    mBlocks.add(blockId);
  }

  /**
   * Removes a block from the worker.
   *
   * @param blockId the id of the block to be removed
   */
  public void removeBlock(long blockId) {
    mBlocks.remove(blockId);
    mToRemoveBlocks.remove(blockId);
  }

  /**
   * Adds a new worker lost storage path.
   *
   * @param tierAlias the tier alias
   * @param dirPath the lost storage path
   */
  public void addLostStorage(String tierAlias, String dirPath) {
    List<String> paths = mLostStorage.getOrDefault(tierAlias, new ArrayList<>());
    paths.add(dirPath);
    mLostStorage.put(tierAlias, paths);
  }

  /**
   * Adds new worker lost storage paths.
   *
   * @param lostStorage the lost storage to add
   */
  public void addLostStorage(Map<String, StorageList> lostStorage) {
    for (Map.Entry<String, StorageList> entry : lostStorage.entrySet()) {
      List<String> paths = mLostStorage.getOrDefault(entry.getKey(), new ArrayList<>());
      paths.addAll(entry.getValue().getStorageList());
      mLostStorage.put(entry.getKey(), paths);
    }
  }

  /**
   * Gets the selected field information for this worker.
   *
   * @param fieldRange the client selected fields
   * @param isLiveWorker the worker is live or not
   * @return generated worker information
   */
  public WorkerInfo generateWorkerInfo(Set<WorkerInfoField> fieldRange, boolean isLiveWorker) {
    WorkerInfo info = new WorkerInfo();
    Set<WorkerInfoField> checkedFieldRange = fieldRange != null ? fieldRange :
        new HashSet<>(Arrays.asList(WorkerInfoField.values()));
    for (WorkerInfoField field : checkedFieldRange) {
      switch (field) {
        case ADDRESS:
          info.setAddress(mWorkerAddress);
          break;
        case WORKER_CAPACITY_BYTES:
          info.setCapacityBytes(mCapacityBytes);
          break;
        case WORKER_CAPACITY_BYTES_ON_TIERS:
          info.setCapacityBytesOnTiers(mTotalBytesOnTiers);
          break;
        case ID:
          info.setId(mId);
          break;
        case LAST_CONTACT_SEC:
          info.setLastContactSec(
              (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS));
          break;
        case START_TIME_MS:
          info.setStartTimeMs(mStartTimeMs);
          break;
        case STATE:
          if (isLiveWorker) {
            info.setState(LIVE_WORKER_STATE);
          } else {
            info.setState(LOST_WORKER_STATE);
          }
          break;
        case WORKER_USED_BYTES:
          info.setUsedBytes(mUsedBytes);
          break;
        case WORKER_USED_BYTES_ON_TIERS:
          info.setUsedBytesOnTiers(mUsedBytesOnTiers);
          break;
        default:
          LOG.warn("Unrecognized worker info field: " + field);
      }
    }
    return info;
  }

  /**
   * @return the worker's address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the available space of the worker in bytes
   */
  public long getAvailableBytes() {
    return mCapacityBytes - mUsedBytes;
  }

  /**
   * @return ids of all blocks the worker contains
   */
  public Set<Long> getBlocks() {
    return new HashSet<>(mBlocks);
  }

  /**
   * @return the capacity of the worker in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the id of the worker
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the worker in ms
   */
  public long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @return ids of blocks the worker should remove
   */
  public List<Long> getToRemoveBlocks() {
    return new ArrayList<>(mToRemoveBlocks);
  }

  /**
   * @return used space of the worker in bytes
   */
  public long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the storage tier mapping for the worker
   */
  public StorageTierAssoc getStorageTierAssoc() {
    return mStorageTierAssoc;
  }

  /**
   * @return the total bytes on each storage tier
   */
  public Map<String, Long> getTotalBytesOnTiers() {
    return mTotalBytesOnTiers;
  }

  /**
   * @return the used bytes on each storage tier
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @return the start time in milliseconds
   */
  public long getStartTime() {
    return mStartTimeMs;
  }

  /**
   * @return whether the worker has been registered yet
   */
  public boolean isRegistered() {
    return mIsRegistered;
  }

  /**
   * @return the free bytes on each storage tier
   */
  public Map<String, Long> getFreeBytesOnTiers() {
    Map<String, Long> freeCapacityBytes = new HashMap<>();
    for (Map.Entry<String, Long> entry : mTotalBytesOnTiers.entrySet()) {
      freeCapacityBytes.put(entry.getKey(),
          entry.getValue() - mUsedBytesOnTiers.get(entry.getKey()));
    }
    return freeCapacityBytes;
  }

  /**
   * @return the map from tier alias to lost storage paths in this worker
   */
  public Map<String, List<String>> getLostStorage() {
    return new HashMap<>(mLostStorage);
  }

  /**
   * @return true if this worker has lost storage, false otherwise
   */
  public boolean hasLostStorage() {
    return mLostStorage.size() > 0;
  }

  @Override
  public String toString() {
    Collection<Long> blocks = mBlocks;
    String blockFieldName = "blocks";
    // We truncate the list of block IDs to print, unless it is for DEBUG logs
    if (!LOG.isDebugEnabled() && mBlocks.size() > BLOCK_SIZE_LIMIT) {
      blockFieldName = "blocks-truncated";
      blocks = mBlocks.stream().limit(BLOCK_SIZE_LIMIT).collect(Collectors.toList());
    }
    return MoreObjects.toStringHelper(this)
        .add("id", mId)
        .add("workerAddress", mWorkerAddress)
        .add("capacityBytes", mCapacityBytes)
        .add("usedBytes", mUsedBytes)
        .add("lastUpdatedTimeMs", mLastUpdatedTimeMs)
        .add("blockCount", mBlocks.size())
        .add(blockFieldName, blocks)
        .add("lostStorage", mLostStorage).toString();
  }

  /**
   * Updates the last updated time of the worker in ms.
   */
  public void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Adds or removes a block from the to-be-removed blocks set of the worker.
   *
   * @param add true if to add, to remove otherwise
   * @param blockId the id of the block to be added or removed
   */
  public void updateToRemovedBlock(boolean add, long blockId) {
    if (add) {
      if (mBlocks.contains(blockId)) {
        mToRemoveBlocks.add(blockId);
      }
    } else {
      mToRemoveBlocks.remove(blockId);
    }
  }

  /**
   * Sets the capacity of the worker in bytes.
   *
   * @param capacityBytesOnTiers used bytes on each storage tier
   */
  public void updateCapacityBytes(Map<String, Long> capacityBytesOnTiers) {
    mCapacityBytes = 0;
    mTotalBytesOnTiers = capacityBytesOnTiers;
    for (long t : mTotalBytesOnTiers.values()) {
      mCapacityBytes += t;
    }
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * @param usedBytesOnTiers used bytes on each storage tier
   */
  public void updateUsedBytes(Map<String, Long> usedBytesOnTiers) {
    mUsedBytes = 0;
    mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    for (long t : mUsedBytesOnTiers.values()) {
      mUsedBytes += t;
    }
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * @param tierAlias alias of storage tier
   * @param usedBytesOnTier used bytes on certain storage tier
   */
  public void updateUsedBytes(String tierAlias, long usedBytesOnTier) {
    mUsedBytes += usedBytesOnTier - mUsedBytesOnTiers.get(tierAlias);
    mUsedBytesOnTiers.put(tierAlias, usedBytesOnTier);
  }
}
