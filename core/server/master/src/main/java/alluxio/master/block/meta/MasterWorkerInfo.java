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
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.grpc.StorageList;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
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

  /** worker metadata */
  private final WorkerMeta mMeta;
  /** lock the worker metadata */
  public ReentrantReadWriteLock mMetaLock;
  /** worker usage data */
  private WorkerUsageMeta mUsage;
  /** lock the worker usage data */
  public ReentrantReadWriteLock mUsageLock;

  /** ids of blocks the worker contains. */
  private Set<Long> mBlocks;
  /** ids of blocks the worker should remove. */
  private Set<Long> mToRemoveBlocks;
  public ReentrantReadWriteLock mBlockListLock;

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, WorkerNetAddress address) {
    // TODO(jiacheng): do I lock here? Or do I wrap the lock into the object?
    mMeta = new WorkerMeta(id, address);
    mUsage = new WorkerUsageMeta();
    mBlocks = new HashSet<>();
    mToRemoveBlocks = new HashSet<>();

    // Init all locks
    mMetaLock = new ReentrantReadWriteLock();
    mUsageLock = new ReentrantReadWriteLock();
    mBlockListLock = new ReentrantReadWriteLock();
  }

  /**
   * Marks the worker as registered, while updating all of its metadata.
   * Write locks on {@link MasterWorkerInfo#mUsageLock} and {@link MasterWorkerInfo#mBlockListLock}
   * are required.
   *
   * @param globalStorageTierAssoc global mapping between storage aliases and ordinal position
   * @param storageTierAliases list of storage tier aliases in order of their position in the
   *        hierarchy
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used byes
   * @param blocks set of block ids on this worker
   * @return A Set of blocks removed (or lost) from this worker
   */
  // TODO(jiacheng): All the locks are already locked before reaching here
  public Set<Long> register(final StorageTierAssoc globalStorageTierAssoc,
      final List<String> storageTierAliases, final Map<String, Long> totalBytesOnTiers,
      final Map<String, Long> usedBytesOnTiers, final Set<Long> blocks) {
    // TODO(jiacheng): IllegalArgumentException?
    mUsage.updateUsage(globalStorageTierAssoc, storageTierAliases,
            totalBytesOnTiers, usedBytesOnTiers);

    Set<Long> removedBlocks;
    if (mMeta.mIsRegistered) {
      // This is a re-register of an existing worker. Assume the new block ownership data is more
      // up-to-date and update the existing block information.
      LOG.info("re-registering an existing workerId: {}", mMeta.mId);

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
    List<String> paths = mUsage.mLostStorage.getOrDefault(tierAlias, new ArrayList<>());
    paths.add(dirPath);
    mUsage.mLostStorage.put(tierAlias, paths);
  }

  /**
   * Adds new worker lost storage paths.
   *
   * @param lostStorage the lost storage to add
   */
  public void addLostStorage(Map<String, StorageList> lostStorage) {
    for (Map.Entry<String, StorageList> entry : lostStorage.entrySet()) {
      List<String> paths = mUsage.mLostStorage.getOrDefault(entry.getKey(), new ArrayList<>());
      paths.addAll(entry.getValue().getStorageList());
      mUsage.mLostStorage.put(entry.getKey(), paths);
    }
  }

  /**
   * Gets the selected field information for this worker.
   *
   * @param fieldRange the client selected fields
   * @param isLiveWorker the worker is live or not
   * @return generated worker information
   */
  // TODO(jiacheng): this will be rewritten because some fields will be regrouped
  public WorkerInfo generateWorkerInfo(Set<WorkerInfoField> fieldRange, boolean isLiveWorker) {
    WorkerInfo info = new WorkerInfo();
    Set<WorkerInfoField> checkedFieldRange = fieldRange != null ? fieldRange :
        new HashSet<>(Arrays.asList(WorkerInfoField.values()));
    for (WorkerInfoField field : checkedFieldRange) {
      switch (field) {
        case ADDRESS:
          info.setAddress(mMeta.mWorkerAddress);
          break;
        case WORKER_CAPACITY_BYTES:
          info.setCapacityBytes(mUsage.mCapacityBytes);
          break;
        case WORKER_CAPACITY_BYTES_ON_TIERS:
          info.setCapacityBytesOnTiers(mUsage.mTotalBytesOnTiers);
          break;
        case ID:
          info.setId(mMeta.mId);
          break;
        case LAST_CONTACT_SEC:
          info.setLastContactSec(
              (int) ((CommonUtils.getCurrentMs() - mMeta.mLastUpdatedTimeMs) / Constants.SECOND_MS));
          break;
        case START_TIME_MS:
          info.setStartTimeMs(mMeta.mStartTimeMs);
          break;
        case STATE:
          if (isLiveWorker) {
            info.setState(LIVE_WORKER_STATE);
          } else {
            info.setState(LOST_WORKER_STATE);
          }
          break;
        case WORKER_USED_BYTES:
          info.setUsedBytes(mUsage.mUsedBytes);
          break;
        case WORKER_USED_BYTES_ON_TIERS:
          info.setUsedBytesOnTiers(mUsage.mUsedBytesOnTiers);
          break;
        default:
          LOG.warn("Unrecognized worker info field: " + field);
      }
    }
    return info;
  }

  /**
   * {@link WorkerMeta#mWorkerAddress is final} so the value can be read without locking
   *
   * @return the worker's address
   */
  public WorkerNetAddress getWorkerAddress() {
    return mMeta.mWorkerAddress;
  }

  /**
   * @return the available space of the worker in bytes
   */
  public long getAvailableBytes() {
    return mUsage.getAvailableBytes();
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
    return mUsage.mCapacityBytes;
  }

  /**
   * @return the id of the worker
   */
  public long getId() {
    return mMeta.mId;
  }

  /**
   * @return the last updated time of the worker in ms
   */
  public long getLastUpdatedTimeMs() {
    return mMeta.mLastUpdatedTimeMs;
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
    return mUsage.mUsedBytes;
  }

  /**
   * @return the storage tier mapping for the worker
   */
  public StorageTierAssoc getStorageTierAssoc() {
    return mUsage.mStorageTierAssoc;
  }

  /**
   * @return the total bytes on each storage tier
   */
  public Map<String, Long> getTotalBytesOnTiers() {
    return mUsage.mTotalBytesOnTiers;
  }

  /**
   * @return the used bytes on each storage tier
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    return mUsage.mUsedBytesOnTiers;
  }

  /**
   * @return the start time in milliseconds
   */
  public long getStartTime() {
    return mMeta.mStartTimeMs;
  }

  /**
   * @return whether the worker has been registered yet
   */
  public boolean isRegistered() {
    return mMeta.mIsRegistered;
  }

  /**
   * @return the free bytes on each storage tier
   */
  public Map<String, Long> getFreeBytesOnTiers() {
    Map<String, Long> freeCapacityBytes = new HashMap<>();
    for (Map.Entry<String, Long> entry : mUsage.mTotalBytesOnTiers.entrySet()) {
      freeCapacityBytes.put(entry.getKey(),
          entry.getValue() - mUsage.mUsedBytesOnTiers.get(entry.getKey()));
    }
    return freeCapacityBytes;
  }

  /**
   * @return the map from tier alias to lost storage paths in this worker
   */
  public Map<String, List<String>> getLostStorage() {
    return new HashMap<>(mUsage.mLostStorage);
  }

  /**
   * @return true if this worker has lost storage, false otherwise
   */
  public boolean hasLostStorage() {
    return mUsage.mLostStorage.size() > 0;
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
        .add("id", mMeta.mId)
        .add("workerAddress", mMeta.mWorkerAddress)
        .add("capacityBytes", mUsage.mCapacityBytes)
        .add("usedBytes", mUsage.mUsedBytes)
        .add("lastUpdatedTimeMs", mMeta.mLastUpdatedTimeMs)
        .add("blockCount", mBlocks.size())
        .add(blockFieldName, blocks)
        .add("lostStorage", mUsage.mLostStorage).toString();
  }

  /**
   * Updates the last updated time of the worker in ms.
   */
  public void updateLastUpdatedTimeMs() {
    mMeta.mLastUpdatedTimeMs = CommonUtils.getCurrentMs();
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
    long capacityBytes = 0;
    mUsage.mTotalBytesOnTiers = capacityBytesOnTiers;
    for (long t : mUsage.mTotalBytesOnTiers.values()) {
      capacityBytes += t;
    }
    mUsage.mCapacityBytes = capacityBytes;
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * @param usedBytesOnTiers used bytes on each storage tier
   */
  public void updateUsedBytes(Map<String, Long> usedBytesOnTiers) {
    long usedBytes = 0;
    mUsage.mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    for (long t : mUsage.mUsedBytesOnTiers.values()) {
      usedBytes += t;
    }
    mUsage.mUsedBytes = usedBytes;
  }

  /**
   * Sets the used space of the worker in bytes.
   *
   * @param tierAlias alias of storage tier
   * @param usedBytesOnTier used bytes on certain storage tier
   */
  public void updateUsedBytes(String tierAlias, long usedBytesOnTier) {
    mUsage.mUsedBytes += usedBytesOnTier - mUsage.mUsedBytesOnTiers.get(tierAlias);
    mUsage.mUsedBytesOnTiers.put(tierAlias, usedBytesOnTier);
  }
}
