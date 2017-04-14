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
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Metadata for an Alluxio worker. This class is not thread safe, so external locking is required.
 */
@NotThreadSafe
public final class MasterWorkerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(MasterWorkerInfo.class);

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

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, WorkerNetAddress address) {
    mWorkerAddress = Preconditions.checkNotNull(address);
    mId = id;
    mStartTimeMs = System.currentTimeMillis();
    mLastUpdatedTimeMs = System.currentTimeMillis();
    mIsRegistered = false;
    mStorageTierAssoc = null;
    mTotalBytesOnTiers = new HashMap<>();
    mUsedBytesOnTiers = new HashMap<>();
    mBlocks = new HashSet<>();
    mToRemoveBlocks = new HashSet<>();
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
    mBlocks = new HashSet<>(blocks);

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
   * @return generated {@link WorkerInfo} for this worker
   */
  public WorkerInfo generateClientWorkerInfo() {
    return new WorkerInfo()
        .setId(mId)
        .setAddress(mWorkerAddress)
        .setLastContactSec(
            (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS))
        .setState("In Service").setCapacityBytes(mCapacityBytes).setUsedBytes(mUsedBytes)
        .setStartTimeMs(mStartTimeMs);
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("id", mId).add("workerAddress", mWorkerAddress)
        .add("capacityBytes", mCapacityBytes).add("usedBytes", mUsedBytes)
        .add("lastUpdatedTimeMs", mLastUpdatedTimeMs).add("blocks", mBlocks).toString();
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
   * Sets the used space of the worker in bytes.
   *
   * @param usedBytesOnTiers used bytes on each storage tier
   */
  public void updateUsedBytes(Map<String, Long> usedBytesOnTiers) {
    mUsedBytes = 0;
    mUsedBytesOnTiers = usedBytesOnTiers;
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
