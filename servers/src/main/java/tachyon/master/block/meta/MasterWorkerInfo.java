/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.block.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.StorageTierAssoc;
import tachyon.WorkerStorageTierAssoc;
import tachyon.thrift.WorkerInfo;
import tachyon.util.CommonUtils;
import tachyon.worker.NetAddress;

/**
 * Metadata for a Tachyon worker.
 */
@ThreadSafe
public final class MasterWorkerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Worker's address */
  private final NetAddress mWorkerAddress;
  /** The id of the worker */
  private final long mId;
  /** Start time of the worker in ms */
  private final long mStartTimeMs;
  /** Capacity of worker in bytes */
  private long mCapacityBytes;
  /** Worker's used bytes */
  private long mUsedBytes;
  /** Worker's last updated time in ms */
  private long mLastUpdatedTimeMs;
  /** If true, the worker is considered registered. */
  private boolean mIsRegistered;
  /** Worker-specific mapping between storage tier alias and storage tier ordinal. */
  private StorageTierAssoc mStorageTierAssoc;
  /** Mapping from storage tier alias to total bytes */
  private Map<String, Long> mTotalBytesOnTiers;
  /** Mapping from storage tier alias to used bytes */
  private Map<String, Long> mUsedBytesOnTiers;

  /** ids of blocks the worker contains */
  private Set<Long> mBlocks;
  /** ids of blocks the worker should remove */
  private Set<Long> mToRemoveBlocks;

  /**
   * Creates a new instance of {@link MasterWorkerInfo}.
   *
   * @param id the worker id to use
   * @param address the worker address to use
   */
  public MasterWorkerInfo(long id, NetAddress address) {
    mWorkerAddress = Preconditions.checkNotNull(address);
    mId = id;
    mStartTimeMs = System.currentTimeMillis();
    mLastUpdatedTimeMs = System.currentTimeMillis();
    mIsRegistered = false;
    mStorageTierAssoc = null;
    mTotalBytesOnTiers = new HashMap<String, Long>();
    mUsedBytesOnTiers = new HashMap<String, Long>();
    mBlocks = new HashSet<Long>();
    mToRemoveBlocks = new HashSet<Long>();
  }

  /**
   * Marks the worker as registered, while updating all of its metadata.
   *
   * @param globalStorageTierAssoc global mapping between storage aliases and ordinal position
   * @param storageTierAliases list of storage tier alises in order of their position in the
   *        hierarchy
   * @param totalBytesOnTiers mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers mapping from storage tier alias to used byes
   * @param blocks set of block ids on this worker
   * @return A Set of blocks removed (or lost) from this worker
   */
  public synchronized Set<Long> register(final StorageTierAssoc globalStorageTierAssoc,
      final List<String> storageTierAliases, final Map<String, Long> totalBytesOnTiers,
      final Map<String, Long> usedBytesOnTiers, final Set<Long> blocks) {
    // If the storage aliases do not have strictly increasing ordinal value based on the total
    // ordering, throw an error
    for (int i = 0; i < storageTierAliases.size() - 1; i ++) {
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
    mTotalBytesOnTiers = new HashMap<String, Long>(totalBytesOnTiers);
    mUsedBytesOnTiers = new HashMap<String, Long>(usedBytesOnTiers);
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
    mBlocks = new HashSet<Long>(blocks);

    mIsRegistered = true;
    return removedBlocks;
  }

  /**
   * Adds a block to the worker.
   *
   * @param blockId the id of the block to be added
   */
  public synchronized void addBlock(long blockId) {
    mBlocks.add(blockId);
  }

  /**
   * Removes a block from the worker.
   *
   * @param blockId the id of the block to be removed
   */
  public synchronized void removeBlock(long blockId) {
    mBlocks.remove(blockId);
    mToRemoveBlocks.remove(blockId);
  }

  /**
   * @return generated {@link WorkerInfo} for this worker
   */
  public synchronized WorkerInfo generateClientWorkerInfo() {
    WorkerInfo ret = new WorkerInfo();
    ret.id = mId;
    ret.address = mWorkerAddress.toThrift();
    ret.lastContactSec =
        (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS);
    ret.state = "In Service";
    ret.capacityBytes = mCapacityBytes;
    ret.usedBytes = mUsedBytes;
    ret.startTimeMs = mStartTimeMs;
    return ret;
  }

  /**
   * @return the worker's address
   */
  public synchronized NetAddress getWorkerAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the available space of the worker in bytes
   */
  public synchronized long getAvailableBytes() {
    return mCapacityBytes - mUsedBytes;
  }

  /**
   * @return ids of all blocks the worker contains
   */
  public synchronized Set<Long> getBlocks() {
    return new HashSet<Long>(mBlocks);
  }

  /**
   * @return the capacity of the worker in bytes
   */
  public synchronized long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the id of the worker
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the worker in ms
   */
  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @return ids of blocks the worker should remove
   */
  public synchronized List<Long> getToRemoveBlocks() {
    return new ArrayList<Long>(mToRemoveBlocks);
  }

  /**
   * @return used space of the worker in bytes
   */
  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * @return the storage tier mapping for the worker
   */
  public synchronized StorageTierAssoc getStorageTierAssoc() {
    return mStorageTierAssoc;
  }

  /**
   * @return the total bytes on each storage tier
   */
  public synchronized Map<String, Long> getTotalBytesOnTiers() {
    return mTotalBytesOnTiers;
  }

  /**
   * @return the used bytes on each storage tier
   */
  public synchronized Map<String, Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @return the start time in milliseconds
   */
  public synchronized long getStartTime() {
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
  public synchronized Map<String, Long> getFreeBytesOnTiers() {
    Map<String, Long> freeCapacityBytes = new HashMap<String, Long>();
    for (Map.Entry<String, Long> entry : mTotalBytesOnTiers.entrySet()) {
      freeCapacityBytes.put(entry.getKey(),
          entry.getValue() - mUsedBytesOnTiers.get(entry.getKey()));
    }
    return freeCapacityBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterWorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", mWorkerAddress: ").append(mWorkerAddress);
    sb.append(", TOTAL_BYTES: ").append(mCapacityBytes);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(mCapacityBytes - mUsedBytes);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    sb.append(", mBlocks: [ ");
    for (long blockId : mBlocks) {
      sb.append(blockId).append(", ");
    }
    sb.append("] )");
    return sb.toString();
  }

  /**
   * Updates the last updated time of the worker in ms.
   */
  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Adds or removes a block from the to-be-removed blocks set of the worker.
   *
   * @param add true if to add, to remove otherwise
   * @param blockId the id of the block to be added or removed
   */
  public synchronized void updateToRemovedBlock(boolean add, long blockId) {
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
  public synchronized void updateUsedBytes(Map<String, Long> usedBytesOnTiers) {
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
  public synchronized void updateUsedBytes(String tierAlias, long usedBytesOnTier) {
    mUsedBytes += usedBytesOnTier - mUsedBytesOnTiers.get(tierAlias);
    mUsedBytesOnTiers.put(tierAlias, usedBytesOnTier);
  }
}
