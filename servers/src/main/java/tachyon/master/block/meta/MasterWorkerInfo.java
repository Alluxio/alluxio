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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;
import tachyon.util.CommonUtils;

/**
 * Metadata for a Tachyon worker.
 */
public final class MasterWorkerInfo {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** Worker's address */
  public final NetAddress mWorkerAddress;
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
  // TODO: convert all tier information to tierAlias (or storage type).
  /** Total bytes on each storage tier */
  private List<Long> mTotalBytesOnTiers;
  /** Used bytes on each storage tier */
  private List<Long> mUsedBytesOnTiers;

  /** IDs of blocks the worker contains */
  private Set<Long> mBlocks;
  /** IDs of blocks the worker should remove */
  private Set<Long> mToRemoveBlocks;

  public MasterWorkerInfo(long id, NetAddress address) {
    mId = id;
    mWorkerAddress = Preconditions.checkNotNull(address);
    mStartTimeMs = System.currentTimeMillis();
    mLastUpdatedTimeMs = System.currentTimeMillis();
    mBlocks = new HashSet<Long>();
    mToRemoveBlocks = new HashSet<Long>();
    mIsRegistered = false;
  }

  /**
   * Marks the worker as registered, while updating all of its metadata.
   *
   * @param totalBytesOnTiers list of total bytes on each tier
   * @param usedBytesOnTiers list of the used byes on each tier
   * @param blocks set of block ids on this worker
   * @return A Set of blocks removed (or lost) from this worker.
   */
  public Set<Long> register(final List<Long> totalBytesOnTiers, final List<Long> usedBytesOnTiers,
      final Set<Long> blocks) {
    // validate the number of tiers
    if (totalBytesOnTiers.size() != usedBytesOnTiers.size()) {
      throw new IllegalArgumentException(
          "totalBytesOnTiers should have the same number of tiers as usedBytesOnTiers,"
              + " but totalBytesOnTiers has " + totalBytesOnTiers.size()
              + " tiers, while usedBytesOnTiers has " + usedBytesOnTiers.size() + " tiers");
    }

    // defensive copy
    mTotalBytesOnTiers = new ArrayList<Long>(totalBytesOnTiers);
    mUsedBytesOnTiers = new ArrayList<Long>(usedBytesOnTiers);
    mCapacityBytes = 0;
    for (long bytes : mTotalBytesOnTiers) {
      mCapacityBytes += bytes;
    }
    mUsedBytes = 0;
    for (long bytes : mUsedBytesOnTiers) {
      mUsedBytes += bytes;
    }

    Set<Long> removedBlocks;
    if (mIsRegistered) {
      // This is a re-register of an existing worker. Assume the new block ownership data is more
      // up-to-date and update the existing block information.
      LOG.info("re-registering an existing workerId: " + mId);

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
   * Adds a block to the worker
   *
   * @param blockId the ID of the block to be added
   */
  public synchronized void addBlock(long blockId) {
    mBlocks.add(blockId);
  }

  /**
   * Removes a block from the worker
   *
   * @param blockId the ID of the block to be removed
   */
  public synchronized void removeBlock(long blockId) {
    mBlocks.remove(blockId);
    mToRemoveBlocks.remove(blockId);
  }

  /**
   * @return Generated {@link WorkerInfo} for this worker
   */
  public synchronized WorkerInfo generateClientWorkerInfo() {
    WorkerInfo ret = new WorkerInfo();
    ret.id = mId;
    ret.address = mWorkerAddress;
    ret.lastContactSec =
        (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS);
    ret.state = "In Service";
    ret.capacityBytes = mCapacityBytes;
    ret.usedBytes = mUsedBytes;
    ret.startTimeMs = mStartTimeMs;
    return ret;
  }

  /**
   * @return the worker's address.
   */
  public NetAddress getAddress() {
    return mWorkerAddress;
  }

  /**
   * @return the available space of the worker in bytes
   */
  public synchronized long getAvailableBytes() {
    return mCapacityBytes - mUsedBytes;
  }

  /**
   * @return IDs of all blocks the worker contains.
   */
  public synchronized Set<Long> getBlocks() {
    return new HashSet<Long>(mBlocks);
  }

  /**
   * @return the capacity of the worker in bytes
   */
  public long getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * @return the ID of the worker
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the last updated time of the worker in ms.
   */
  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * @return IDs of blocks the worker should remove
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
   * @return the total bytes on each storage tier
   */
  public synchronized List<Long> getTotalBytesOnTiers() {
    return mTotalBytesOnTiers;
  }

  /**
   * @return the used bytes on each storage tier
   */
  public synchronized List<Long> getUsedBytesOnTiers() {
    return mUsedBytesOnTiers;
  }

  /**
   * @return the start time in milliseconds.
   */
  public long getStartTime() {
    return mStartTimeMs;
  }

  /**
   * @return the free bytes on each storage tier
   */
  public synchronized List<Long> getFreeBytesOnTiers() {
    List<Long> freeCapacityBytes = new ArrayList<Long>();
    for (int i = 0; i < mTotalBytesOnTiers.size(); i ++) {
      freeCapacityBytes.add(mTotalBytesOnTiers.get(i) - mUsedBytesOnTiers.get(i));
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
   * Updates the last updated time of the worker in ms
   */
  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Adds or removes a block from the to-be-removed blocks set of the worker.
   *
   * @param add true if to add, to remove otherwise.
   * @param blockId the ID of the block to be added or removed
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
   * Set the used space of the worker in bytes.
   *
   * @param usedBytesOnTiers used bytes on each storage tier
   */
  public synchronized void updateUsedBytes(List<Long> usedBytesOnTiers) {
    mUsedBytes = 0;
    mUsedBytesOnTiers = usedBytesOnTiers;
    for (long t : mUsedBytesOnTiers) {
      mUsedBytes += t;
    }
  }

  /**
   * Set the used space of the worker in bytes.
   *
   * @param tierLevel value of tier level
   * @param usedBytesOnTier used bytes on certain storage tier.
   */
  public synchronized void updateUsedBytes(int tierLevel, long usedBytesOnTier) {
    mUsedBytes += usedBytesOnTier - mUsedBytesOnTiers.get(tierLevel);
    mUsedBytesOnTiers.set(tierLevel, usedBytesOnTier);
  }
}
