/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import tachyon.Constants;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

/**
 * The structure to store a worker's information in master node.
 */
public class MasterWorkerInfo {

  /* Worker's address */
  public final InetSocketAddress ADDRESS;
  /* Capacity of worker in bytes */
  private final long CAPACITY_BYTES;
  /* Worker's start time in ms */
  private final long START_TIME_MS;
  /* Id of worker in long */
  private long mId;
  /* Worker's used bytes */
  private long mUsedBytes;
  /* Worker's last updated time in ms */
  private long mLastUpdatedTimeMs;
  /* Collection to store blockIds */
  private Set<Long> mBlocks;
  /* Collection to store blockId to be removed */
  private Set<Long> mToRemoveBlocks;

  public MasterWorkerInfo(long id, InetSocketAddress address, long capacityBytes) {
    mId = id;
    ADDRESS = address;
    CAPACITY_BYTES = capacityBytes;
    START_TIME_MS = System.currentTimeMillis();

    mUsedBytes = 0;
    mBlocks = new HashSet<Long>();
    mToRemoveBlocks = new HashSet<Long>();
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Generates <code>ClientWorkerInfo</code> for this worker
   *
   * @return contains instance of <code>ClientWorkerInfo</code>
   */
  public synchronized ClientWorkerInfo generateClientWorkerInfo() {
    ClientWorkerInfo ret = new ClientWorkerInfo();
    ret.id = mId;
    ret.address = new NetAddress(ADDRESS.getAddress().getCanonicalHostName(), ADDRESS.getPort());
    ret.lastContactSec = (int) ((CommonUtils.getCurrentMs() - mLastUpdatedTimeMs) / Constants.SECOND_MS);
    ret.state = "In Service";
    ret.capacityBytes = CAPACITY_BYTES;
    ret.usedBytes = mUsedBytes;
    ret.starttimeMs = START_TIME_MS;
    return ret;
  }

  /**
   * Returns worker's address
   *
   * @return contains instance of <code>InetSocketAddress</code>
   */
  public InetSocketAddress getAddress() {
    return ADDRESS;
  }

  /**
   * Returns the available bytes in worker's memory
   *
   * @return long contains available bytes
   */
  public synchronized long getAvailableBytes() {
    return CAPACITY_BYTES - mUsedBytes;
  }

  /**
   * Get all blocks in the worker's memory.
   * 
   * @return ids of the blocks.
   */
  public synchronized Set<Long> getBlocks() {
    return new HashSet<Long>(mBlocks);
  }

  /**
   * Returns the capacity of worker's memory
   *
   * @return long represents capacity of worker's memory
   */
  public long getCapacityBytes() {
    return CAPACITY_BYTES;
  }

  public synchronized long getId() {
    return mId;
  }

  public synchronized long getLastUpdatedTimeMs() {
    return mLastUpdatedTimeMs;
  }

  /**
   * Get all blocks in the worker's memory need to be removed.
   * 
   * @return ids of the blocks need to be removed.
   */
  public synchronized List<Long> getToRemovedBlocks() {
    return new ArrayList<Long>(mToRemoveBlocks);
  }

  /**
   * Get used bytes of worker's memory
   *
   * @return long represents used bytes
   */
  public synchronized long getUsedBytes() {
    return mUsedBytes;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterWorkerInfo(");
    sb.append(" ID: ").append(mId);
    sb.append(", ADDRESS: ").append(ADDRESS);
    sb.append(", TOTAL_BYTES: ").append(CAPACITY_BYTES);
    sb.append(", mUsedBytes: ").append(mUsedBytes);
    sb.append(", mAvailableBytes: ").append(CAPACITY_BYTES - mUsedBytes);
    sb.append(", mLastUpdatedTimeMs: ").append(mLastUpdatedTimeMs);
    sb.append(", mBlocks: [ ");
    for (long blockId : mBlocks) {
      sb.append(blockId).append(", ");
    }
    sb.append("] )");
    return sb.toString();
  }

  /**
   * Adds or removes the blockId based on update flag
   *
   * @param add boolean if true then blockId will be added
   *            in blocks set otherwise will be removed from blocks set
   * @param blockId blockId to be added or removed
   */
  public synchronized void updateBlock(boolean add, long blockId) {
    if (add) {
      mBlocks.add(blockId);
    } else {
      mBlocks.remove(blockId);
    }
  }

  /**
   * Adds or removes the blockId in blocks set based on update flag
   *
   * @param add boolean if true then set of blockIds will be added
   *            in blocks set otherwise will be removed from blocks set
   * @param blockIds set of blockIds to be added or removed
   */
  public synchronized void updateBlocks(boolean add, Collection<Long> blockIds) {
    if (add) {
      mBlocks.addAll(blockIds);
    } else {
      mBlocks.removeAll(blockIds);
    }
  }

  /**
   * Updates the last updated time in ms
   */
  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  /**
   * Adds or removes the blockId in to-be-removed blocks set based on flag.
   * If input flag <code>add</code> is true then blockId will be added into to-be-removed blocks set
   * only if given blockId is already available in blocks set.
   * If input flag <code>add</code> is false then blockId will be removed from to-be-removed blocks set
   *
   * @param add boolean flag to indicate whether blockId to be added or to be removed in to-be-removed blocks set
   * @param blockId long represents blockId
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
   * Adds or removes the blockId in to be removed blocks set based on flag.
   * If input flag <code>add</code> is true then blockId will be added into to-be-removed blocks set
   * only if given blockId is already available in blocks set.
   * If input flag <code>add</code> is false then blockId will be removed from to-be-removed blocks set
   *
   * @param add boolean flag to indicate whether blockId to be added or to be removed in to-be-removed blocks set
   * @param blockIds set of blockIds
   */
  public synchronized void updateToRemovedBlocks(boolean add, Collection<Long> blockIds) {
    for (long blockId : blockIds) {
      updateToRemovedBlock(add, blockId);
    }
  }

  /**
   * Updates the used bytes of worker's memory
   *
   * @param usedBytes long represents used bytes
   */
  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}