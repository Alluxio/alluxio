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
  public final InetSocketAddress ADDRESS;
  private final long CAPACITY_BYTES;
  private final long START_TIME_MS;

  private long mId;
  private long mUsedBytes;
  private long mLastUpdatedTimeMs;
  private Set<Long> mBlocks;
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

  public InetSocketAddress getAddress() {
    return ADDRESS;
  }

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

  public synchronized void updateBlock(boolean add, long blockId) {
    if (add) {
      mBlocks.add(blockId);
    } else {
      mBlocks.remove(blockId);
    }
  }

  public synchronized void updateBlocks(boolean add, Collection<Long> blockIds) {
    if (add) {
      mBlocks.addAll(blockIds);
    } else {
      mBlocks.removeAll(blockIds);
    }
  }

  public synchronized void updateLastUpdatedTimeMs() {
    mLastUpdatedTimeMs = System.currentTimeMillis();
  }

  public synchronized void updateToRemovedBlock(boolean add, long blockId) {
    if (add) {
      if (mBlocks.contains(blockId)) {
        mToRemoveBlocks.add(blockId);
      }
    } else {
      mToRemoveBlocks.remove(blockId);
    }
  }

  public synchronized void updateToRemovedBlocks(boolean add, Collection<Long> blockIds) {
    for (long blockId : blockIds) {
      updateToRemovedBlock(add, blockId);
    }
  }

  public synchronized void updateUsedBytes(long usedBytes) {
    mUsedBytes = usedBytes;
  }
}