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
import java.util.List;
import java.util.Map;
import java.util.Set;

import tachyon.StorageLevelAlias;

public final class MasterBlockInfo {
  private final long mBlockId;
  private final long mLength;

  /** Maps from the worker id to the alias the block is on. */
  private final Map<Long, Integer> mWorkerIdToAlias;

  public MasterBlockInfo(long blockId, long length) {
    // TODO: check valid length?
    mBlockId = blockId;
    mLength = length;

    mWorkerIdToAlias = new HashMap<Long, Integer>();
  }

  public long getLength() {
    return mLength;
  }

  public long getBlockId() {
    return mBlockId;
  }

  /**
   * Adds a location of the block. It means that the worker has the block in one of its tiers.
   *
   * @param workerId The id of the worker
   * @param tierAlias The int value of the tier alias that this block is on
   */
  public synchronized void addWorker(long workerId, int tierAlias) {
    mWorkerIdToAlias.put(workerId, tierAlias);
  }

  public void removeWorker(long workerId) {
    mWorkerIdToAlias.remove(workerId);
  }

  public Set<Long> getWorkers() {
    return Collections.unmodifiableSet(mWorkerIdToAlias.keySet());
  }

  public int getNumLocations() {
    return mWorkerIdToAlias.size();
  }

  /**
   * Gets the locations of the block, which are the workers' net address who has the data of the
   * block in its tiered storage. The list is sorted by the storage level alias(MEM, SSD, HDD). That
   * is, the worker who has the data of the block in its memory is in the top of the list.
   *
   * @return the net addresses of the locations
   */
  public synchronized List<MasterBlockLocation> getBlockLocations() {
    List<MasterBlockLocation> ret = new ArrayList<MasterBlockLocation>(mWorkerIdToAlias.size());
    for (StorageLevelAlias alias : StorageLevelAlias.values()) {
      for (Map.Entry<Long, Integer> entry : mWorkerIdToAlias.entrySet()) {
        if (alias.getValue() == entry.getValue()) {
          ret.add(new MasterBlockLocation(entry.getKey(), alias.getValue()));
        }
      }
    }
    return ret;
  }

  /**
   * @return true if the block is in some worker's memory, false otherwise
   */
  public synchronized boolean isInMemory() {
    for (int aliasValue : mWorkerIdToAlias.values()) {
      if (aliasValue == StorageLevelAlias.MEM.getValue()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized String toString() {
    // TODO
    StringBuilder sb = new StringBuilder("MasterBlockInfo(");
    sb.append("mBlockId: ").append(mBlockId);
    return sb.toString();
  }
}
