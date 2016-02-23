/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.block.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The metadata for an Alluxio block, managed by the block master.
 */
@NotThreadSafe
public final class MasterBlockInfo {
  /** The id of the block. */
  private final long mBlockId;
  /** The length of the block in bytes. */
  private final long mLength;

  /** Maps from the worker id to the tier alias the block is on. */
  private final Map<Long, String> mWorkerIdToAlias;

  /**
   * Creates a new instance of {@link MasterBlockInfo}.
   *
   * @param blockId the block id to use
   * @param length the block length in bytes to use
   */
  public MasterBlockInfo(long blockId, long length) {
    // TODO(gene): Check valid length?
    mBlockId = blockId;
    mLength = length;

    mWorkerIdToAlias = new HashMap<Long, String>();
  }

  /**
   * @return the length of the block
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return the block id
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * Adds a location of the block. It means that the worker has the block in one of its tiers.
   *
   * @param workerId The id of the worker
   * @param tierAlias The alias of the storage tier that this block is on
   */
  public synchronized void addWorker(long workerId, String tierAlias) {
    mWorkerIdToAlias.put(workerId, tierAlias);
  }

  /**
   * Removes the worker from the locations of this block.
   *
   * @param workerId the worker id to remove
   */
  public void removeWorker(long workerId) {
    mWorkerIdToAlias.remove(workerId);
  }

  /**
   * @return all the worker ids that this block is on
   */
  public Set<Long> getWorkers() {
    return Collections.unmodifiableSet(mWorkerIdToAlias.keySet());
  }

  /**
   * @return the number of workers this block is on
   */
  public int getNumLocations() {
    return mWorkerIdToAlias.size();
  }

  /**
   * Gets the locations of the block, which are the workers' net address who has the data of the
   * block in its tiered storage.
   *
   * @return the net addresses of the locations
   */
  public synchronized List<MasterBlockLocation> getBlockLocations() {
    List<MasterBlockLocation> ret = new ArrayList<MasterBlockLocation>(mWorkerIdToAlias.size());
    for (Map.Entry<Long, String> entry : mWorkerIdToAlias.entrySet()) {
      ret.add(new MasterBlockLocation(entry.getKey(), entry.getValue()));
    }
    return ret;
  }

  /**
   * @param targetTierAlias the tier alias to target
   * @return true if the block is in the given tier
   */
  public synchronized boolean isInTier(String targetTierAlias) {
    for (String tierAlias : mWorkerIdToAlias.values()) {
      if (tierAlias.equals(targetTierAlias)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("MasterBlockInfo(");
    sb.append("mBlockId: ").append(mBlockId);
    sb.append(", mLength: ").append(mLength);
    sb.append(")");
    return sb.toString();
  }
}
