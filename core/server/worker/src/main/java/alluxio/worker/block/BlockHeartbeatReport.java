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

package alluxio.worker.block;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Container for the delta information in each worker-to-master heartbeat.
 */
@ThreadSafe
public final class BlockHeartbeatReport {
  /** Map of storage location to a list of blocks ids added in the last heartbeat period. */
  private final Map<BlockStoreLocation, List<Long>> mAddedBlocks;
  /** List of block ids removed in the last heartbeat period. */
  private final List<Long> mRemovedBlocks;
  /**
   * Map of storage tier alias to a list of lost storage paths
   * that were removed in the last heartbeat period.
   */
  private final Map<String, List<String>> mLostStorage;

  /**
   * Creates a new instance of {@link BlockHeartbeatReport}.
   *
   * @param addedBlocks added blocks
   * @param removedBlocks remove blocks
   * @param lostStorage lost storage
   */
  public BlockHeartbeatReport(Map<BlockStoreLocation, List<Long>> addedBlocks,
      List<Long> removedBlocks, Map<String, List<String>> lostStorage) {
    mAddedBlocks = new HashMap<>(addedBlocks);
    mRemovedBlocks = new ArrayList<>(removedBlocks);
    mLostStorage = new HashMap<>(lostStorage);
  }

  /**
   * Gets the list of blocks added by the worker in the heartbeat this report represents.
   *
   * @return a map from storage location to lists of block ids added
   */
  public Map<BlockStoreLocation, List<Long>> getAddedBlocks() {
    return Collections.unmodifiableMap(mAddedBlocks);
  }

  /**
   * Gets the list of blocks removed from this worker in the heartbeat this report represents.
   *
   * @return a list of block ids which have been removed
   */
  public List<Long> getRemovedBlocks() {
    return Collections.unmodifiableList(mRemovedBlocks);
  }

  /**
   * Gets the storage paths which were lost
   * in the heartbeat this report represents.
   *
   * @return a map from storage tier alias to lost storage paths
   */
  public Map<String, List<String>> getLostStorage() {
    return Collections.unmodifiableMap(mLostStorage);
  }
}
