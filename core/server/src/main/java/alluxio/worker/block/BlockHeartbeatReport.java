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

package alluxio.worker.block;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Container for the delta information in each worker to master heartbeat.
 */
@ThreadSafe
public final class BlockHeartbeatReport {
  /** Map of storage tier alias to a list of blocks ids added in the last heartbeat period. */
  private final Map<String, List<Long>> mAddedBlocks;
  /** List of block ids removed in the last heartbeat period. */
  private final List<Long> mRemovedBlocks;

  /**
   * Creates a new instance of {@link BlockHeartbeatReport}.
   *
   * @param addedBlocks added blocks
   * @param removedBlocks remove blocks
   */
  public BlockHeartbeatReport(Map<String, List<Long>> addedBlocks, List<Long> removedBlocks) {
    mAddedBlocks = addedBlocks;
    mRemovedBlocks = removedBlocks;
  }

  /**
   * Gets the list of blocks added by the worker in the heartbeat this report represents.
   *
   * @return a map from storage tier alias to lists of block ids added
   */
  public Map<String, List<Long>> getAddedBlocks() {
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
}
