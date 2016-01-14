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

package tachyon.worker.block;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Container for the delta information in each worker to master heartbeat.
 */
public final class BlockHeartbeatReport {
  /** Map of storage tier alias to a list of blocks ids added in the last heartbeat period */
  private final Map<String, List<Long>> mAddedBlocks;
  /** List of block ids removed in the last heartbeat period */
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
