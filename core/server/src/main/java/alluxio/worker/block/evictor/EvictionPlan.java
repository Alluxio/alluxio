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

package alluxio.worker.block.evictor;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.collections.Pair;
import alluxio.worker.block.BlockStoreLocation;

/**
 * This class provides information about the blocks that need to be moved when evicting.
 */
@ThreadSafe
public final class EvictionPlan {
  /** A list of block transfer information, with block id, source and destination location */
  private final List<BlockTransferInfo> mToMove;
  /** A list of pairs of block id to remove and its location */
  private final List<Pair<Long, BlockStoreLocation>> mToEvict;

  /**
   * Creates a new instance of {@link EvictionPlan}.
   *
   * @param toTransfer a list of block transfer information
   * @param toEvict a list of blocks to be evicted
   */
  public EvictionPlan(List<BlockTransferInfo> toTransfer,
      List<Pair<Long, BlockStoreLocation>> toEvict) {
    mToMove = Preconditions.checkNotNull(toTransfer);
    mToEvict = Preconditions.checkNotNull(toEvict);
  }

  /**
   * @return a list of block transfer information, with block id, source and destination location
   */
  public List<BlockTransferInfo> toMove() {
    return mToMove;
  }

  /**
   * @return a list of pairs of block id to remove and its location
   */
  public List<Pair<Long, BlockStoreLocation>> toEvict() {
    return mToEvict;
  }

  /**
   * Whether the plan is empty, an empty plan means both toMove and toEvict are empty, also, an
   * empty plan indicates no action (move or evict) needs to be taken to meet the requirement.
   *
   * @return true if empty otherwise false
   */
  public boolean isEmpty() {
    return mToEvict.isEmpty() && mToMove.isEmpty();
  }

  @Override
  public String toString() {
    return "toMove: " + mToMove.toString() + ", toEvict: " + mToEvict.toString();
  }
}
