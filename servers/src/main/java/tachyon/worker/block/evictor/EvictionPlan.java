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

package tachyon.worker.block.evictor;

import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.Pair;
import tachyon.worker.block.BlockStoreLocation;

/**
 * This class provides information about the blocks that need to be moved when evicting.
 */
public final class EvictionPlan {
  /** A list of pairs of blockId and its location to move to */
  private final List<Pair<Long, Pair<BlockStoreLocation, BlockStoreLocation>>> mToMove;
  /** A list of blockId to remove */
  private final List<Pair<Long, BlockStoreLocation>> mToEvict;

  public EvictionPlan(List<Pair<Long, Pair<BlockStoreLocation, BlockStoreLocation>>> toTransfer,
      List<Pair<Long, BlockStoreLocation>> toEvict) {
    mToMove = Preconditions.checkNotNull(toTransfer);
    mToEvict = Preconditions.checkNotNull(toEvict);
  }

  /**
   * @return a list of pairs of a block id and the locations of where it comes from and will be
   * moved to
   */
  public List<Pair<Long, Pair<BlockStoreLocation, BlockStoreLocation>>> toMove() {
    return mToMove;
  }

  /**
   * @return a list of blocks to remove, with id and location information
   */
  public List<Pair<Long, BlockStoreLocation>> toEvict() {
    return mToEvict;
  }

  /**
   * Whether the plan is empty, an empty plan means both toMove and toEvict are
   * empty, also, an empty plan indicates no action (move or evict) needs to be taken to meet the
   * requirement.
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
