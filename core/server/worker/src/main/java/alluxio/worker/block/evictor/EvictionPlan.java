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

package alluxio.worker.block.evictor;

import alluxio.collections.Pair;
import alluxio.worker.block.BlockStoreLocation;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides information about the blocks that need to be moved when evicting.
 */
@ThreadSafe
public final class EvictionPlan {
  /** A list of block transfer information, with block id, source and destination location. */
  private final List<BlockTransferInfo> mToMove;
  /** A list of pairs of block id to remove and its location. */
  private final List<Pair<Long, BlockStoreLocation>> mToEvict;

  /**
   * Creates a new instance of {@link EvictionPlan}.
   *
   * @param toTransfer a list of block transfer information
   * @param toEvict a list of blocks to be evicted
   */
  public EvictionPlan(List<BlockTransferInfo> toTransfer,
      List<Pair<Long, BlockStoreLocation>> toEvict) {
    mToMove = Preconditions.checkNotNull(toTransfer, "toTransfer");
    mToEvict = Preconditions.checkNotNull(toEvict, "toEvict");
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
    return MoreObjects.toStringHelper(this)
        .add("toMove", mToMove)
        .add("toEvict", mToEvict).toString();
  }
}
