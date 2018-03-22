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

package alluxio.master.file;

import alluxio.exception.status.UnavailableException;
import alluxio.master.block.BlockMaster;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Class for tracking which blocks need to be deleted, and deleting them on close.
 */
public final class DefaultBlockDeletionContext implements BlockDeletionContext {
  private final BlockMaster mBlockMaster;
  private final List<Long> mBlocks;

  /**
   * @param blockMaster the block master to use for deleting blocks
   */
  public DefaultBlockDeletionContext(BlockMaster blockMaster) {
    mBlockMaster = blockMaster;
    mBlocks = new ArrayList<>();
  }

  @Override
  public void registerBlocksForDeletion(Collection<Long> blockIds) {
    mBlocks.addAll(blockIds);
  }

  @Override
  public void registerBlockForDeletion(long blockId) {
    mBlocks.add(blockId);
  }

  @Override
  public List<Long> getBlocks() {
    return ImmutableList.copyOf(mBlocks);
  }

  @Override
  public void close() throws UnavailableException {
    mBlockMaster.removeBlocks(mBlocks, true);
    // In case close gets called multiple times for some reason.
    mBlocks.clear();
  }
}
