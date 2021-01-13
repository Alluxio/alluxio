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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class for tracking which blocks need to be deleted, and deleting them on close.
 */
public final class DefaultBlockDeletionContext implements BlockDeletionContext {
  private final List<BlockDeletionListener> mListeners;
  private final ConcurrentLinkedQueue<Long> mBlocks;

  /**
   * @param listeners listeners to call for each deleted block when the context is closed
   */
  public DefaultBlockDeletionContext(BlockDeletionListener... listeners) {
    mListeners = Arrays.asList(listeners);
    mBlocks = new ConcurrentLinkedQueue();
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
  public void close() throws IOException {
    // Make sure every listener gets called, even if some throw exceptions.
    Throwable thrown = null;
    List<Long> blocksCopy = ImmutableList.copyOf(mBlocks);
    for (BlockDeletionListener listener : mListeners) {
      try {
        listener.process(blocksCopy);
      } catch (Throwable t) {
        if (thrown != null) {
          thrown.addSuppressed(t);
        } else {
          thrown = t;
        }
      }
    }
    if (thrown != null) {
      Throwables.propagateIfPossible(thrown, IOException.class);
      throw new RuntimeException(thrown);
    }
  }
}
