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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Interface for a class which gathers block deletion requests, then handles them during close.
 */
public interface BlockDeletionContext extends Closeable {
  /**
   * @param blockIds the blocks to be deleted when the context closes
   */
  default void registerBlocksForDeletion(Collection<Long> blockIds) {
    blockIds.forEach(this::registerBlockForDeletion);
  }

  /**
   * @param blockId the block to be deleted when the context closes
   */
  void registerBlockForDeletion(long blockId);

  /**
   * Interface for block deletion listeners.
   */
  @FunctionalInterface
  interface BlockDeletionListener {
    /**
     * Processes block deletion.
     *
     * @param blocks the deleted blocks
     */
    void process(List<Long> blocks) throws IOException;
  }
}
