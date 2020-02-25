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

package alluxio.worker.block.io;

import alluxio.worker.block.BlockStoreLocation;

/**
 * An interface to listen for block stream events.
 */
public interface BlockStreamListener {
  /**
   * Called with a new block reader.
   *
   * @param reader block reader
   * @param location location of read
   */
  void readerOpened(BlockReader reader, BlockStoreLocation location);

  /**
   * Called when an existing block reader is closed.
   *
   * @param reader block reader
   * @param location location of read
   */
  void readerClosed(BlockReader reader, BlockStoreLocation location);

  /**
   * Called with a new block writer.
   *
   * @param writer block writer
   * @param location location of write
   */
  void writerOpened(BlockWriter writer, BlockStoreLocation location);

  /**
   * Called when an existing block writer is closed.
   *
   * @param writer block writer
   * @param location location of write
   */
  void writerClosed(BlockWriter writer, BlockStoreLocation location);
}
