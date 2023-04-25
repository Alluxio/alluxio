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
 * An interface to listen for creation and destruction of block clients.
 */
public interface BlockClientListener {

  /**
   * Called when a new block client is opened.
   *
   * @param blockClient the block read/write client
   * @param location the block location
   */
  void clientOpened(BlockClient blockClient, BlockStoreLocation location);

  /**
   * Called when an open block client is closed.
   *
   * @param blockClient the block read/write client
   * @param location the block location
   */
  void clientClosed(BlockClient blockClient, BlockStoreLocation location);
}
