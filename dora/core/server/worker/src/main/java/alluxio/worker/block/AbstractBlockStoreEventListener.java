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

package alluxio.worker.block;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A block store event listener base class implementing the {@link BlockStoreEventListener}
 * interface with empty methods bodies.
 */
@NotThreadSafe
public abstract class AbstractBlockStoreEventListener implements BlockStoreEventListener {

  @Override
  public void onAccessBlock(long blockId) {}

  @Override
  public void onAccessBlock(long blockId, BlockStoreLocation location) {}

  @Override
  public void onAbortBlock(long blockId) {}

  @Override
  public void onCommitBlockToLocal(long blockId, BlockStoreLocation location) {}

  @Override
  public void onCommitBlockToMaster(long blockId, BlockStoreLocation location) {}

  @Override
  public void onMoveBlockByClient(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {}

  @Override
  public void onMoveBlockByWorker(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {}

  @Override
  public void onRemoveBlockByClient(long blockId) {}

  @Override
  public void onRemoveBlockByWorker(long blockId) {}

  @Override
  public void onRemoveBlock(long blockId, BlockStoreLocation location) {}

  @Override
  public void onBlockLost(long blockId) {}

  @Override
  public void onStorageLost(String tierAlias, String dirPath) {}

  @Override
  public void onStorageLost(BlockStoreLocation dirLocation) {}
}
