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

/**
 * A block store event listener base class implementing the {@link BlockStoreEventListener}
 * interface with empty methods bodies.
 */
public class BlockStoreEventListenerBase implements BlockStoreEventListener {

  @Override
  public void onAccessBlock(long userId, long blockId) {}

  @Override
  public void onAbortBlock(long userId, long blockId) {}

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {}

  @Override
  public void onMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {}

  @Override
  public void onMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {}

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {}

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {}

}
