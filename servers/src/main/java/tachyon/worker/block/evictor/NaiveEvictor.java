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

import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.BlockMetadataManager;

/**
 * A simple evictor that evicts random blocks until the required size in the target tier is met.
 */
public class NaiveEvictor implements Evictor {
  private final BlockMetadataManager mMetadata;

  public NaiveEvictor(BlockMetadataManager metadata) {
    mMetadata = metadata;
  }

  @Override
  public EvictionPlan freeSpace(long bytes,  BlockStoreLocation location) {
    return new EvictionPlan(null, null);
  }

  @Override
  public void preCommitBlock(long userId, long blockId) {}

  @Override
  public void postCommitBlock(long userId, long blockId) {}

  @Override
  public void preMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {}

  @Override
  public void postMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {}

  @Override
  public void preRemoveBlock(long userId, long blockId) {}

  @Override
  public void postRemoveBlock(long userId, long blockId) {}
}
