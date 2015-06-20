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

import com.google.common.base.Optional;

import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.BlockMetaEventListener;
import tachyon.worker.block.BlockMetadataManager;

/**
 * Interface for the eviction policy in Tachyon
 */
public interface Evictor {
  /**
   * Frees space in the given block store location to ensure a specific amount of free space
   * available. The location can be a specific location, or {@link BlockStoreLocation#anyTier()} or
   * {@link BlockStoreLocation#anyDirInTier(int)}. This operation returns absent if Evictor fails to
   * propose a feasible plan to meet its requirement, or an eviction plan (possibly an empty one) to
   * ensure the free space.
   *
   * @param availableBytes the size in bytes
   * @param location the location in block store
   * @return an eviction plan (possibly empty) to get the free space, or absent if no feasible plan
   */
  Optional<EvictionPlan> freeSpace(long availableBytes, BlockStoreLocation location);
}
