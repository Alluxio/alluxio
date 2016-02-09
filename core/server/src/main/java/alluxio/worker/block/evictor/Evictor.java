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

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;

import com.google.common.base.Throwables;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface for the eviction policy in Alluxio.
 */
@PublicApi
public interface Evictor {

  /**
   * Factory for {@link Evictor}.
   */
  @ThreadSafe
  class Factory {
    /**
     * Factory for {@link Evictor}.
     *
     * @param conf {@link Configuration} to determine the {@link Evictor} type
     * @param view {@link BlockMetadataManagerView} to pass to {@link Evictor}
     * @param allocator an allocation policy
     * @return the generated {@link Evictor}
     */
    public static Evictor create(Configuration conf, BlockMetadataManagerView view,
                                 Allocator allocator) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<Evictor>getClass(Constants.WORKER_EVICTOR_CLASS),
            new Class[]{BlockMetadataManagerView.class, Allocator.class},
            new Object[]{view, allocator});
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Frees space in the given block store location and with the given view. After eviction, at least
   * one {@link alluxio.worker.block.meta.StorageDir} in the location has the specific amount of
   * free space after eviction. The location can be a specific
   * {@link alluxio.worker.block.meta.StorageDir}, or {@link BlockStoreLocation#anyTier()} or
   * {@link BlockStoreLocation#anyDirInTier(String)}. The view is generated and passed by the
   * calling {@link alluxio.worker.block.BlockStore}.
   * <p>
   * This method returns null if {@link Evictor} fails to propose a feasible plan to meet the
   * requirement, or an eviction plan with toMove and toEvict fields to indicate how to free space.
   * If both toMove and toEvict of the plan are empty, it indicates that {@link Evictor} has no
   * actions to take and the requirement is already met.
   *
   * Throws an {@link IllegalArgumentException} if the given block location is invalid.
   *
   * @param availableBytes the amount of free space in bytes to be ensured after eviction
   * @param location the location in block store
   * @param view generated and passed by block store
   * @return an {@link EvictionPlan} (possibly with empty fields) to get the free space, or null if
   *         no plan is feasible
   */
  EvictionPlan freeSpaceWithView(long availableBytes, BlockStoreLocation location,
      BlockMetadataManagerView view);
}
