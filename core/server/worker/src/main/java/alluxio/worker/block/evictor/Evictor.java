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

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;

import java.lang.RuntimeException;
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

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link Evictor}.
     *
     * @param view {@link BlockMetadataManagerView} to pass to {@link Evictor}
     * @param allocator an allocation policy
     * @return the generated {@link Evictor}
     */
    public static Evictor create(BlockMetadataManagerView view, Allocator allocator) {
      try {
        return CommonUtils.createNewClassInstance(
            Configuration.<Evictor>getClass(PropertyKey.WORKER_EVICTOR_CLASS),
            new Class[]{BlockMetadataManagerView.class, Allocator.class},
            new Object[]{view, allocator});
      } catch (Exception e) {
        throw new RuntimeException(e);
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
