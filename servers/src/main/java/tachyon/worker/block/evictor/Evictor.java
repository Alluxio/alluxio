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

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;

/**
 * Interface for the eviction policy in Tachyon
 */
public interface Evictor {

  class Factory {
    /**
     *
     * @param conf TachyonConf to determine the evictor type
     * @param view BlockMetadataManagerView to pass to Evictor
     * @return the generated Evictor
     */
    public static Evictor createEvictor(TachyonConf conf, BlockMetadataManagerView view) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.getClass(Constants.WORKER_EVICT_STRATEGY_CLASS, LRUEvictor.class),
            new Class[]{BlockMetadataManagerView.class},
            new Object[]{view});
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Frees space in the given block store location and with the given view.
   * After eviction, at least one StorageDir in the location
   * has the specific amount of free space after eviction. The location can be a specific
   * StorageDir, or {@link BlockStoreLocation#anyTier} or {@link BlockStoreLocation#anyDirInTier}.
   * The view is generated and passed by the calling {@link tachyon.worker.block.BlockStore}.
   *
   * <P>
   * This method returns null if Evictor fails to propose a feasible plan to meet the requirement,
   * or an eviction plan with toMove and toEvict fields to indicate how to free space. If both
   * toMove and toEvict of the plan are empty, it indicates that Evictor has no actions to take and
   * the requirement is already met.
   *
   * @param availableBytes the amount of free space in bytes to be ensured after eviction
   * @param location the location in block store
   * @param view generated and passed by block store
   * @return an eviction plan (possibly with empty fields) to get the free space, or null if no plan
   *         is feasible
   * @throws IllegalArgumentException if given block location is invalid
   */
  EvictionPlan freeSpaceWithView(final long availableBytes, final BlockStoreLocation location,
      final BlockMetadataManagerView view);
}
