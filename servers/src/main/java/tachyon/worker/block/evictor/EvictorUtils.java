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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;

public class EvictorUtils {
  /**
   * Checks whether the plan of a cascading evictor is valid.
   *
   * A cascading evictor will try to free space by recursively moving blocks to next 1 tier and
   * evict blocks only in the bottom tier.
   *
   * The plan is invalid when the requested space can not be satisfied or lower level of tiers do
   * not have enough space to hold blocks moved from higher level of tiers.
   *
   * @param bytesToBeAvailable requested bytes to be available after eviction
   * @param plan the eviction plan, should not be empty
   * @param metaManager the meta data manager
   * @return true if the above requirements are satisfied, otherwise false.
   */
  // TODO: unit test this method
  public static boolean validCascadingPlan(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManager metaManager) throws IOException {
    // reassure the plan is feasible: enough free space to satisfy bytesToBeAvailable, and enough
    // space in lower tier to move blocks in upper tier there

    // Map from dir to a pair of bytes to be available in this dir and bytes to move into this dir
    // after the plan taking action
    Map<StorageDir, Pair<Long, Long>> spaceInfoInDir = new HashMap<StorageDir, Pair<Long, Long>>();

    for (long blockId : plan.toEvict()) {
      BlockMeta block = metaManager.getBlockMeta(blockId);
      StorageDir dir = block.getParentDir();
      if (spaceInfoInDir.containsKey(dir)) {
        Pair<Long, Long> spaceInfo = spaceInfoInDir.get(dir);
        spaceInfo.setFirst(spaceInfo.getFirst() + block.getBlockSize());
      } else {
        spaceInfoInDir.put(dir, new Pair<Long, Long>(
            dir.getAvailableBytes() + block.getBlockSize(), 0L));
      }
    }

    for (Pair<Long, BlockStoreLocation> move : plan.toMove()) {
      long blockId = move.getFirst();
      BlockMeta block = metaManager.getBlockMeta(blockId);
      long blockSize = block.getBlockSize();
      StorageDir srcDir = block.getParentDir();
      StorageDir destDir = metaManager.getDir(move.getSecond());

      if (spaceInfoInDir.containsKey(srcDir)) {
        Pair<Long, Long> spaceInfo = spaceInfoInDir.get(srcDir);
        spaceInfo.setFirst(spaceInfo.getFirst() + blockSize);
      } else {
        spaceInfoInDir
            .put(srcDir, new Pair<Long, Long>(srcDir.getAvailableBytes() + blockSize, 0L));
      }

      if (spaceInfoInDir.containsKey(destDir)) {
        Pair<Long, Long> spaceInfo = spaceInfoInDir.get(destDir);
        spaceInfo.setSecond(spaceInfo.getSecond() + blockSize);
      } else {
        spaceInfoInDir.put(destDir, new Pair<Long, Long>(destDir.getAvailableBytes(), blockSize));
      }
    }

    // the top tier among all tiers where blocks in the plan reside in
    int topTierAlias = Integer.MAX_VALUE;
    for (StorageDir dir : spaceInfoInDir.keySet()) {
      topTierAlias = Math.min(topTierAlias, dir.getParentTier().getTierAlias());
    }
    long maxSpace = Long.MIN_VALUE; // maximum bytes to be available in a dir in the top tier
    for (StorageDir dir : spaceInfoInDir.keySet()) {
      if (dir.getParentTier().getTierAlias() == topTierAlias) {
        Pair<Long, Long> space = spaceInfoInDir.get(dir);
        maxSpace = Math.max(maxSpace, space.getFirst() - space.getSecond());
      }
    }
    if (maxSpace < bytesToBeAvailable) {
      // plan is invalid because requested space can not be satisfied in the top tier
      return false;
    }

    for (StorageDir dir : spaceInfoInDir.keySet()) {
      Pair<Long, Long> spaceInfo = spaceInfoInDir.get(dir);
      if (spaceInfo.getFirst() < spaceInfo.getSecond()) {
        // plan is invalid because there is not enough space in this dir to hold the blocks waiting
        // to be moved into this dir
        return false;
      }
    }

    return true;
  }
}
