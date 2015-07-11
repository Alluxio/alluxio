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
   * Checks whether the plan is legal for an evictor with cascading eviction feature like
   * {@link LRUEvictor}. The plan is legal when the requested space can be satisfied, and there is
   * enough space in lower tier to move in blocks from upper tier.
   *
   * @param bytesToBeAvailable requested bytes to be available after eviction
   * @param plan the eviction plan
   * @param metaManager the meta data manager
   * @return true if the above requirements are satisfied, otherwise false.
   */
  public static boolean legalCascadingPlan(long bytesToBeAvailable, EvictionPlan plan,
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
        spaceInfoInDir.put(dir, spaceInfo);
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
        spaceInfoInDir.put(srcDir, spaceInfo);
      } else {
        spaceInfoInDir
            .put(srcDir, new Pair<Long, Long>(srcDir.getAvailableBytes() + blockSize, 0L));
      }

      if (spaceInfoInDir.containsKey(destDir)) {
        Pair<Long, Long> spaceInfo = spaceInfoInDir.get(destDir);
        spaceInfo.setSecond(spaceInfo.getSecond() + blockSize);
        spaceInfoInDir.put(destDir, spaceInfo);
      } else {
        spaceInfoInDir.put(destDir, new Pair<Long, Long>(destDir.getAvailableBytes(), blockSize));
      }
    }

    StorageDir firstTierDir = null;
    for (StorageDir dir : spaceInfoInDir.keySet()) {
      if (firstTierDir == null
          || dir.getParentTier().getTierAlias() < firstTierDir.getParentTier().getTierAlias()) {
        firstTierDir = dir;
      }
    }
    Pair<Long, Long> firstTierDirSpace = spaceInfoInDir.get(firstTierDir);
    if (firstTierDirSpace.getFirst() - firstTierDirSpace.getSecond() < bytesToBeAvailable) {
      return false;
    }

    for (StorageDir dir : spaceInfoInDir.keySet()) {
      Pair<Long, Long> spaceInfo = spaceInfoInDir.get(dir);
      if (spaceInfo.getFirst() < spaceInfo.getSecond()) {
        return false;
      }
    }

    return true;
  }
}
