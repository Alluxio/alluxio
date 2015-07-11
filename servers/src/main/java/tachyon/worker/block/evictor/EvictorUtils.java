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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;

public class EvictorUtils {
  /**
   * Checks whether the plan is legal for an evictor with cascading eviction feature like
   * {@link LRUEvictor}. The plan is legal when the requested space can be satisfied, and there is
   * enough space in lower tier to move in blocks from upper tier.
   *
   * @param bytesToBeAvailable requested bytes to be available after eviction
   * @param plan the eviction plan
   * @param managerView  view of the meta data manager
   * @return true if the above requirements are satisfied, otherwise false.
   */
  public static boolean legalCascadingPlan(long bytesToBeAvailable, EvictionPlan plan,
      BlockMetadataManagerView managerView) throws IOException {
    // reassure the plan is feasible: enough free space to satisfy bytesToBeAvailable, and enough
    // space in lower tier to move blocks in upper tier there
    Map<Integer, Long> bytesToBeAvailableInTier =
        new HashMap<Integer, Long>(managerView.getTierViews().size());
    List<Integer> tierAliases = new ArrayList<Integer>();

    List<Long> blockIds = new ArrayList<Long>(plan.toEvict().size() + plan.toMove().size());
    blockIds.addAll(plan.toEvict());
    for (Pair<Long, BlockStoreLocation> move : plan.toMove()) {
      blockIds.add(move.getFirst());
    }

    for (long blockId : blockIds) {
      BlockMeta block = managerView.getBlockMeta(blockId);
      BlockStoreLocation blockDir = block.getBlockLocation();
      long blockSize = block.getBlockSize();
      int tierAlias = blockDir.tierAlias();
      if (bytesToBeAvailableInTier.containsKey(tierAlias)) {
        bytesToBeAvailableInTier
            .put(tierAlias, bytesToBeAvailableInTier.get(tierAlias) + blockSize);
      } else {
        tierAliases.add(tierAlias);
        bytesToBeAvailableInTier
            .put(tierAlias, managerView.getAvailableBytes(blockDir) + blockSize);
      }
    }

    // upper to lower tier
    Collections.sort(tierAliases);
    int currentTierAlias = tierAliases.get(0);
    // first tier to free space from needs to have bytesToBeAvailable after eviction
    boolean isLegal = bytesToBeAvailableInTier.get(currentTierAlias) >= bytesToBeAvailable;
    for (int nextTierAlias : tierAliases.subList(1, tierAliases.size())) {
      if (!isLegal) {
        break;
      }
      // next tier should have enough space to hold blocks to be transferred from current tier
      long nextTierFreeSpace = bytesToBeAvailableInTier.get(nextTierAlias);
      long bytesToTransfer = bytesToBeAvailableInTier.get(currentTierAlias);
      isLegal = isLegal && (nextTierFreeSpace >= bytesToTransfer);
      currentTierAlias = nextTierAlias;
    }
    return isLegal;
  }
}
