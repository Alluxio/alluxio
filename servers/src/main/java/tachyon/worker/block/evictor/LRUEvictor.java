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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreEventListenerBase;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;

public class LRUEvictor extends BlockStoreEventListenerBase implements Evictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMeta;

  /**
   * access-ordered {@link java.util.LinkedHashMap} from blockId to {@code true}, acts as a LRU
   * double linked list where most recently accessed element is put at the tail while least recently
   * accessed element is put at the head
   */
  private Map<Long, Boolean> mLRUCache = Collections
      .synchronizedMap(new LinkedHashMap<Long, Boolean>(200, 0.75f, true));

  public LRUEvictor(BlockMetadataManager meta) {
    mMeta = meta;
  }

  @Override
  public EvictionPlan freeSpace(long availableBytes, BlockStoreLocation location)
      throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = null;

    long alreadyAvailableBytes = mMeta.getAvailableBytes(location);
    if (alreadyAvailableBytes >= availableBytes) {
      plan = new EvictionPlan(toMove, toEvict);
      return plan;
    }
    long toEvictBytes = availableBytes - alreadyAvailableBytes;

    EvictionDirCandidates dirCandidates = new EvictionDirCandidates();

    Iterator<Map.Entry<Long, Boolean>> it = mLRUCache.entrySet().iterator();
    while (it.hasNext() && dirCandidates.maxAvailableBytes() < toEvictBytes) {
      long blockId = it.next().getKey();

      try {
        BlockMeta meta = mMeta.getBlockMeta(blockId);

        BlockStoreLocation dir = meta.getBlockLocation();
        if (dir.belongTo(location)) {
          dirCandidates.add(dir, meta.getBlockId(), meta.getBlockSize());
        }
      } catch (IOException ioe) {
        LOG.warn("Remove block %d from LRU Cache because %s", blockId, ioe);
        it.remove();
      }
    }

    // enough free space
    if (dirCandidates.maxAvailableBytes() >= toEvictBytes) {
      toEvict = dirCandidates.toEvict();
      for (Long blockId : toEvict) {
        mLRUCache.remove(blockId);
      }
      plan = new EvictionPlan(toMove, toEvict);
    }

    return plan;
  }

  /**
   * Thread safe
   */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    if (mLRUCache.containsKey(blockId)) {
      mLRUCache.get(blockId);
    } else {
      mLRUCache.put(blockId, true);
    }
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    // Since the temp block has been committed, update Evictor about the new added blocks
    if (mLRUCache.containsKey(blockId)) {
      mLRUCache.get(blockId);
    } else {
      mLRUCache.put(blockId, true);
    }
  }
}
