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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.BlockAccessEventListener;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.meta.BlockMeta;

public class LRUEvictor implements Evictor, BlockAccessEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMeta;
  private final Object mLock = new Object();

  /** Double-Link List, most recently accessed block is at tail of the list */
  private class Node extends DoubleLinkListNode {
    long mBlockId;

    Node() {
      super();
      mBlockId = -1;
    }

    Node(long blkId) {
      super();
      mBlockId = blkId;
    }

    Node next() {
      return (Node) mNext;
    }

    Node prev() {
      return (Node) mPrev;
    }
  }

  private Node mHead;
  private Node mTail;
  /** Map from blockId to corresponding Node in the Double-Link List */
  private Map<Long, Node> mCache;

  public LRUEvictor(BlockMetadataManager meta) {
    mMeta = meta;

    mHead = new Node();
    mTail = new Node();
    mHead.append(mTail);
    mCache = new HashMap<Long, Node>();
  }

  @Override
  public EvictionPlan freeSpace(long bytes, BlockStoreLocation location) throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = null;

    if (bytes <= 0) {
      plan = new EvictionPlan(toMove, toEvict);
      return plan;
    }

    EvictionCandidates dirCandidates = new EvictionCandidates();

    Node p = mHead.next();
    // erase race condition with onAccessBlock on internal data structure
    synchronized (mLock) {
      while (p != mTail && dirCandidates.maxAvailableBytes() < bytes) {
        Node next = p.next();

        try {
          BlockMeta meta = mMeta.getBlockMeta(p.mBlockId);

          BlockStoreLocation dir = meta.getBlockLocation();
          if (dir.belongTo(location)) {
            dirCandidates.add(dir, meta.getBlockId(), meta.getBlockSize());
          }
        } catch (IOException ioe) {
          LOG.warn("Remove block %d from LRU Cache because %s", p.mBlockId, ioe);
          removeNode(p);
        }

        p = next;
      }

      // enough free space
      if (dirCandidates.maxAvailableBytes() >= bytes) {
        toEvict = dirCandidates.toEvict();
        for (Long blockId : toEvict) {
          removeNode(mCache.get(blockId));
        }
        plan = new EvictionPlan(toMove, toEvict);
      }
    }

    return plan;
  }

  /**
   * Thread safe
   */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    Node node;
    synchronized (mLock) {
      if (mCache.containsKey(blockId)) {
        node = mCache.get(blockId);
        node.remove();
      } else {
        node = new Node(blockId);
        mCache.put(blockId, node);
      }
      mTail.prev().append(node);
    }
  }

  private void removeNode(Node p) {
    p.remove();
    mCache.remove(p.mBlockId);
  }
}
