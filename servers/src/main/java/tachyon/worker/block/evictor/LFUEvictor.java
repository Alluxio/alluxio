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

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.BlockAccessEventListener;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.meta.BlockMeta;

public class LFUEvictor implements Evictor, BlockAccessEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMeta;
  private final Object mLock = new Object();

  /**
   * Layered Double-Linked List
   *
   * "n" such as "1" represents access count, corresponding to class CountNode.
   * "o" represents class Node.
   * "->" represents single direction link.
   * "-" represents bi-directional link.
   *
   * 1 -> o - o
   * |
   * 2 -> o
   * |
   * ...
   *
   * When a Node is accessed, move it to next layer, if next layer represents the same access count
   * with this node, append it to the end of next layer, otherwise, create a new next layer with the
   * Node as the only element
   *
   * If the access count arrives at limit of {@code long}, it will no longer be increased
   */
  class CountNode extends DoubleLinkListNode {
    long mCount;
    Node mFirst;
    Node mLast;

    CountNode() {
      super();
      mCount = 0;
      mFirst = mLast = null;
    }

    CountNode(long count) {
      super();
      mCount = count;
      mFirst = mLast = null;
    }

    void appendNode(Node node) {
      if (mFirst == null) {
        mFirst = node;
      } else {
        mLast.append(node);
      }
      mLast = node;
    }

    CountNode nextCountNode() {
      return (CountNode) mNext;
    }
  }

  class Node extends DoubleLinkListNode {
    long mBlockId;
    CountNode mHead;

    Node() {
      super();
      mBlockId = -1;
      mHead = null;
    }

    Node(long id) {
      super();
      mBlockId = id;
      mHead = null;
    }

    /** assume next layer exists and have elements other than the count node */
    void appendToNextLayer() {
      remove();
      nextLayerHead().appendNode(this);
    }

    long count() {
      return mHead.mCount;
    }

    Node first() {
      return mHead.mFirst;
    }

    CountNode nextLayerHead() {
      return mHead.nextCountNode();
    }

    Node nextNode() {
      return (Node) mNext;
    }
  }

  /** Always be the head of the first layer */
  private CountNode mHead;
  /** Map from blockId to corresponding Node */
  private Map<Long, Node> mCache;

  public LFUEvictor(BlockMetadataManager meta) {
    mMeta = meta;

    mHead = new CountNode();
    mCache = new HashMap<Long, Node>();
  }

  /** Complexity: O(1) */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    synchronized (mLock) {
      if (mCache.containsKey(blockId)) {
        Node node = mCache.get(blockId);
        if (node.count() == Long.MAX_VALUE) {
          return;
        }
        if (node.nextLayerHead() == null && node.first() == node) { // only Node in last layer
          node.mHead.mCount += 1;
        } else {
          // not the only Node in last layer
          // or count of next layer is not its current count plus 1
          if (node.nextLayerHead() == null || node.nextLayerHead().mCount != node.count() + 1) {
            // create new layer
            node.mHead.append(new CountNode(node.count() + 1));
          }
          node.appendToNextLayer();
        }
      } else {
        Node node = new Node(blockId);
        mHead.appendNode(node);
        mCache.put(blockId, node);
      }
    }
  }

  /**
   * Evict layer by layer, in each layer, evict from first to last
   */
  @Override
  public EvictionPlan freeSpace(long bytes, BlockStoreLocation location) throws IOException {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();
    EvictionPlan plan = null;

    if (bytes <= 0) {
      plan = new EvictionPlan(toMove, toEvict);
      return plan;
    }

    // map from directory to a pair of id of blocks to evict and total size of these blocks
    Map<BlockStoreLocation, Pair<List<Long>, Long>> dirCandidate =
        new HashMap<BlockStoreLocation, Pair<List<Long>, Long>>();
    long maxEvictBytes = 0;
    BlockStoreLocation dirWithMaxEvictBytes = null;

    synchronized (mLock) {
      CountNode head = mHead;
      // layer by layer
      while (maxEvictBytes < bytes && head != null) {
        Node p = head.mFirst;
        // in each layer, left to right
        while (maxEvictBytes < bytes && p != null) {
          Node next = p.nextNode();

          try {
            BlockMeta meta = mMeta.getBlockMeta(p.mBlockId);
            BlockStoreLocation dir = meta.getBlockLocation();
            if (dir.belongTo(location)) {
              Pair<List<Long>, Long> candidate;
              if (dirCandidate.containsKey(dir)) {
                candidate = dirCandidate.get(dir);
              } else {
                candidate = new Pair<List<Long>, Long>(new ArrayList<Long>(), 0L);
                dirCandidate.put(dir, candidate);
              }

              candidate.getFirst().add(meta.getBlockId());
              long evictBytes = candidate.getSecond() + meta.getBlockSize();
              candidate.setSecond(evictBytes);

              if (maxEvictBytes < evictBytes) {
                maxEvictBytes = evictBytes;
                dirWithMaxEvictBytes = dir;
              }
            }
          } catch (IOException ioe) {
            LOG.warn("Remove block %d because %s", p.mBlockId, ioe);
            removeNode(p);
          }

          // go to next right node
          p = next;
        }
        // go to next layer
        head = head.nextCountNode();
      }

      if (maxEvictBytes >= bytes) {
        toEvict = dirCandidate.get(dirWithMaxEvictBytes).getFirst();
        for (long blockId : toEvict) {
          removeNode(mCache.get(blockId));
        }
        plan = new EvictionPlan(toMove, toEvict);
      }
    }

    return plan;
  }

  private void removeNode(Node node) {
    node.remove();
    mCache.remove(node.mBlockId);
  }
}
