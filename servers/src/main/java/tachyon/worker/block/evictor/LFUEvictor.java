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

public class LFUEvictor implements Evictor, BlockAccessEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final BlockMetadataManager mMeta;
  private final Object mLock = new Object();

  /**
   * Layered Double-Linked List
   *
   * "n" such as "1" represents access count, corresponding to class CountNode.
   * 
   * "o" represents class Node, each "o" has a pointer to the first "n" in the same row.
   * 
   * "->" represents single direction link.
   * 
   * "-" represents bi-directional link.
   *
   * {@literal
   * 1 -> o - o
   * |
   * 2 -> o
   * |
   * ...
   * }
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
      node.mHead = this;
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

    long count() {
      return mHead.mCount;
    }

    Node first() {
      return mHead.mFirst;
    }

    Node last() {
      return mHead.mLast;
    }

    CountNode nextLayerHead() {
      return mHead.nextCountNode();
    }

    Node nextNode() {
      return (Node) mNext;
    }

    Node prevNode() {
      return (Node) mPrev;
    }
  }

  /** Always be the head of the first layer */
  private final CountNode mHead;
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
        // only Node in last but not first layer, just update count
        if (node.nextLayerHead() == null && node.first() == node && node.mHead != mHead) {
          node.mHead.mCount += 1;
        } else {
          // no next layer or
          // count of next layer is not its current count plus 1,
          if (node.nextLayerHead() == null || node.nextLayerHead().mCount != node.count() + 1) {
            // create new layer
            node.mHead.append(new CountNode(node.count() + 1));
          }
          // move to the last of next layer
          CountNode nextHead = node.nextLayerHead();
          removeNode(node);
          nextHead.appendNode(node);
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

    synchronized (mLock) {
      CountNode head = mHead;
      // layer by layer
      while (dirCandidates.maxAvailableBytes() < toEvictBytes && head != null) {
        Node p = head.mFirst;
        // in each layer, left to right
        while (dirCandidates.maxAvailableBytes() < toEvictBytes && p != null) {
          Node next = p.nextNode();

          try {
            BlockMeta meta = mMeta.getBlockMeta(p.mBlockId);
            BlockStoreLocation dir = meta.getBlockLocation();
            if (dir.belongTo(location)) {
              dirCandidates.add(dir, meta.getBlockId(), meta.getBlockSize());
            }
          } catch (IOException ioe) {
            LOG.warn("Remove block {} because {}", p.mBlockId, ioe);
            removeNode(p);
            mCache.remove(p.mBlockId);
          }

          // go to next right node
          p = next;
        }
        // go to next layer
        head = head.nextCountNode();
      }

      if (dirCandidates.maxAvailableBytes() >= toEvictBytes) {
        toEvict = dirCandidates.toEvict();
        for (long blockId : toEvict) {
          removeNode(mCache.get(blockId));
          mCache.remove(blockId);
        }
        plan = new EvictionPlan(toMove, toEvict);
      }
    }

    return plan;
  }

  /**
   * Remove {@code Node node} from LFU List. maintain relationship with other Nodes as well as the
   * CountNode in the same layer
   */
  private void removeNode(Node node) {
    if (node.first() == node) {
      node.mHead.mFirst = node.nextNode();
    }
    if (node.last() == node) {
      node.mHead.mLast = node.prevNode();
    }
    // not the first layer and contain no Node, delete this layer
    if (node.mHead.mFirst == null && node.mHead != mHead) {
      node.mHead.remove();
    }
    node.mHead = null;
    
    node.remove();
  }
}
