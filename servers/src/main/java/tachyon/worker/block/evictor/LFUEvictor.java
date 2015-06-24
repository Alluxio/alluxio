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

  /**
   * Double-Linked List
   *
   * "n" such as "1" represents access count, corresponding to class CountNode "o" represents class
   * Node, "-" represents bi-directional link
   *
   * 1 - o - o
   * |
   * 2 - o
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
    long count;
    Node first, last;

    CountNode() {
      count = 0;
      prev = next = null;
      first = last = null;
    }

    CountNode(long count) {
      this.count = count;
      prev = next = null;
      first = last = null;
    }

    void appendNode(Node node) {
      if (first == null) {
        first = node;
        node.prev = first;
      } else {
        last.append(node);
        last = node;
      }
      last = node;
    }

    CountNode nextCountNode() {
      return (CountNode)next;
    }
  }
  class Node extends DoubleLinkListNode {
    long blockId;
    CountNode head;

    Node() {
      blockId = -1;
      prev = next = null;
      head = null;
    }

    Node(long id) {
      blockId = id;
      prev = next = null;
      head = null;
    }

    // assume next layer exists and have elements other than the count node
    void appendToNextLayer() {
      remove();
      nextLayerHead().appendNode(this);
    }

    long count() {
      return head.count;
    }

    Node first() {
      return head.first;
    }

    CountNode nextLayerHead() {
      return head.nextCountNode();
    }

    Node nextNode() {
      return (Node)next;
    }
  }

  /** Always be the head of the first layer */
  private CountNode mHead;
  /** Map from blockId to corresponding Node */
  private Map<Long, Node> mCache;

  private final BlockMetadataManager mMeta;

  public LFUEvictor(BlockMetadataManager meta) {
    mMeta = meta;

    mHead = new CountNode();
    mCache = new HashMap<Long, Node>();
  }

  /** Complexity: O(1) */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    synchronized (mMeta) {
      if (mCache.containsKey(blockId)) {
        Node node = mCache.get(blockId);
        if (node.count() == Long.MAX_VALUE) {
          return;
        }
        if (node.nextLayerHead() == null && node.first() == node) { // only Node in last layer
          node.head.count += 1;
        } else {
          if (node.nextLayerHead() == null || node.nextLayerHead().count != node.count() + 1) {
            // create new layer
            node.head.append(new CountNode(node.count() + 1));
          }
          node.appendToNextLayer();
        }
      } else {
        mHead.appendNode(new Node(blockId));
      }
    }
  }

  /**
   * Evict layer by layer, in each layer, evict from first to last
   */
  @Override
  public EvictionPlan freeSpace(long bytes, BlockStoreLocation location) {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();

    synchronized (mMeta) {
      long evictBytes = 0;
      CountNode head = mHead;
      // layer by layer
      while (evictBytes < bytes && head != null) {
        Node p = head.first;
        // in each layer, left to right
        while (evictBytes < bytes && p != null) {
          Node next = p.nextNode();
          boolean remove = false;

          try {
            BlockMeta meta = mMeta.getBlockMeta(p.blockId);
            if (meta.getBlockLocation().belongTo(location)) {
              toEvict.add(p.blockId);
              evictBytes += meta.getBlockSize();
              remove = true;
            }
          } catch (IOException ioe) {
            LOG.warn("Remove block %d because %s", p.blockId, ioe);
            remove = true;
          }

          if (remove) {
            p.remove();
            mCache.remove(p.blockId);
          }

          // go to next right node
          p = next;
        }
        // go to next layer
        head = head.nextCountNode();
      }
    }

    return new EvictionPlan(toMove, toEvict);
  }
}
