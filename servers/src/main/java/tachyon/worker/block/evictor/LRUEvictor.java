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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;

import tachyon.Pair;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.BlockAccessEventListener;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.meta.BlockMeta;

public class LRUEvictor implements Evictor, BlockAccessEventListener {
  protected BlockMetadataManager mMeta;

  /** Double-Link List, most recently accessed block is at tail of the list */
  private class Node {
    long blockId;
    Node prev, next;
    Node() {
      blockId = -1;
      prev = next = null;
    }
    Node(long blkId) {
      blockId = blkId;
      prev = next = null;
    }

    void append(Node node) {
      node.next = next;
      node.prev = this;
      next = node;
    }

    // Remove itself from the List
    void remove() {
      if (prev != null) {
        prev.next = next;
      }
      if (next != null) {
        next.prev = prev;
      }
      next = prev = null;
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

  /**
   * Free space in the given block store location. The location can be a specific location, or
   * {@link BlockStoreLocation#anyTier()} or {@link BlockStoreLocation#anyDirInTier(int)} .
   *
   * If the total free space is fewer than {@param bytes}, they will all be evicted
   *
   * Thread safe.
   *
   * @param bytes the size in bytes
   * @param location the location in block store
   * @return an eviction plan to achieve the freed space
   */
  @Override
  public EvictionPlan freeSpace(long bytes, BlockStoreLocation location) {
    List<Pair<Long, BlockStoreLocation>> toMove = new ArrayList<Pair<Long, BlockStoreLocation>>();
    List<Long> toEvict = new ArrayList<Long>();

    Node p = mHead;
    long evictBytes = 0;
    // erase race condition with onAccessBlock on internal data structure
    synchronized (mTail) {
      while (p.next != mTail && evictBytes < bytes) {
        Optional<BlockMeta> meta = mMeta.getBlockMeta(p.blockId);
        if (!meta.isPresent() || !meta.get().getBlockLocation().belongTo(location)) {
          continue;
        }

        evictBytes += meta.get().getBlockSize();
        toEvict.add(p.blockId);

        mCache.remove(p.blockId);

        Node current = p;
        p = p.next;
        current.remove();
      }
    }

    return new EvictionPlan(toMove, toEvict);
  }

  /**
   * Thread safe
   */
  @Override
  public void onAccessBlock(long userId, long blockId) {
    Node node;
    synchronized (mTail) {
      if (mCache.containsKey(blockId)) {
        node = mCache.get(blockId);
        node.remove();
      } else {
        node = new Node(blockId);
        mCache.put(blockId, node);
      }
      mTail.prev.append(node);
    }
  }
}
