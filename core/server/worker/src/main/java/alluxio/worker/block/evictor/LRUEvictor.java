/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of an evictor which follows the least recently used algorithm. It discards the
 * least recently used item based on its access.
 *
 * @deprecated use block annotator instead
 */
@NotThreadSafe
@Deprecated
public class LRUEvictor extends AbstractEvictor {
  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  /**
   * Access-ordered {@link java.util.LinkedHashMap} from blockId to {@link #UNUSED_MAP_VALUE}(just a
   * placeholder to occupy the value), acts as a LRU double linked list where most recently accessed
   * element is put at the tail while least recently accessed element is put at the head.
   */
  protected Map<Long, Boolean> mLRUCache =
      Collections.synchronizedMap(new LinkedHashMap<Long, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  /**
   * Creates a new instance of {@link LRUEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LRUEvictor(BlockMetadataEvictorView view, Allocator allocator) {
    super(view, allocator);

    // preload existing blocks loaded by StorageDir to Evictor
    for (StorageTierView tierView : mMetadataView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        for (BlockMeta blockMeta : ((StorageDirEvictorView) dirView)
            .getEvictableBlocks()) { // all blocks with initial view
          mLRUCache.put(blockMeta.getBlockId(), UNUSED_MAP_VALUE);
        }
      }
    }
  }

  @Override
  protected Iterator<Long> getBlockIterator() {
    List<Long> blocks = new ArrayList<>(mLRUCache.keySet());
    return blocks.iterator();
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
    // Since the temp block has been committed, update Evictor about the new added blocks
    mLRUCache.put(blockId, UNUSED_MAP_VALUE);
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  public void onBlockLost(long blockId) {
    mLRUCache.remove(blockId);
  }

  @Override
  protected void onRemoveBlockFromIterator(long blockId) {
    mLRUCache.remove(blockId);
  }
}
