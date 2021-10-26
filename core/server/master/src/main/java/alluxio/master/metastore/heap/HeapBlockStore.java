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

package alluxio.master.metastore.heap;

import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.metastore.BlockStore;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.util.ObjectSizeCalculator;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class requires external synchronization for operations on the same block id. Operations on
 * different block ids can be performed concurrently.
 */
@ThreadSafe
public class HeapBlockStore implements BlockStore {
  // Map from block id to block metadata.
  public final Map<Long, BlockMeta> mBlocks = new ConcurrentHashMap<>();
  // Map from block id to block locations.
  public final TwoKeyConcurrentMap<Long, Long, BlockLocation, Map<Long, BlockLocation>>
      mBlockLocations = new TwoKeyConcurrentMap<>(() -> new HashMap<>(4));

  /**
   * constructor a HeapBlockStore.
   *
   */
  public HeapBlockStore() {
    super();
    if (ServerConfiguration.getBoolean(PropertyKey.MASTER_METRICS_HEAP_ENABLED)) {
      MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_BLOCK_HEAP_SIZE.getName(),
          () -> ObjectSizeCalculator.getObjectSize(mBlocks,
              ImmutableSet.of(Long.class, BlockMeta.class)));
    }
  }

  @Override
  public Optional<BlockMeta> getBlock(long id) {
    return Optional.ofNullable(mBlocks.get(id));
  }

  @Override
  public void putBlock(long id, BlockMeta meta) {
    mBlocks.put(id, meta);
  }

  @Override
  public void removeBlock(long id) {
    mBlocks.remove(id);
  }

  @Override
  public Iterator<Block> iterator() {
    Iterator<Entry<Long, BlockMeta>> it = mBlocks.entrySet().iterator();
    return new Iterator<Block>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Block next() {
        Entry<Long, BlockMeta> entry = it.next();
        return new Block(entry.getKey(), entry.getValue());
      }
    };
  }

  @Override
  public void clear() {
    mBlocks.clear();
  }

  @Override
  public void close() {
    // Nothing to close for HEAP store.
  }

  @Override
  public long size() {
    return mBlocks.size();
  }

  @Override
  public List<BlockLocation> getLocations(long blockid) {
    if (!mBlockLocations.containsKey(blockid)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(mBlockLocations.get(blockid).values());
  }

  @Override
  public void addLocation(long blockId, BlockLocation location) {
    mBlockLocations.addInnerValue(blockId, location.getWorkerId(), location);
  }

  @Override
  public void removeLocation(long blockId, long workerId) {
    mBlockLocations.removeInnerValue(blockId, workerId);
  }
}
