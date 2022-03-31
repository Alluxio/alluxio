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

package alluxio.master.metastore.rocks;

import alluxio.collections.TwoKeyConcurrentMap;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.meta.Block.BlockLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Block store backed by RocksDB and HEAP.
 */
@ThreadSafe
public class RocksBlockMetaStoreMetaOnly extends RocksBlockMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(RocksBlockMetaStoreMetaOnly.class);
  // Map from block id to block locations.
  public final TwoKeyConcurrentMap<Long, Long, BlockLocation, Map<Long, BlockLocation>>
      mBlockLocations = new TwoKeyConcurrentMap<>(() -> new HashMap<>(4));

  /**
   * Creates and initializes a rocks block store.
   *
   * @param baseDir the base directory in which to store block store metadata
   */
  public RocksBlockMetaStoreMetaOnly(String baseDir) {
    super(baseDir);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_BLOCK_LOCATIONS_COUNT.getName(),
        mBlockLocations::size);
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
