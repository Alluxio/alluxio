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

package alluxio.util.proto;

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.proto.meta.Block.BlockLocation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * An util class to create cached grpc block locations.
 */
public class BlockLocationUtils {
  private static final IndexDefinition<BlockLocation, BlockLocation> OBJECT_INDEX =
      IndexDefinition.ofUnique((b) -> b);

  private static final IndexDefinition<BlockLocation, Long> WORKER_ID_INDEX =
      IndexDefinition.ofNonUnique(BlockLocation::getWorkerId);

  private static final IndexedSet<BlockLocation> BLOCK_LOCATION_CACHE =
      new IndexedSet<>(OBJECT_INDEX, WORKER_ID_INDEX);

  private static final Set<String> VALID_MEDIUM_TYPE_VALUES =
      Sets.newHashSet("MEM", "HDD", "SSD");

  /**
   * Get a shared grpc block location object. If it does not exist, create and cache it.
   * Because the valid values of tierAlias and mediumType are only MEM, SSD and HDD,
   * The size of the cache map is limited.
   *
   * @param workerId the worker id
   * @param tierAlias the tier alias
   * @param mediumType the medium type
   * @return a shared block location object from the cache
   */
  public static BlockLocation getCached(
      long workerId, String tierAlias, String mediumType) {
    BlockLocation location = BlockLocation
        .newBuilder()
        .setWorkerId(workerId)
        .setTier(tierAlias)
        .setMediumType(mediumType)
        .build();
    return getCached(location);
  }

  /**
   * Get a shared grpc block location object. If it does not exist, create and cache it.
   * Because the valid values of tierAlias and mediumType are only MEM, SSD and HDD,
   * The size of the cache map is limited.
   *
   * @param blockLocation the block location to cache
   * @return a shared block location object from the cache
   */
  public static BlockLocation getCached(BlockLocation blockLocation) {
    Preconditions.checkState(VALID_MEDIUM_TYPE_VALUES.contains(blockLocation.getTier()),
        "TierAlias must be one of {MEM, HDD and SSD} but got %s",
        blockLocation.getTier());
    Preconditions.checkState(VALID_MEDIUM_TYPE_VALUES.contains(blockLocation.getMediumType()),
        "MediumType must be one of {MEM, HDD and SSD} but got %s",
        blockLocation.getMediumType());
    BLOCK_LOCATION_CACHE.add(blockLocation);
    return BLOCK_LOCATION_CACHE.getFirstByField(OBJECT_INDEX, blockLocation);
  }

  /**
   * Evict cache entries by worker id.
   * @param workerId the worker id
   */
  public static void evictByWorkerId(long workerId) {
    BLOCK_LOCATION_CACHE.removeByField(WORKER_ID_INDEX, workerId);
  }

  /**
   * Gets the cached block location size.
   * @return the cached block location size
   */
  public static int getCachedBlockLocationSize() {
    return BLOCK_LOCATION_CACHE.size();
  }
}
