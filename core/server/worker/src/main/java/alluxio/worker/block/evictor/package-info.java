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

/**
 * Set of evictors for evicting or moving blocks to other locations.
 *
 * The main entry point is {@link alluxio.worker.block.evictor.Evictor} which returns an evictor by
 * calling alluxio.worker.block.Evictor.Factory#create.
 *
 * <h2>Evictor</h2>
 *
 * All evictors implement {@link alluxio.worker.block.evictor.Evictor} interface. In order to evict
 * blocks by different policies, each evictor only need to implement
 * {@link alluxio.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * alluxio.worker.block.BlockStoreLocation, alluxio.worker.block.BlockMetadataEvictorView)}
 * method. When this method is called, blocks will be evicted or moved if no enough space left.
 *
 * <h3>Cascading Eviction</h3> Cascading Eviction means evict blocks recursively and is only called
 * in {@link alluxio.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * alluxio.worker.block.BlockStoreLocation, alluxio.worker.block.BlockMetadataEvictorView)}
 * method. Cascading Eviction will try to free space in next tier view where blocks need to be
 * transferred to, if the next tier view does not have enough free space to hold the blocks, the
 * next next tier view will be tried and so on until the bottom tier is reached, if blocks can not
 * even be transferred to the bottom tier, they will be evicted, otherwise, only blocks to be freed
 * in the bottom tier will be evicted.
 *
 * For example,
 * {@link alluxio.worker.block.evictor.LRUEvictor#cascadingEvict(long,
 * alluxio.worker.block.BlockStoreLocation,EvictionPlan)}
 * is an implementation of Cascading Eviction.
 *
 * <h3>Eviction Plan</h3>
 *
 * Eviction plan will be returned when
 * {@link alluxio.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * alluxio.worker.block.BlockStoreLocation, alluxio.worker.block.BlockMetadataEvictorView)}
 * is called. The eviction plan will return two block lists, one is the blocks to be removed
 * directly and another is the blocks to be moved to lower tier views.
 */

package alluxio.worker.block.evictor;
