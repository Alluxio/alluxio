/**
 * Set of evictors for evicting or moving blocks to other locations.
 *
 * The main entry point is {@link tachyon.worker.block.evictor.Evictor} which returns an evictor by
 * calling tachyon.worker.block.Evictor.Factory#createEvictor.
 *
 * <h2>Evictor</h2>
 *
 * All evictors implement {@link tachyon.worker.block.evictor.Evictor} interface. In order to evict
 * blocks by different policies, each evictor only need to implement
 * {@link tachyon.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * tachyon.worker.block.BlockStoreLocation, tachyon.worker.block.BlockMetadataManagerView)}
 * method. When this method is called, blocks will be evicted or moved if no enough space left.
 *
 * <h3>Cascading Eviction</h3> Cascading Eviction means evict blocks recursively and is only called
 * in {@link tachyon.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * tachyon.worker.block.BlockStoreLocation, tachyon.worker.block.BlockMetadataManagerView)}
 * method. Cascading Eviction will try to free space in next tier view where blocks need to be
 * transferred to, if the next tier view does not have enough free space to hold the blocks, the
 * next next tier view will be tried and so on until the bottom tier is reached, if blocks can not
 * even be transferred to the bottom tier, they will be evicted, otherwise, only blocks to be freed
 * in the bottom tier will be evicted.
 *
 * For example,
 * {@link tachyon.worker.block.evictor.LRUEvictor#cascadingEvict(long,
 * tachyon.worker.block.BlockStoreLocation,EvictionPlan)}
 * is an implementation of Cascading Eviction.
 *
 * <h3>Eviction Plan</h3>
 *
 * Eviction plan will be returned when
 * {@link tachyon.worker.block.evictor.Evictor#freeSpaceWithView(long,
 * tachyon.worker.block.BlockStoreLocation, tachyon.worker.block.BlockMetadataManagerView)}
 * is called. The eviction plan will return two block lists, one is the blocks to be removed
 * directly and another is the blocks to be moved to lower tier views.
 */
package tachyon.worker.block.evictor;

