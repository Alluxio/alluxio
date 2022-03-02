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

package alluxio.master.metastore;

import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The block store keeps track of block sizes and block locations.
 */
@ThreadSafe
public interface BlockStore {
  /**
   * @param id a block id
   * @return the block's metadata, or empty if the block does not exist
   */
  Optional<BlockMeta> getBlock(long id);

  /**
   * Adds block metadata to the block store. If the block already exists, its metadata will be
   * updated to the given metadata.
   *
   * @param id the block id
   * @param meta the block metadata
   */
  void putBlock(long id, BlockMeta meta);

  /**
   * Removes a block, or does nothing if the block does not exist.
   *
   * @param id a block id to remove
   */
  void removeBlock(long id);

  /**
   * Removes all metadata from the block store.
   */
  void clear();

  /**
   * Gets locations for a block. If the block does not exist or has no locations, an empty list is
   * returned.
   *
   * @param id a block id
   * @return the locations of the block
   */
  List<BlockLocation> getLocations(long id);

  /**
   * Adds a new block location. If the location already exists, this method is a no-op.
   *
   * @param id a block id
   * @param location a block location
   */
  void addLocation(long id, BlockLocation location);

  /**
   * Removes a block location. If the location doesn't exist, this method is a no-op.
   *
   * @param blockId  a block id
   * @param workerId a worker id
   */
  void removeLocation(long blockId, long workerId);

  /**
   * Closes the block store and releases all resources.
   */
  void close();

  /**
   * @return size of the block store
   */
  long size();

  /**
   * Gets a {@link CloseableIterator} over the blocks.
   * The iterator must be closed properly.
   * One option is to follow the below idiom:
   * <pre>{@code
   *   try (CloseableIterator<Block> iter =
   *       mBlockStore.getCloseableIterator()) {
   *     while (iter.hasNext()) {
   *       // take the element and perform operations
   *     }
   *   }
   * }</pre>
   *
   * If the iterator must be passed to other methods,
   * it must be closed at the end of operation or on exceptions.
   * Otherwise there can be a leak!
   *
   * @return a {@link CloseableIterator} over the blocks
   * */
  CloseableIterator<Block> getCloseableIterator();

  /**
   * Block metadata.
   */
  class Block {
    private final long mId;
    private final BlockMeta mMeta;

    /**
     * @param id the block id
     * @param meta the block meta
     */
    public Block(long id, BlockMeta meta) {
      mId = id;
      mMeta = meta;
    }

    /**
     * @return id
     */
    public long getId() {
      return mId;
    }

    /**
     * @return block meta
     */
    public BlockMeta getMeta() {
      return mMeta;
    }
  }

  /**
   * Factory for creating block stores.
   */
  interface Factory extends Supplier<BlockStore> {}
}
