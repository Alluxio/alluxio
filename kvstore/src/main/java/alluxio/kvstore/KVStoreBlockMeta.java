package alluxio.kvstore;

import alluxio.proto.kvstore.BlockLocationKey;
import alluxio.proto.kvstore.BlockLocationValue;
import alluxio.resource.CloseableIterator;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public interface KVStoreBlockMeta {
  /**
   * @param id a block id
   * @return the block's metadata, or empty if the block does not exist
   */
  Optional<BlockLocationValue> getBlock(BlockLocationKey id);

  /**
   * Adds block metadata to the block store. If the block already exists, its metadata will be
   * updated to the given metadata.
   *
   * @param id the block id
   * @param meta the block metadata
   */
  void putBlock(BlockLocationKey id, BlockLocationValue meta);

  /**
   * Removes a block, or does nothing if the block does not exist.
   *
   * @param id a block id to remove
   */
  void removeBlock(BlockLocationKey id);

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
  List<BlockLocationValue> getLocations(BlockLocationKey id);

  /**
   * Adds a new block location. If the location already exists, this method is a no-op.
   *
   * @param id a block id
   * @param location a block location
   */
  void addLocation(BlockLocationKey id, BlockLocationValue location);

  /**
   * Removes a block location. If the location doesn't exist, this method is a no-op.
   *
   * @param blockId  a block id
   */
  void removeLocation(BlockLocationKey blockId);

  /**
   * Closes the block store and releases all resources.
   */
  void close();

  /**
   * @return size of the block store
   */
  long size();

  List<org.tikv.kvproto.Kvrpcpb.KvPair> scan(byte[] keyStart, int limit);

  /**
   * Factory for creating block stores.
   */
  interface Factory extends Supplier<KVStoreBlockMeta> {}
}
