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

package alluxio.master.metastore.java;

import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.master.metastore.BlockStore;
import alluxio.proto.master.Block.BlockLocation;
import alluxio.proto.master.Block.BlockMeta;
import alluxio.resource.LockResource;
import alluxio.resource.LockedResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Block store implemented using java data structures.
 */
@ThreadSafe
public class JavaBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(JavaBlockStore.class);

  /** Blocks on all workers, including active and lost blocks. This state must be journaled. */
  private final ConcurrentHashMap<Long, BlockInfo> mBlocks =
      new ConcurrentHashMap<>(8192, 0.90f, 64);

  /// Read methods

  @Override
  public boolean contains(long id) {
    return mBlocks.containsKey(id);
  }

  @Override
  public BlockMeta getMeta(long id) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(id)) {
      return info.get().mBlockMeta;
    }
  }

  @Nullable
  @Override
  public BlockMeta getMetaOrNull(long id) {
    BlockInfo info = mBlocks.get(id);
    if (info == null) {
      return null;
    }
    try (LockResource lr = new LockResource(info.mLock)) {
      return info.mBlockMeta;
    }
  }

  @Override
  public Set<BlockLocation> getLocations(long id) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(id)) {
      return new HashSet<>(info.get().mBlockLocations.values());
    }
  }

  @Override
  public Iterator<Long> blockIds() {
    return mBlocks.keySet().iterator();
  }

  /// Write methods

  @Override
  public void newBlock(long id, BlockMeta blockMeta) throws AlreadyExistsException {
    if (mBlocks.putIfAbsent(id, new BlockInfo(blockMeta)) != null) {
      throw new AlreadyExistsException(String.format("Block %d already exists", id));
    }
  }

  @Override
  public void remove(long id) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(id)) {
      mBlocks.remove(id);
    }
  }

  @Override
  public void setBlockLength(long id, long length) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(id)) {
      info.get().mBlockMeta = BlockMeta.newBuilder().setLength(length).build();
    }
  }

  @Override
  public void setLocation(long id, BlockLocation blockLocation) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(id)) {
      info.get().mBlockLocations.put(blockLocation.getWorkerId(), blockLocation);
    }
  }

  @Override
  public void removeLocation(long blockId, long workerId) throws NotFoundException {
    try (LockedResource<BlockInfo> info = acquireBlock(blockId)) {
      info.get().mBlockLocations.remove(workerId);
    }
  }

  @Override
  public LockResource lockBlock(long id) throws NotFoundException {
    BlockInfo info = mBlocks.get(id);
    if (info == null) {
      throw new NotFoundException(String.format("Block %d does not exist", id));
    }
    return new LockResource(info.mLock);
  }

  @Nullable
  @Override
  public LockResource lockBlockOrNull(long id) {
    BlockInfo info = mBlocks.get(id);
    if (info == null) {
      return null;
    }
    return new LockResource(info.mLock);
  }

  @Override
  public void reset() {
    LOG.info("Clearing block store");
    mBlocks.clear();
  }

  /**
   * Looks up and locks the info for the specified block id.
   *
   * @param id the block id to look up
   * @return a resource giving access to the block info and allowing the info to be unlocked
   * @throws NotFoundException if the block does not exist
   */
  private LockedResource<BlockInfo> acquireBlock(long id) throws NotFoundException {
    BlockInfo info = mBlocks.get(id);
    if (info == null) {
      throw new NotFoundException(String.format("Block %d does not exist", id));
    }
    info.mLock.lock();
    // Verify that the block wasn't removed between the initial lookup and the call to lock().
    // This check works because we never re-create a block with the same block id.
    if (mBlocks.get(id) == null) {
      throw new NotFoundException(String.format("Block %d does not exist", id));
    }
    return new LockedResource<>(info, info.mLock);
  }

  private static class BlockInfo {
    private final ReentrantLock mLock;
    private final Map<Long, BlockLocation> mBlockLocations;
    private BlockMeta mBlockMeta;

    public BlockInfo(BlockMeta blockMeta) {
      mBlockMeta = blockMeta;
      mBlockLocations = new ConcurrentHashMap<>(3);
      mLock = new ReentrantLock();
    }
  }
}
