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

package tachyon.worker.block;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.allocator.NaiveAllocator;
import tachyon.worker.block.evictor.EvictionPlan;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.evictor.NaiveEvictor;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockReader;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This class represents an object store that manages all the blocks in the local tiered storage.
 * This store exposes simple public APIs to operate blocks. Inside this store, it creates an
 * Allocator to decide where to put a new block, an Evictor to decide where to evict a stale block,
 * a BlockMetadataManager to maintain the status of the tiered storage, and a LockManager to
 * coordinate read/write on the same block.
 * <p>
 * This class is thread-safe.
 */
public class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;

  private List<BlockAccessEventListener> mAccessEventListeners = new
      ArrayList<BlockAccessEventListener>();
  private List<BlockMetaEventListener> mMetaEventListeners = new
      ArrayList<BlockMetaEventListener>();

  /** A readwrite lock for meta data **/
  private final ReentrantReadWriteLock mEvictionLock = new ReentrantReadWriteLock();

  public TieredBlockStore(TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager(mMetaManager);

    // TODO: create Allocator according to tachyonConf.
    mAllocator = new NaiveAllocator(mMetaManager);
    // TODO: create Evictor according to tachyonConf
    mEvictor = new NaiveEvictor(mMetaManager);
  }

  @Override
  public Optional<Long> lockBlock(long userId, long blockId) {
    return mLockManager.lockBlock(userId, blockId, BlockLockType.READ);
  }

  @Override
  public boolean unlockBlock(long lockId) {
    return mLockManager.unlockBlock(lockId);
  }

  @Override
  public boolean unlockBlock(long userId, long blockId) {
    return mLockManager.unlockBlock(userId, blockId);
  }

  @Override
  public Optional<BlockWriter> getBlockWriter(long userId, long blockId) throws IOException {
    Optional<TempBlockMeta> optBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optBlock.isPresent()) {
      return Optional.absent();
    }
    BlockWriter writer = new LocalFileBlockWriter(optBlock.get());
    return Optional.of(writer);
  }

  @Override
  public Optional<BlockReader> getBlockReader(long userId, long blockId, long lockId)
      throws IOException {
    Preconditions.checkState(mLockManager.validateLockId(userId, blockId, lockId));

    Optional<BlockMeta> optBlock = mMetaManager.getBlockMeta(blockId);
    if (!optBlock.isPresent()) {
      return Optional.absent();
    }
    BlockReader reader = new LocalFileBlockReader(optBlock.get());
    return Optional.of(reader);
  }

  @Override
  public Optional<TempBlockMeta> createBlockMeta(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws IOException {
    mEvictionLock.readLock().lock();
    Optional<TempBlockMeta> optTempBlock =
        createBlockMetaNoLock(userId, blockId, location, initialBlockSize);
    mEvictionLock.readLock().unlock();
    return optTempBlock;
  }

  @Override
  public Optional<BlockMeta> getBlockMeta(long userId, long blockId, long lockId) {
    Preconditions.checkState(mLockManager.validateLockId(userId, blockId, lockId));
    return mMetaManager.getBlockMeta(blockId);
  }

  @Override
  public boolean commitBlock(long userId, long blockId) {
    TempBlockMeta tempBlock = mMetaManager.getTempBlockMeta(blockId).orNull();
    for (BlockMetaEventListener listener: mMetaEventListeners) {
      listener.preCommitBlock(userId, blockId, tempBlock.getBlockLocation());
    }

    mEvictionLock.readLock().lock();
    boolean result = commitBlockNoLock(userId, blockId);
    mEvictionLock.readLock().unlock();

    if (result) {
      for (BlockMetaEventListener listener : mMetaEventListeners) {
        listener.postCommitBlock(userId, blockId, tempBlock.getBlockLocation());
      }
    }
    return true;
  }

  @Override
  public boolean abortBlock(long userId, long blockId) {
    mEvictionLock.readLock().lock();
    boolean result = abortBlockNoLock(userId, blockId);
    mEvictionLock.readLock().unlock();
    return result;
  }

  @Override
  public boolean requestSpace(long userId, long blockId, long moreBytes) throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = requestSpaceNoLock(userId, blockId, moreBytes);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  @Override
  public boolean moveBlock(long userId, long blockId, BlockStoreLocation newLocation)
      throws IOException {
    for (BlockMetaEventListener listener: mMetaEventListeners) {
      listener.preMoveBlock(userId, blockId, newLocation);
    }

    mEvictionLock.readLock().lock();
    // TODO: Handle absent
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE).get();
    boolean result = moveBlockNoLock(userId, blockId, newLocation);
    mLockManager.unlockBlock(lockId);
    mEvictionLock.readLock().unlock();

    if (result) {
      for (BlockMetaEventListener listener: mMetaEventListeners) {
        listener.postMoveBlock(userId, blockId, newLocation);
      }
    }
    return result;
  }

  @Override
  public boolean removeBlock(long userId, long blockId) throws IOException {
    for (BlockMetaEventListener listener: mMetaEventListeners) {
      listener.preRemoveBlock(userId, blockId);
    }

    mEvictionLock.readLock().lock();
    // TODO: Handle absent
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE).get();
    boolean result = removeBlockNoLock(userId, blockId);
    mLockManager.unlockBlock(lockId);
    mEvictionLock.readLock().unlock();

    if (result) {
      for (BlockMetaEventListener listener: mMetaEventListeners) {
        listener.postRemoveBlock(userId, blockId);
      }
    }
    return result;
  }

  @Override
  public void accessBlock(long userId, long blockId) {
    for (BlockAccessEventListener listener: mAccessEventListeners) {
      listener.onAccessBlock(userId, blockId);
    }
  }

  @Override
  public boolean freeSpace(long userId, long availableBytes, BlockStoreLocation location)
      throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = freeSpaceNoEvictionLock(userId, availableBytes, location);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  @Override
  public boolean cleanupUser(long userId) {
    mEvictionLock.readLock().lock();
    mMetaManager.cleanupUser(userId);
    mLockManager.cleanupUser(userId);
    mEvictionLock.readLock().unlock();
    return true;
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    return mMetaManager.getBlockStoreMeta();
  }

  @Override
  public void registerMetaListener(BlockMetaEventListener listener) {
    mMetaEventListeners.add(listener);
  }

  @Override
  public void registerAccessListener(BlockAccessEventListener listener) {
    mAccessEventListeners.add(listener);
  }

  private Optional<TempBlockMeta> createBlockMetaNoLock(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws IOException {
    Optional<TempBlockMeta> optTempBlock =
        mAllocator.allocateBlock(userId, blockId, initialBlockSize, location);
    if (!optTempBlock.isPresent()) {
      // Failed to allocate a temp block, let Evictor kick in to ensure sufficient space available.

      // Upgrade to write lock to guard evictor.
      mEvictionLock.readLock().unlock();
      mEvictionLock.writeLock().lock();

      boolean result = freeSpaceNoEvictionLock(userId, initialBlockSize, location);

      // Downgrade to read lock again after eviction
      mEvictionLock.readLock().lock();
      mEvictionLock.writeLock().unlock();

      // Not enough space in this block store, let's try to free some space.
      if (!result) {
        LOG.error("Cannot free {} bytes space in {}", initialBlockSize, location);
        return Optional.absent();
      }
      optTempBlock = mAllocator.allocateBlock(userId, blockId, initialBlockSize, location);
      Preconditions.checkState(optTempBlock.isPresent(), "Cannot allocate block {}:", blockId);
    }
    // Add allocated temp block to metadata manager
    mMetaManager.addTempBlockMeta(optTempBlock.get());
    return optTempBlock;
  }

  private boolean commitBlockNoLock(long userId, long blockId) {
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    TempBlockMeta tempBlock = optTempBlock.get();
    // Check the userId is the owner of this temp block
    if (tempBlock.getUserId() != userId) {
      return false;
    }
    String sourcePath = tempBlock.getPath();
    String destPath = tempBlock.getCommitPath();
    boolean renamed = new File(sourcePath).renameTo(new File(destPath));
    if (!renamed) {
      return false;
    }
    return mMetaManager.commitTempBlockMeta(tempBlock);
  }

  private boolean abortBlockNoLock(long userId, long blockId) {
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    TempBlockMeta tempBlock = optTempBlock.get();
    // Check the userId is the owner of this temp block
    if (tempBlock.getUserId() != userId) {
      return false;
    }
    String path = tempBlock.getPath();
    boolean deleted = new File(path).delete();
    if (!deleted) {
      return false;
    }
    return mMetaManager.abortTempBlockMeta(tempBlock);
  }

  private boolean requestSpaceNoLock(long userId, long blockId, long moreBytes) throws IOException {
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    TempBlockMeta tempBlock = optTempBlock.get();
    BlockStoreLocation location = tempBlock.getBlockLocation();
    if (!freeSpaceNoEvictionLock(userId, moreBytes, location)) {
      return false;
    }

    // Increase the size of this temp block
    tempBlock.setBlockSize(tempBlock.getBlockSize() + moreBytes);
    return true;
  }

  private boolean moveBlockNoLock(long userId, long blockId, BlockStoreLocation newLocation)
      throws IOException {
    Optional<BlockMeta> optSrcBlock = mMetaManager.getBlockMeta(blockId);
    if (!optSrcBlock.isPresent()) {
      return false;
    }
    String srcPath = optSrcBlock.get().getPath();
    Optional<BlockMeta> optDestBlock = mMetaManager.moveBlockMeta(userId, blockId, newLocation);
    if (!optDestBlock.isPresent()) {
      return false;
    }
    String destPath = optDestBlock.get().getPath();

    return new File(srcPath).renameTo(new File(destPath));
  }

  private boolean removeBlockNoLock(long userId, long blockId) throws IOException {
    Optional<BlockMeta> optBlock = mMetaManager.getBlockMeta(blockId);
    if (!optBlock.isPresent()) {
      LOG.error("Block is not present");
      return false;
    }
    BlockMeta block = optBlock.get();
    // Delete metadata of the block
    if (!mMetaManager.removeBlockMeta(block)) {
      LOG.error("Unable to remove metadata");
      return false;
    }
    // Delete the data file of the block
    return new File(block.getPath()).delete();
  }

  private boolean freeSpaceNoEvictionLock(long userId, long availableBytes,
      BlockStoreLocation location) throws IOException {
    Optional<EvictionPlan> optPlan = mEvictor.freeSpace(availableBytes, location);
    // Absent plan means failed to evict enough space.
    if (!optPlan.isPresent()) {
      LOG.error("Failed to free space: no eviction plan by evictor");
      return false;
    }
    EvictionPlan plan = optPlan.get();
    // Step1: remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      // TODO: Handle absent
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE).get();
      boolean result = removeBlockNoLock(userId, blockId);
      mLockManager.unlockBlock(lockId);
      if (!result) {
        LOG.error("Failed to free space: cannot evict block {}", blockId);
        return false;
      }
    }
    // Step2: transfer blocks among tiers.
    for (Pair<Long, BlockStoreLocation> entry : plan.toMove()) {
      long blockId = entry.getFirst();
      BlockStoreLocation newLocation = entry.getSecond();
      // TODO: Handle absent
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE).get();
      boolean result = moveBlockNoLock(userId, blockId, newLocation);
      mLockManager.unlockBlock(lockId);
      if (!result) {
        LOG.error("Failed to free space: cannot move block {} to {}", blockId, newLocation);
        return false;
      }
    }
    return true;
  }

}
