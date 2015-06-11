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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.worker.BlockLockManager;
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

  /** A readwrite lock for meta data **/
  private final ReentrantReadWriteLock mEvictionLock = new ReentrantReadWriteLock();

  public TieredBlockStore(TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mMetaManager = new BlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager();

    // TODO: create Allocator according to tachyonConf.
    mAllocator = new NaiveAllocator(mMetaManager);
    // TODO: create Evictor according to tachyonConf
    mEvictor = new NaiveEvictor(mMetaManager);

    // TODO: implement initialization
  }

  @Override
  public Optional<Long> lockBlock(long userId, long blockId, BlockLock.BlockLockType blockLockType) {
    mEvictionLock.readLock().lock();
    return mLockManager.lockBlock(userId, blockId, blockLockType);
  }

  @Override
  public boolean unlockBlock(long lockId) {
    boolean result = mLockManager.unlockBlock(lockId);
    mEvictionLock.readLock().unlock();
    return result;
  }

  @Override
  public Optional<TempBlockMeta> createBlockMeta(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws IOException {
    mEvictionLock.writeLock().lock();
    Optional<TempBlockMeta> optTempBlock = createBlockMetaNoLock(userId, blockId, location,
        initialBlockSize);
    mEvictionLock.writeLock().unlock();
    return optTempBlock;
  }

  private Optional<TempBlockMeta> createBlockMetaNoLock(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws IOException {
    Optional<TempBlockMeta> optTempBlock =
        mAllocator.allocateBlock(userId, blockId, initialBlockSize, location);
    if (!optTempBlock.isPresent()) {
      // Not enough space in this block store, let's try to free some space.
      if (freeSpaceNoLock(userId, initialBlockSize, location)) {
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

  @Override
  public Optional<BlockMeta> getBlockMeta(long userId, long blockId, long lockId) {
    Preconditions.checkState(mLockManager.validateLockId(userId, blockId, lockId));
    return mMetaManager.getBlockMeta(blockId);
  }

  @Override
  public boolean commitBlock(long userId, long blockId) {
    mEvictionLock.writeLock().lock();
    boolean result = commitBlockNoLock(userId, blockId);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  private boolean commitBlockNoLock(long userId, long blockId) {
    // TODO: check the userId is the owner of this temp block?
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    TempBlockMeta tempBlock = optTempBlock.get();
    String sourcePath = tempBlock.getPath();
    String destPath = tempBlock.getCommitPath();
    boolean renamed = new File(sourcePath).renameTo(new File(destPath));
    if (!renamed) {
      return false;
    }
    return mMetaManager.commitTempBlockMeta(tempBlock);
  }

  @Override
  public boolean abortBlock(long userId, long blockId) throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = abortBlockNoLock(userId, blockId);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  private boolean abortBlockNoLock(long userId, long blockId) throws IOException {
    // TODO: check the userId is the owner of this temp block?
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    TempBlockMeta tempBlock = optTempBlock.get();
    String path = tempBlock.getPath();
    boolean deleted = new File(path).delete();
    if (!deleted) {
      return false;
    }
    return mMetaManager.abortTempBlockMeta(tempBlock);
  }

  @Override
  public boolean requestSpace(long userId, long blockId, long size) throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = requestSpaceNoLock(userId, blockId, size);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  private boolean requestSpaceNoLock(long userId, long blockId, long size) throws IOException {
    Optional<TempBlockMeta> optTempBlock = mMetaManager.getTempBlockMeta(blockId);
    if (!optTempBlock.isPresent()) {
      return false;
    }
    BlockStoreLocation location = optTempBlock.get().getBlockLocation();
    if (!freeSpaceNoLock(userId, size, location)) {
      return false;
    }
    // TODO: claim space for this block
    return true;
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
  public boolean moveBlock(long userId, long blockId, long lockId, BlockStoreLocation newLocation)
      throws IOException {
    Preconditions.checkState(mLockManager.validateLockId(userId, blockId, lockId));
    return moveBlockNoLock(userId, blockId, newLocation);
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

  @Override
  public boolean removeBlock(long userId, long blockId, long lockId) throws IOException {
    Preconditions.checkState(mLockManager.validateLockId(userId, blockId, lockId));
    return removeBlockNoLock(userId, blockId);
  }

  private boolean removeBlockNoLock(long userId, long blockId) throws IOException {
    Optional<BlockMeta> optBlock = mMetaManager.getBlockMeta(blockId);
    if (!optBlock.isPresent()) {
      return false;
    }

    // Delete metadata of the block
    if (!mMetaManager.removeBlockMeta(blockId)) {
      return false;
    }
    // Delete the data file of the block
    return new File(optBlock.get().getPath()).delete();
  }

  @Override
  public void accessBlock(long userId, long blockId) {
    // TODO: implement me by calling corresponding evictor methods.
  }

  @Override
  public boolean freeSpace(long userId, long size, BlockStoreLocation location) throws IOException {
    mEvictionLock.writeLock().lock();
    boolean result = freeSpaceNoLock(userId, size, location);
    mEvictionLock.writeLock().unlock();
    return result;
  }

  private boolean freeSpaceNoLock(long userId, long size, BlockStoreLocation location)
      throws IOException {
    EvictionPlan plan = mEvictor.freeSpace(size, location);
    // Step1: remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      if (!removeBlockNoLock(userId, blockId)) {
        return false;
      }
    }
    // Step2: transfer blocks among tiers.
    for (Pair<Long, BlockStoreLocation> entry : plan.toMove()) {
      long blockId = entry.getFirst();
      BlockStoreLocation newLocation = entry.getSecond();
      if (!moveBlockNoLock(userId, blockId, newLocation)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean cleanupUser(long userId) {
    // TODO: implement me
    return false;
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    mEvictionLock.readLock().lock();
    BlockStoreMeta meta = new BlockStoreMeta(mMetaManager);
    mEvictionLock.readLock().unlock();
    return meta;
  }

  @Override
  public void registerMetaListener(BlockMetaEventListener listener) {
    // TODO: implement me
  }

  @Override
  public void registerAccessListener(BlockAccessEventListener listener) {
    // TODO: implement me
  }
}
