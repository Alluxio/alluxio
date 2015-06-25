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

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;
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
  private List<BlockAccessEventListener> mAccessEventListeners =
      new ArrayList<BlockAccessEventListener>();
  private List<BlockMetaEventListener> mMetaEventListeners =
      new ArrayList<BlockMetaEventListener>();

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
  public long lockBlock(long userId, long blockId) throws IOException {
    return mLockManager.lockBlock(userId, blockId, BlockLockType.READ);
  }

  @Override
  public void unlockBlock(long lockId) throws IOException {
    mLockManager.unlockBlock(lockId);
  }

  @Override
  public void unlockBlock(long userId, long blockId) throws IOException {
    mLockManager.unlockBlock(userId, blockId);
  }

  @Override
  public BlockWriter getBlockWriter(long userId, long blockId) throws IOException {
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    return new LocalFileBlockWriter(tempBlockMeta);
  }

  @Override
  public BlockReader getBlockReader(long userId, long blockId, long lockId) throws IOException {
    mLockManager.validateLockId(userId, blockId, lockId);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
    return new LocalFileBlockReader(blockMeta);
  }

  @Override
  public TempBlockMeta createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws IOException {
    TempBlockMeta tempBlock;
    mEvictionLock.readLock().lock();
    try {
      tempBlock = createBlockMetaNoLock(userId, blockId, location, initialBlockSize);
    } finally {
      mEvictionLock.readLock().unlock();
    }
    return tempBlock;
  }

  @Override
  public BlockMeta getBlockMeta(long userId, long blockId, long lockId) throws IOException {
    mLockManager.validateLockId(userId, blockId, lockId);
    return mMetaManager.getBlockMeta(blockId);
  }

  @Override
  public void commitBlock(long userId, long blockId) throws IOException {
    mEvictionLock.readLock().lock();
    try {
      commitBlockNoLock(userId, blockId);
    } finally {
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void abortBlock(long userId, long blockId) throws IOException {
    mEvictionLock.readLock().lock();
    try {
      abortBlockNoLock(userId, blockId);
    } finally {
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void requestSpace(long userId, long blockId, long moreBytes) throws IOException {
    // TODO: Change the lock to read lock and only upgrade to write lock if necessary
    mEvictionLock.writeLock().lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      BlockStoreLocation location = tempBlockMeta.getBlockLocation();
      freeSpaceInternal(userId, moreBytes, location);
      // Increase the size of this temp block
      mMetaManager.resizeTempBlockMeta(tempBlockMeta, tempBlockMeta.getBlockSize() + moreBytes);
    } finally {
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public void moveBlock(long userId, long blockId, BlockStoreLocation newLocation)
      throws IOException {
    mEvictionLock.readLock().lock();
    try {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        moveBlockNoLock(userId, blockId, newLocation);
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    } finally {
      // If we fail to lock, the block is no longer in tiered store
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void removeBlock(long userId, long blockId) throws IOException {
    mEvictionLock.readLock().lock();
    try {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        removeBlockNoLock(userId, blockId);
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    } finally {
      // If we fail to lock, the block is no longer in tiered store
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void accessBlock(long userId, long blockId) {
    for (BlockAccessEventListener listener : mAccessEventListeners) {
      listener.onAccessBlock(userId, blockId);
    }
  }

  @Override
  public void freeSpace(long userId, long availableBytes, BlockStoreLocation location)
      throws IOException {
    mEvictionLock.writeLock().lock();
    try {
      freeSpaceInternal(userId, availableBytes, location);
    } finally {
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public void cleanupUser(long userId) throws IOException {
    mEvictionLock.readLock().lock();
    List<TempBlockMeta> tempBlocksToRemove = mMetaManager.cleanupUser(userId);
    mLockManager.cleanupUser(userId);
    mEvictionLock.readLock().unlock();
    List<String> dirs = new ArrayList<String>();
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      String fileName = tempBlockMeta.getPath();
      try {
        String dirName = CommonUtils.getParent(fileName);
        dirs.add(dirName);
      } catch (InvalidPathException e) {
        LOG.error("Cannot parse parent dir of {}", fileName);
      }
      if (!new File(fileName).delete()) {
        throw new IOException("Failed to cleanup userId " + userId + ": cannot delete file "
            + fileName);
      }
    }
    // Cleanup the user folder
    for (String dirName : dirs) {
      if (!new File(dirName).delete()) {
        throw new IOException("Failed to cleanup userId " + userId + ": cannot delete directory "
            + dirName);
      }
    }
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

  private TempBlockMeta createBlockMetaNoLock(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws IOException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new IOException("Failed to create TempBlockMeta: blockId " + blockId + " exists");
    }
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new IOException("Failed to create TempBlockMeta: blockId " + blockId + " committed");
    }
    TempBlockMeta tempBlock = mAllocator.allocateBlock(userId, blockId, initialBlockSize, location);
    if (tempBlock == null) {
      // Failed to allocate a temp block, let Evictor kick in to ensure sufficient space available.

      // Upgrade to write lock to guard evictor.
      mEvictionLock.readLock().unlock();
      mEvictionLock.writeLock().lock();
      try {
        freeSpaceInternal(userId, initialBlockSize, location);
      } finally {
        // Downgrade to read lock again after eviction
        mEvictionLock.readLock().lock();
        mEvictionLock.writeLock().unlock();
      }
      tempBlock = mAllocator.allocateBlock(userId, blockId, initialBlockSize, location);
      Preconditions.checkNotNull(tempBlock, "Cannot allocate block %s:", blockId);
    }
    // Add allocated temp block to metadata manager
    mMetaManager.addTempBlockMeta(tempBlock);
    return tempBlock;
  }

  private void commitBlockNoLock(long userId, long blockId) throws IOException {
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.preCommitBlock(userId, blockId, tempBlockMeta.getBlockLocation());
    }

    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new IOException("Failed to commit block " + blockId + ": block is committed");
    }
    // Check the userId is the owner of this temp block
    long ownerUserId = tempBlockMeta.getUserId();
    if (ownerUserId != userId) {
      throw new IOException("Failed to commit temp block " + blockId + ": ownerUserId "
          + ownerUserId + " but userId " + userId);
    }
    String sourcePath = tempBlockMeta.getPath();
    String destPath = tempBlockMeta.getCommitPath();
    boolean renamed = new File(sourcePath).renameTo(new File(destPath));
    if (!renamed) {
      throw new IOException("Failed to commit temp block " + blockId + ": cannot rename from "
          + sourcePath + " to " + destPath);
    }
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.postCommitBlock(userId, blockId, tempBlockMeta.getBlockLocation());
    }
  }

  private void abortBlockNoLock(long userId, long blockId) throws IOException {
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new IOException("Failed to abort block " + blockId + ": block is committed");
    }

    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    // Check the userId is the owner of this temp block
    long ownerUserId = tempBlockMeta.getUserId();
    if (ownerUserId != userId) {
      throw new IOException("Failed to abort temp block " + blockId + ": ownerUserId "
          + ownerUserId + " but userId " + userId);
    }
    String path = tempBlockMeta.getPath();
    boolean deleted = new File(path).delete();
    if (!deleted) {
      throw new IOException("Failed to abort temp block " + blockId + ": cannot delete " + path);
    }
    mMetaManager.abortTempBlockMeta(tempBlockMeta);
  }

  private void moveBlockNoLock(long userId, long blockId, BlockStoreLocation newLocation)
      throws IOException {
    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.preMoveBlock(userId, blockId, newLocation);
    }

    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new IOException("Failed to move block " + blockId + ": block is uncommited");
    }
    BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
    String srcPath = blockMeta.getPath();
    blockMeta = mMetaManager.moveBlockMeta(blockMeta, newLocation);
    String destPath = blockMeta.getPath();

    if (!new File(srcPath).renameTo(new File(destPath))) {
      throw new IOException("Failed to move block " + blockId + ": cannot rename from " + srcPath
          + " to " + destPath);
    }

    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.postMoveBlock(userId, blockId, newLocation);
    }
  }

  private void removeBlockNoLock(long userId, long blockId) throws IOException {
    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.preRemoveBlock(userId, blockId);
    }

    // Delete metadata of the block---no matter it is a temp block.
    String filePath;
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      mMetaManager.abortTempBlockMeta(tempBlockMeta);
      filePath = tempBlockMeta.getPath();
    } else if (mMetaManager.hasBlockMeta(blockId)) {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      mMetaManager.removeBlockMeta(blockMeta);
      filePath = blockMeta.getPath();
    } else {
      throw new IOException("Failed to move block " + blockId + ": block is not found");
    }

    // Delete the data of the block on "disk"
    if (!new File(filePath).delete()) {
      throw new IOException("Failed to remove block " + blockId + ": cannot delete " + filePath);
    }

    for (BlockMetaEventListener listener : mMetaEventListeners) {
      listener.postRemoveBlock(userId, blockId);
    }
  }

  // This method must be guarded by WRITE lock of mEvictionLock
  private void freeSpaceInternal(long userId, long availableBytes, BlockStoreLocation location)
      throws IOException {
    EvictionPlan plan = mEvictor.freeSpace(availableBytes, location);
    // Absent plan means failed to evict enough space.
    if (plan == null) {
      throw new IOException("Failed to free space: no eviction plan by evictor");
    }

    // Step1: remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        removeBlockNoLock(userId, blockId);
      } catch (IOException e) {
        throw new IOException("Failed to free space: cannot evict block " + blockId);
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    }
    // Step2: transfer blocks among tiers.
    for (Pair<Long, BlockStoreLocation> entry : plan.toMove()) {
      long blockId = entry.getFirst();
      BlockStoreLocation newLocation = entry.getSecond();
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        moveBlockNoLock(userId, blockId, newLocation);
      } catch (IOException e) {
        throw new IOException("Failed to free space: cannot move block " + blockId + " to "
            + newLocation);
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    }
  }
}
