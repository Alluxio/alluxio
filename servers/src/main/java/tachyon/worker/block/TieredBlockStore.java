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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.util.io.FileUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.evictor.EvictionPlan;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockReader;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
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
// TODO: This atomicity comes with cost of heavy locking, improve locking by not guard eviction
// (and its IO performance) with heavy write lock (TACHYON-584)
// TODO: If a method requires certain locks being hold, validate it.
public class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;
  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new ArrayList<BlockStoreEventListener>();
  /** A set of pinned inodes fetched from the master */
  private final Set<Integer> mPinnedInodes = new HashSet<Integer>();

  /**
   * A read/write lock to ensure eviction is atomic w.r.t. other operations. An eviction may trigger
   * a sequence of block remove and move and we want eviction to be atomic so no remove or move
   * interleaves during eviction is working. The current workaround is to wrap single block
   * operations like remove and move by read lock and wrap eviction operations by write lock that
   * can wait for previous operations and block following operations.
   */
  private final ReentrantReadWriteLock mEvictionLock = new ReentrantReadWriteLock();

  public TieredBlockStore(TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mMetaManager = BlockMetadataManager.newBlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager(mMetaManager);

    BlockMetadataManagerView initManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
            Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.createAllocator(mTachyonConf, initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    initManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
            Collections.<Long>emptySet());
    mEvictor = Evictor.Factory.createEvictor(mTachyonConf, initManagerView);
    if (mEvictor instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    }
  }

  @Override
  public long lockBlock(long userId, long blockId) throws NotFoundException {
    mEvictionLock.readLock().lock();
    try {
      return mLockManager.lockBlock(userId, blockId, BlockLockType.READ);
    } finally {
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void unlockBlock(long lockId) throws NotFoundException {
    mLockManager.unlockBlock(lockId);
  }

  @Override
  public void unlockBlock(long userId, long blockId) throws NotFoundException {
    mLockManager.unlockBlock(userId, blockId);
  }

  @Override
  public BlockWriter getBlockWriter(long userId, long blockId) throws NotFoundException,
      IOException {
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    return new LocalFileBlockWriter(tempBlockMeta);
  }

  @Override
  public BlockReader getBlockReader(long userId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException, IOException {
    mLockManager.validateLock(userId, blockId, lockId);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
    return new LocalFileBlockReader(blockMeta);
  }

  @Override
  public TempBlockMeta createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws AlreadyExistsException, OutOfSpaceException, NotFoundException,
      IOException, InvalidStateException {
    mEvictionLock.writeLock().lock();
    try {
      return createBlockMetaNoLock(userId, blockId, location, initialBlockSize);
    } finally {
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws NotFoundException {
    return mMetaManager.getBlockMeta(blockId);
  }

  @Override
  public BlockMeta getBlockMeta(long userId, long blockId, long lockId) throws NotFoundException,
      InvalidStateException {
    mLockManager.validateLock(userId, blockId, lockId);
    return mMetaManager.getBlockMeta(blockId);
  }

  @Override
  public void commitBlock(long userId, long blockId) throws AlreadyExistsException,
      InvalidStateException, NotFoundException, IOException, OutOfSpaceException {
    mEvictionLock.readLock().lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      commitBlockNoLock(userId, blockId, tempBlockMeta);
      // TODO: move listeners outside of the lock.
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onCommitBlock(userId, blockId, tempBlockMeta.getBlockLocation());
        }
      }
    } finally {
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void abortBlock(long userId, long blockId) throws AlreadyExistsException,
      NotFoundException, InvalidStateException, IOException {
    mEvictionLock.readLock().lock();
    try {
      abortBlockNoLock(userId, blockId);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onAbortBlock(userId, blockId);
        }
      }
    } finally {
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void requestSpace(long userId, long blockId, long additionalBytes)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException,
      InvalidStateException {
    // TODO: Change the lock to read lock and only upgrade to write lock if necessary
    mEvictionLock.writeLock().lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      freeSpaceInternal(userId, additionalBytes, tempBlockMeta.getBlockLocation());
      // Increase the size of this temp block
      mMetaManager.resizeTempBlockMeta(tempBlockMeta, tempBlockMeta.getBlockSize()
          + additionalBytes);
    } finally {
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public void moveBlock(long userId, long blockId, BlockStoreLocation newLocation)
      throws NotFoundException, AlreadyExistsException, InvalidStateException, OutOfSpaceException,
      IOException {
    mEvictionLock.writeLock().lock();
    try {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
        BlockStoreLocation oldLocation = blockMeta.getBlockLocation();
        // freeSpaceInternal ensures newLocation has enough space
        freeSpaceInternal(userId, blockMeta.getBlockSize(), newLocation);
        moveBlockNoLock(blockId, newLocation);
        blockMeta = mMetaManager.getBlockMeta(blockId);
        BlockStoreLocation actualNewLocation = blockMeta.getBlockLocation();
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByClient(userId, blockId, oldLocation, actualNewLocation);
          }
        }
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    } finally {
      // If we fail to lock, the block is no longer in tiered store
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public void removeBlock(long userId, long blockId) throws InvalidStateException,
      NotFoundException, IOException {
    mEvictionLock.readLock().lock();
    try {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        removeBlockNoLock(userId, blockId);
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onRemoveBlockByClient(userId, blockId);
          }
        }
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    } finally {
      // If we fail to lock, the block is no longer in tiered store
      mEvictionLock.readLock().unlock();
    }
  }

  @Override
  public void accessBlock(long userId, long blockId) throws NotFoundException {
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAccessBlock(userId, blockId);
      }
    }
  }

  @Override
  public void freeSpace(long userId, long availableBytes, BlockStoreLocation location)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException,
      InvalidStateException {
    mEvictionLock.writeLock().lock();
    try {
      freeSpaceInternal(userId, availableBytes, location);
    } finally {
      mEvictionLock.writeLock().unlock();
    }
  }

  @Override
  public void cleanupUser(long userId) {
    List<TempBlockMeta> tempBlocksToRemove = mMetaManager.getUserTempBlocks(userId);
    List<Long> removedTempBlocks = new ArrayList<Long>(tempBlocksToRemove.size());
    // TODO: fix the block removing below, there is possible risk condition when the client which
    // is considered "dead" may still be using or committing this block.
    // A user may have multiple temporary directories for temp blocks, in diffrent StorageTier
    // and StorageDir.
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      String fileName = tempBlockMeta.getPath();
      if (!new File(fileName).delete()) {
        LOG.error("Error in cleanup userId {}: cannot delete file {}", userId, fileName);
      } else {
        removedTempBlocks.add(tempBlockMeta.getBlockId());
      }
    }

    // Go through all the storage directories and delete the user folders which should be empty
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        File userFolder = new File(PathUtils.concatPath(dir.getDirPath(), userId));
        if (userFolder.exists() && !userFolder.delete()) {
          // This error means we could not delete the directory but should not affect the
          // correctness of the method since the data has already been deleted. It is not
          // necessary to throw an exception here.
          LOG.error("Failed to clean up user: {} with directory: {}", userId, userFolder.getPath());
        }
      }
    }

    // Release all locks the user is holding.
    mLockManager.cleanupUser(userId);

    // Delete the temporary metadata for the user.
    mEvictionLock.readLock().lock();
    mMetaManager.cleanupUserTempBlocks(userId, removedTempBlocks);
    mEvictionLock.readLock().unlock();
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mMetaManager.hasBlockMeta(blockId);
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    return mMetaManager.getBlockStoreMeta();
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    synchronized (mBlockStoreEventListeners) {
      mBlockStoreEventListeners.add(listener);
    }
  }

  /**
   * Validate blockId is a temporary block and owned by userId, if the validation succeeds, return
   * the {@link tachyon.worker.block.meta.TempBlockMeta} of blockId.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @throws NotFoundException if blockId can not be found in temporary blocks
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws InvalidStateException if blockId is not owned by userId
   * @return the {@link tachyon.worker.block.meta.TempBlockMeta} of blockId
   */
  private TempBlockMeta validateUserTempBlock(long userId, long blockId) throws NotFoundException,
      AlreadyExistsException, InvalidStateException {
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new AlreadyExistsException("blockId " + blockId + " is committed");
    }

    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    // Check the userId is the owner of this temp block
    long ownerUserId = tempBlockMeta.getUserId();
    if (ownerUserId != userId) {
      throw new InvalidStateException("ownerUserId of blockId " + blockId + " is " + ownerUserId
          + " but userId passed in is " + userId);
    }

    return tempBlockMeta;
  }

  // Abort a temp block. This method requires eviction lock in READ mode.
  private void abortBlockNoLock(long userId, long blockId) throws NotFoundException,
      AlreadyExistsException, InvalidStateException, IOException {
    TempBlockMeta tempBlockMeta = validateUserTempBlock(userId, blockId);
    String path = tempBlockMeta.getPath();
    if (!new File(path).delete()) {
      throw new IOException("Failed to abort temp block " + blockId + ": cannot delete " + path);
    }
    mMetaManager.abortTempBlockMeta(tempBlockMeta);
  }

  // Commit a temp block. This method requires eviction lock in READ mode.
  private void commitBlockNoLock(long userId, long blockId, TempBlockMeta tempBlockMeta)
      throws AlreadyExistsException, InvalidStateException, NotFoundException, IOException,
      OutOfSpaceException {
    validateUserTempBlock(userId, blockId);
    String sourcePath = tempBlockMeta.getPath();
    String destPath = tempBlockMeta.getCommitPath();
    FileUtils.move(new File(sourcePath), new File(destPath));
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
  }

  // Create a temp block meta. This method requires eviction lock in READ mode.
  private TempBlockMeta createBlockMetaNoLock(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws AlreadyExistsException,
      OutOfSpaceException, NotFoundException, IOException, InvalidStateException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new AlreadyExistsException("Failed to create TempBlockMeta: blockId " + blockId
          + " exists");
    }
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new AlreadyExistsException("Failed to create TempBlockMeta: blockId " + blockId
          + " committed");
    }
    TempBlockMeta tempBlock =
        mAllocator.allocateBlockWithView(userId, blockId, initialBlockSize, location,
            getUpdatedView());
    if (tempBlock == null) {
      // Failed to allocate a temp block, let Evictor kick in to ensure sufficient space available.
      freeSpaceInternal(userId, initialBlockSize, location);
      tempBlock =
          mAllocator.allocateBlockWithView(userId, blockId, initialBlockSize, location,
              getUpdatedView());
      Preconditions.checkNotNull(tempBlock, "Cannot allocate block %s:", blockId);
    }
    // Add allocated temp block to metadata manager
    mMetaManager.addTempBlockMeta(tempBlock);
    return tempBlock;
  }

  /** This method must be guarded by WRITE lock of mEvictionLock */
  private void freeSpaceInternal(long userId, long availableBytes, BlockStoreLocation location)
      throws OutOfSpaceException, IOException, NotFoundException, AlreadyExistsException,
      InvalidStateException {
    EvictionPlan plan = mEvictor.freeSpaceWithView(availableBytes, location, getUpdatedView());
    // Absent plan means failed to evict enough space.
    if (null == plan) {
      throw new OutOfSpaceException("Failed to free space: no eviction plan by evictor");
    }
    // 1. remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
      try {
        removeBlockNoLock(userId, blockId);
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onRemoveBlockByWorker(userId, blockId);
          }
        }
      } finally {
        mLockManager.unlockBlock(lockId);
      }
    }
    // 2. transfer blocks among tiers.
    // 2.1. group blocks move plan by the destination tier.
    Map<Integer, Set<Pair<Long, BlockStoreLocation>>> blocksGroupedByDestTier =
        new HashMap<Integer, Set<Pair<Long, BlockStoreLocation>>>();
    for (Pair<Long, BlockStoreLocation> entry : plan.toMove()) {
      int alias = entry.getSecond().tierAlias();
      if (!blocksGroupedByDestTier.containsKey(alias)) {
        blocksGroupedByDestTier.put(alias, new HashSet<Pair<Long, BlockStoreLocation>>());
      }
      blocksGroupedByDestTier.get(alias).add(entry);
    }
    // 2.2. sort tiers according in reversed order: bottom tier first and top tier last.
    List<Integer> destTierAlias = new ArrayList<Integer>(blocksGroupedByDestTier.keySet());
    Collections.sort(destTierAlias, Collections.reverseOrder());
    // 2.3. move blocks in the order of their dest tiers.
    for (int alias : destTierAlias) {
      Set<Pair<Long, BlockStoreLocation>> toMove = blocksGroupedByDestTier.get(alias);
      for (Pair<Long, BlockStoreLocation> entry : toMove) {
        long blockId = entry.getFirst();
        BlockStoreLocation newLocation = entry.getSecond();
        BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
        BlockStoreLocation oldLocation = blockMeta.getBlockLocation();
        long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
        try {
          moveBlockNoLock(blockId, newLocation);
          synchronized (mBlockStoreEventListeners) {
            for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
              listener.onMoveBlockByWorker(userId, blockId, oldLocation, newLocation);
            }
          }
        } finally {
          mLockManager.unlockBlock(lockId);
        }
      }
    }
  }

  /**
   * Get the most updated view with most recent information on pinned inodes, and currently locked
   * blocks.
   *
   * @return BlockMetadataManagerView, a updated view with most recent infomation.
   */
  private BlockMetadataManagerView getUpdatedView() {
    // TODO: update the view object instead of creating new one every time
    synchronized (mPinnedInodes) {
      return new BlockMetadataManagerView(mMetaManager, mPinnedInodes,
          mLockManager.getLockedBlocks());
    }
  }

  /** Move a block. This method requires block lock in WRITE mode and eviction lock in READ mode */
  private void moveBlockNoLock(long blockId, BlockStoreLocation newLocation)
      throws NotFoundException, AlreadyExistsException, InvalidStateException, OutOfSpaceException,
      IOException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new InvalidStateException("Failed to move block " + blockId + ": block is uncommited");
    }
    BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
    // NOTE: since WRITE Eviction lock is acquired, we move metadata first before moving raw data.
    mMetaManager.moveBlockMeta(blockMeta, newLocation);
    BlockMeta newBlockMeta = mMetaManager.getBlockMeta(blockId);
    String srcFilePath = blockMeta.getPath();
    String dstFilePath = newBlockMeta.getPath();
    FileUtils.move(new File(srcFilePath), new File(dstFilePath));
  }

  /**
   * Remove a block. This method requires block lock in WRITE mode and eviction lock in READ mode.
   */
  private void removeBlockNoLock(long userId, long blockId) throws InvalidStateException,
      NotFoundException, IOException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new InvalidStateException("Failed to remove block " + blockId
          + ": block is uncommitted");
    }
    BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
    String filePath = blockMeta.getPath();
    // Delete the data of the block on "disk"
    if (!new File(filePath).delete()) {
      throw new IOException("Failed to remove block " + blockId + ": cannot delete " + filePath);
    }
    mMetaManager.removeBlockMeta(blockMeta);
  }

  /**
   * updates the pinned blocks
   *
   * @param inodes a set of IDs inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Integer> inodes) {
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }
}
