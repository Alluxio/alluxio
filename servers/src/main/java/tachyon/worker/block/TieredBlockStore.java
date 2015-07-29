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
import com.google.common.base.Throwables;

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
 * <p/>
 * This class is thread-safe.
 * <p/>
 * Locking hierarchy
 * <p/>
 * A read/write lock to ensure eviction is atomic w.r.t. other operations. An eviction may trigger a
 * sequence of block remove and move and we want eviction to be atomic so no remove or move
 * interleaves during eviction is working. The current workaround is to wrap single block operations
 * like remove and move by read lock and wrap eviction operations by write lock that can wait for
 * previous operations and block following operations.
 */
// TODO: This atomicity comes with cost of heavy locking, improve locking by not guard eviction
// (and its IO performance) with heavy write lock (TACHYON-584)
public class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // TODO: change maxRetry to be configurable.
  private static final int MAX_RETRIES = 3;

  private final TachyonConf mTachyonConf;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;
  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new ArrayList<BlockStoreEventListener>();
  /**
   * A set of pinned inodes fetched from the master
   */
  private final Set<Integer> mPinnedInodes = new HashSet<Integer>();

  /**
   * Lock to guard metadata operations
   */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  public TieredBlockStore(TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mMetaManager = BlockMetadataManager.newBlockMetadataManager(mTachyonConf);
    mLockManager = new BlockLockManager(mMetaManager);

    BlockMetadataManagerView initManagerView = new BlockMetadataManagerView(mMetaManager,
        Collections.<Integer>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.createAllocator(mTachyonConf, initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    initManagerView = new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
        Collections.<Long>emptySet());
    mEvictor = Evictor.Factory.createEvictor(mTachyonConf, initManagerView);
    if (mEvictor instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    }
  }

  @Override
  public long lockBlock(long userId, long blockId) throws NotFoundException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.READ);
    mMetadataLock.readLock().lock();
    try {
      if (!mMetaManager.hasBlockMeta(blockId)) {
        throw new NotFoundException("Failed to lockBlock: no blockId " + blockId + " found");
      }
      return lockId;
    } finally {
      mMetadataLock.readLock().unlock();
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
  public BlockWriter getBlockWriter(long userId, long blockId)
      throws NotFoundException, IOException {
    mMetadataLock.readLock().lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      return new LocalFileBlockWriter(tempBlockMeta);
    } finally {
      mMetadataLock.readLock().unlock();
    }

  }

  @Override
  public BlockReader getBlockReader(long userId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException, IOException {
    mLockManager.validateLock(userId, blockId, lockId);
    mMetadataLock.readLock().lock();
    try {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      return new LocalFileBlockReader(blockMeta);
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  @Override
  public TempBlockMeta createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws AlreadyExistsException, OutOfSpaceException, NotFoundException,
          IOException, InvalidStateException {
    for (int i = 0; i < MAX_RETRIES; i ++) {
      TempBlockMeta tempBlockMeta =
          createBlockMetaInternal(userId, blockId, location, initialBlockSize);
      if (tempBlockMeta != null) {
        return tempBlockMeta;
      }
      // Failed to allocate a temp block, so trigger Evictor to make some space Note that, even
      // {@link freeSpaceInternal} succeeds, it does not ensure the next allocation successful,
      // because these two operations are not atomic.
      if (i < MAX_RETRIES - 1) {
        freeSpaceInternal(userId, initialBlockSize, location);
      }
    }
    // TODO: we are probably seeing a rare transient failure, maybe define and throw some other
    // types of exception to indicate this case.
    throw new OutOfSpaceException("Failed to create blockMeta: blockId " + blockId + " "
        + "failed to allocate " + initialBlockSize + " bytes after " + MAX_RETRIES + " retries");
  }

  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws NotFoundException {
    mMetadataLock.readLock().lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  @Override
  public BlockMeta getBlockMeta(long userId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException {
    mLockManager.validateLock(userId, blockId, lockId);
    mMetadataLock.readLock().lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  @Override
  public void commitBlock(long userId, long blockId) throws AlreadyExistsException,
      InvalidStateException, NotFoundException, IOException, OutOfSpaceException {
    BlockStoreLocation loc = commitBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onCommitBlock(userId, blockId, loc);
      }
    }
  }

  @Override
  public void abortBlock(long userId, long blockId)
      throws AlreadyExistsException, NotFoundException, InvalidStateException, IOException {
    abortBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAbortBlock(userId, blockId);
      }
    }
  }

  @Override
  public void requestSpace(long userId, long blockId, long additionalBytes)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException,
      InvalidStateException {
    for (int i = 0; i < MAX_RETRIES; i ++) {
      BlockStoreLocation locationToFree = requestSpaceInternal(blockId, additionalBytes);
      if (locationToFree == null) {
        return;
      }
      freeSpaceInternal(userId, additionalBytes, locationToFree);
    }
  }

  @Override
  public void moveBlock(long userId, long blockId, BlockStoreLocation newLocation)
      throws NotFoundException, AlreadyExistsException, InvalidStateException, OutOfSpaceException,
      IOException {
    Pair<BlockStoreLocation, BlockStoreLocation> locationPair;
    BlockStoreLocation srcLocation;
    BlockStoreLocation dstLocation;
    for (int i = 0; i < MAX_RETRIES; i ++) {
      locationPair = moveBlockInternal(userId, blockId, newLocation);
      if (locationPair != null) {
        srcLocation = locationPair.getFirst();
        dstLocation = locationPair.getSecond();
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByClient(userId, blockId, srcLocation, dstLocation);
          }
        }
        return;
      }
      mMetadataLock.readLock().lock();
      long blockSize = mMetaManager.getBlockMeta(blockId).getBlockSize();
      mMetadataLock.readLock().unlock();
      freeSpaceInternal(userId, blockSize, newLocation);
    }
  }

  @Override
  public void removeBlock(long userId, long blockId)
      throws InvalidStateException, NotFoundException, IOException {
    removeBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onRemoveBlockByClient(userId, blockId);
      }
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
    freeSpaceInternal(userId, availableBytes, location);
  }

  @Override
  public void cleanupUser(long userId) {
    List<TempBlockMeta> tempBlocksToRemove;
    mMetadataLock.readLock().lock();
    try {
      tempBlocksToRemove = mMetaManager.getUserTempBlocks(userId);
    } finally {
      mMetadataLock.readLock().unlock();
    }

    // TODO: fix the block removing below, there is possible risk condition when the client which
    // is considered "dead" may still be using or committing this block.
    // A user may have multiple temporary directories for temp blocks, in different StorageTier
    // and StorageDir.
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      try {
        abortBlockInternal(userId, tempBlockMeta.getBlockId());
      } catch (Exception e) {
        LOG.error("Failed to cleanup tempBlock " + tempBlockMeta.getBlockId() + " due to "
            + e.getMessage());
      }
    }

    // Go through all the storage directories and delete the user folders which should be empty
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        File userFolder = new File(PathUtils.concatPath(dir.getDirPath(), userId));
        try {
          if (userFolder.exists()) {
            FileUtils.delete(userFolder);
          }
        } catch (IOException ioe) {
          // This error means we could not delete the directory but should not affect the
          // correctness of the method since the data has already been deleted. It is not
          // necessary to throw an exception here.
          LOG.error("Failed to clean up user: {} with directory: {}", userId, userFolder.getPath());
        }
      }
    }

    // Release all locks the user is holding.
    mLockManager.cleanupUser(userId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    mMetadataLock.readLock().lock();
    try {
      return mMetaManager.hasBlockMeta(blockId);
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    mMetadataLock.readLock().lock();
    try {
      return mMetaManager.getBlockStoreMeta();
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    synchronized (mBlockStoreEventListeners) {
      mBlockStoreEventListeners.add(listener);
    }
  }

  /**
   * Check if a blockId is available as a new temp block.
   *
   * @param blockId the ID of block
   * @throws AlreadyExistsException if blockId already exists
   */
  private void checkTempBlockIdAvailable(long blockId) throws AlreadyExistsException {
    mMetadataLock.readLock().lock();
    try {
      if (mMetaManager.hasTempBlockMeta(blockId)) {
        throw new AlreadyExistsException("TempBlockMeta blockId " + blockId + " exists");
      }
      if (mMetaManager.hasBlockMeta(blockId)) {
        throw new AlreadyExistsException("TempBlockMeta blockId " + blockId + " committed");
      }
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  /**
   * Check if blockId is a temporary block and owned by userId, return the
   * {@link tachyon.worker.block.meta.TempBlockMeta} of blockId if the validation succeeds.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @return the {@link tachyon.worker.block.meta.TempBlockMeta} of blockId
   * @throws NotFoundException if blockId can not be found in temporary blocks
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws InvalidStateException if blockId is not owned by userId
   */
  private TempBlockMeta checkTempBlockExists(long userId, long blockId)
      throws NotFoundException, AlreadyExistsException, InvalidStateException {
    mMetadataLock.readLock().lock();
    try {
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
    } finally {
      mMetadataLock.readLock().unlock();
    }
  }

  // Abort a temp block.
  private void abortBlockInternal(long userId, long blockId)
      throws NotFoundException, AlreadyExistsException, InvalidStateException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      String path;
      mMetadataLock.writeLock().lock();
      try {
        TempBlockMeta tempBlockMeta = checkTempBlockExists(userId, blockId);
        path = tempBlockMeta.getPath();
        mMetaManager.abortTempBlockMeta(tempBlockMeta);
      } finally {
        mMetadataLock.writeLock().unlock();
      }

      FileUtils.delete(new File(path));
    } finally {
      mLockManager.unlockBlock(lockId);
    }

  }

  // Commit a temp block.
  private BlockStoreLocation commitBlockInternal(long userId, long blockId)
      throws AlreadyExistsException, InvalidStateException, NotFoundException, IOException,
      OutOfSpaceException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      BlockStoreLocation loc;
      String srcPath;
      String dstPath;
      mMetadataLock.writeLock().lock();
      try {
        TempBlockMeta tempBlockMeta = checkTempBlockExists(userId, blockId);
        srcPath = tempBlockMeta.getPath();
        dstPath = tempBlockMeta.getCommitPath();
        loc = tempBlockMeta.getBlockLocation();
        mMetaManager.commitTempBlockMeta(tempBlockMeta);
      } finally {
        mMetadataLock.writeLock().unlock();
      }

      FileUtils.move(new File(srcPath), new File(dstPath));
      return loc;
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Creates a temp block meta only if allocator finds available space. This method will not trigger
   * any eviction.
   *
   * @param userId user Id
   * @param blockId block Id
   * @param location location to create the block
   * @param initialBlockSize initial block size in bytes
   * @return a temp block created if successful, or null if allocation failed (instead of throwing
   *         OutOfSpaceException because allocation failure could be an expected case)
   * @throws AlreadyExistsException if there is a block already having the same block id
   */
  private TempBlockMeta createBlockMetaInternal(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize) throws AlreadyExistsException {
    mMetadataLock.writeLock().lock();
    try {
      checkTempBlockIdAvailable(blockId);
      TempBlockMeta tempBlock = mAllocator.allocateBlockWithView(userId, blockId, initialBlockSize,
          location, getUpdatedView());
      if (tempBlock == null) {
        // Allocator fails to find a proper place for this new block.
        return null;
      }
      try {
        // Add allocated temp block to metadata manager. This should never fail if allocator
        // correctly assigns a StorageDir.
        mMetaManager.addTempBlockMeta(tempBlock);
      } catch (OutOfSpaceException ose) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: " + initialBlockSize + " bytes allocated at " + location
            + " by allocator, but addTempBlockMeta failed");
        throw Throwables.propagate(ose);
      } catch (AlreadyExistsException aee) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: " + initialBlockSize + " bytes allocated at " + location
            + " by allocator, but addTempBlockMeta failed");
        throw Throwables.propagate(aee);
      }
      return tempBlock;
    } finally {
      mMetadataLock.writeLock().unlock();
    }
  }

  /**
   * Increases the temp block size only if there is enough available space in its parent dir.
   * Returns the location needs to be free if it fails.
   */
  private BlockStoreLocation requestSpaceInternal(long blockId, long additionalBytes)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException,
      InvalidStateException {
    mMetadataLock.writeLock().lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      if (tempBlockMeta.getParentDir().getAvailableBytes() < additionalBytes) {
        return tempBlockMeta.getBlockLocation();
      }
      // Increase the size of this temp block
      mMetaManager.resizeTempBlockMeta(tempBlockMeta,
          tempBlockMeta.getBlockSize() + additionalBytes);
      return null;
    } finally {
      mMetadataLock.writeLock().unlock();
    }
  }

  /**
   * This method must be guarded by WRITE lock of mEvictionLock
   */
  private void freeSpaceInternal(long userId, long availableBytes, BlockStoreLocation location)
      throws OutOfSpaceException, IOException, NotFoundException, AlreadyExistsException,
      InvalidStateException {
    EvictionPlan plan;
    mMetadataLock.readLock().lock();
    try {
      plan = mEvictor.freeSpaceWithView(availableBytes, location, getUpdatedView());
      // Absent plan means failed to evict enough space.
      if (null == plan) {
        throw new OutOfSpaceException("Failed to free space: no eviction plan by evictor");
      }
    } finally {
      mMetadataLock.readLock().unlock();
    }

    // 1. remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      removeBlockInternal(userId, blockId);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onRemoveBlockByWorker(userId, blockId);
        }
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
        Pair<BlockStoreLocation, BlockStoreLocation> locationPair =
            moveBlockInternal(userId, blockId, newLocation);
        BlockStoreLocation oldLocation = locationPair.getFirst();
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByWorker(userId, blockId, oldLocation, newLocation);
          }
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

  /**
   * Moves a block to new location only if allocator finds available space in newLocation. This
   * method will not trigger any eviction.
   */
  private Pair<BlockStoreLocation, BlockStoreLocation> moveBlockInternal(long userId, long blockId,
      BlockStoreLocation newLocation) throws NotFoundException, AlreadyExistsException,
          InvalidStateException, OutOfSpaceException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      String srcFilePath;
      String dstFilePath;
      BlockMeta srcBlockMeta;
      BlockMeta dstBlockMeta;
      BlockStoreLocation srcLocation;
      BlockStoreLocation dstLocation;
      mMetadataLock.writeLock().lock();
      try {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidStateException(
              "Failed to move block " + blockId + ": block is uncommited");
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        long blockSize = srcBlockMeta.getBlockSize();
        TempBlockMeta tempBlock = mAllocator.allocateBlockWithView(userId, blockId, blockSize,
            newLocation, getUpdatedView());
        if (tempBlock == null) {
          // Allocator fails to find a proper place in newLocation to move this block.
          return null;
        }
        try {
          mMetaManager.moveBlockMeta(srcBlockMeta, tempBlock);
        } catch (OutOfSpaceException ose) {
          // If we reach here, allocator is not working properly
          LOG.error("Unexpected failure: " + blockSize + " bytes allocated at " + newLocation
              + " by allocator, but moveBlockMeta failed");
          throw Throwables.propagate(ose);
        } catch (AlreadyExistsException aee) {
          // If we reach here, allocator is not working properly
          LOG.error("Unexpected failure: " + blockSize + " bytes allocated at " + newLocation
              + " by allocator, but moveBlockMeta failed");
          throw Throwables.propagate(aee);
        }
        dstBlockMeta = mMetaManager.getBlockMeta(blockId);
        dstLocation = dstBlockMeta.getBlockLocation();
        dstFilePath = dstBlockMeta.getPath();
      } finally {
        mMetadataLock.writeLock().unlock();
      }
      // Heavy IO operation, still guarded by block lock but not metadata lock.
      FileUtils.move(new File(srcFilePath), new File(dstFilePath));
      return new Pair<BlockStoreLocation, BlockStoreLocation>(srcLocation, dstLocation);
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Remove a block. This method requires block lock in WRITE mode and eviction lock in READ mode.
   */
  private void removeBlockInternal(long userId, long blockId)
      throws InvalidStateException, NotFoundException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      String filePath;
      mMetadataLock.writeLock().lock();
      try {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidStateException(
              "Failed to remove block " + blockId + ": block is uncommitted");
        }
        BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
        filePath = blockMeta.getPath();
        mMetaManager.removeBlockMeta(blockMeta);
      } finally {
        // If we fail to lock, the block is no longer in tiered store
        mMetadataLock.writeLock().unlock();
      }

      // Heavy IO operation, still guarded by block lock but not metadata lock.
      FileUtils.delete(new File(filePath));
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * updates the pinned blocks
   *
   * @param inodes, a set of IDs inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Integer> inodes) {
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }
}
