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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.conf.TachyonConf;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.util.io.FileUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.evictor.EvictionPlan;
import tachyon.worker.block.evictor.Evictor;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockReader;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This class represents an object store that manages all the blocks in the local tiered storage.
 * This store exposes simple public APIs to operate blocks. Inside this store, it creates an
 * Allocator to decide where to put a new block, an Evictor to decide where to evict a stale block,
 * a BlockMetadataManager to maintain the status of the tiered storage, and a LockManager to
 * coordinate read/write on the same block.
 * <p>
 * This class is thread-safe, using the following lock hierarchy to ensure thread-safety:
 * <ul>
 * <li>Any block-level operation (e.g., read, move or remove) on an existing block must acquire a
 * block lock for this block via {@link TieredBlockStore#mLockManager}. This block lock is a
 * read/write lock, guarding both the metadata operations and the following I/O on this block. It
 * coordinates different threads (clients) when accessing the same block concurrently.</li>
 * <li>Any metadata operation (read or write) must go through {@link TieredBlockStore#mMetaManager}
 * and guarded by {@link TieredBlockStore#mMetadataLock}. This is also a read/write lock and
 * coordinates different threads (clients) when accessing the shared data structure for metadata.
 * </li>
 * <li>Method {@link #createBlockMeta} does not acquire the block lock, because it only creates a
 * temp block which is only visible to its writer before committed (thus no concurrent access).</li>
 * <li>Eviction is done in {@link #freeSpaceInternal} and it is on the basis of best effort. For
 * operations that may trigger this eviction (e.g., move, create, requestSpace), retry is used</li>
 * </ul>
 */
public final class TieredBlockStore implements BlockStore {
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
  /** A set of pinned inodes fetched from the master */
  private final Set<Long> mPinnedInodes = new HashSet<Long>();
  /** Lock to guard metadata operations */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();
  /** ReadLock provided by {@link #mMetadataReadLock} to guard metadata read operations */
  private final Lock mMetadataReadLock = mMetadataLock.readLock();
  /** WriteLock provided by {@link #mMetadataReadLock} to guard metadata write operations */
  private final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  public TieredBlockStore() {
    mTachyonConf = WorkerContext.getConf();
    mMetaManager = BlockMetadataManager.newBlockMetadataManager();
    mLockManager = new BlockLockManager();

    BlockMetadataManagerView initManagerView = new BlockMetadataManagerView(mMetaManager,
        Collections.<Long>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.createAllocator(mTachyonConf, initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    initManagerView = new BlockMetadataManagerView(mMetaManager, Collections.<Long>emptySet(),
        Collections.<Long>emptySet());
    mEvictor = Evictor.Factory.createEvictor(mTachyonConf, initManagerView, mAllocator);
    if (mEvictor instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    }
  }

  @Override
  public long lockBlock(long userId, long blockId) throws NotFoundException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.READ);
    mMetadataReadLock.lock();
    boolean hasBlock = mMetaManager.hasBlockMeta(blockId);
    mMetadataReadLock.unlock();
    if (hasBlock) {
      return lockId;
    }
    mLockManager.unlockBlock(lockId);
    throw new NotFoundException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_USER, blockId,
        userId);
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
    // NOTE: a temp block is supposed to only be visible by its own writer, unnecessary to acquire
    // block lock here since no sharing
    // TODO: handle the case where multiple writers compete for the same block
    mMetadataReadLock.lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      return new LocalFileBlockWriter(tempBlockMeta);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public BlockReader getBlockReader(long userId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException, IOException {
    mLockManager.validateLock(userId, blockId, lockId);
    mMetadataReadLock.lock();
    try {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      return new LocalFileBlockReader(blockMeta);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public TempBlockMeta createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws AlreadyExistsException, OutOfSpaceException, NotFoundException,
      IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i ++) {
      TempBlockMeta tempBlockMeta =
          createBlockMetaInternal(userId, blockId, location, initialBlockSize, true);
      if (tempBlockMeta != null) {
        return tempBlockMeta;
      }
      if (i < MAX_RETRIES) {
        // Failed to create a temp block, so trigger Evictor to make some space.
        // NOTE: a successful {@link freeSpaceInternal} here does not ensure the subsequent
        // allocation also successful, because these two operations are not atomic.
        freeSpaceInternal(userId, initialBlockSize, location);
      }
    }
    // TODO: we are probably seeing a rare transient failure, maybe define and throw some other
    // types of exception to indicate this case.
    throw new OutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION, initialBlockSize,
        MAX_RETRIES, blockId);
  }

  // TODO: make this method to return a snapshot
  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws NotFoundException {
    mMetadataReadLock.lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public BlockMeta getBlockMeta(long userId, long blockId, long lockId) throws NotFoundException,
      InvalidStateException {
    mLockManager.validateLock(userId, blockId, lockId);
    mMetadataReadLock.lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public void commitBlock(long userId, long blockId) throws AlreadyExistsException,
      InvalidStateException, NotFoundException, IOException {
    BlockStoreLocation loc = commitBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onCommitBlock(userId, blockId, loc);
      }
    }
  }

  @Override
  public void abortBlock(long userId, long blockId) throws AlreadyExistsException,
      NotFoundException, InvalidStateException, IOException {
    abortBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAbortBlock(userId, blockId);
      }
    }
  }

  @Override
  public void requestSpace(long userId, long blockId, long additionalBytes)
      throws NotFoundException, OutOfSpaceException, IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i ++) {
      Pair<Boolean, BlockStoreLocation> requestResult =
          requestSpaceInternal(blockId, additionalBytes);
      if (requestResult.getFirst()) {
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(userId, additionalBytes, requestResult.getSecond());
      }
    }
    throw new OutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION, additionalBytes,
        MAX_RETRIES, blockId);
  }

  @Override
  public void moveBlock(long userId, long blockId, BlockStoreLocation newLocation)
      throws NotFoundException, AlreadyExistsException, InvalidStateException, OutOfSpaceException,
      IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i ++) {
      MoveBlockResult moveResult = moveBlockInternal(userId, blockId, newLocation);
      if (moveResult.success()) {
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByClient(userId, blockId, moveResult.srcLocation(),
                moveResult.dstLocation());
          }
        }
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(userId, moveResult.blockSize(), newLocation);
      }
    }
    throw new OutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE, newLocation, blockId,
        MAX_RETRIES);
  }

  @Override
  public void removeBlock(long userId, long blockId) throws InvalidStateException,
      NotFoundException, IOException {
    removeBlockInternal(userId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onRemoveBlockByClient(userId, blockId);
      }
    }
  }

  @Override
  public void accessBlock(long userId, long blockId) throws NotFoundException {
    mMetadataReadLock.lock();
    boolean hasBlock = mMetaManager.hasBlockMeta(blockId);
    mMetadataReadLock.unlock();
    if (!hasBlock) {
      throw new NotFoundException(ExceptionMessage.NO_BLOCK_ID_FOUND, blockId);
    }
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAccessBlock(userId, blockId);
      }
    }
  }

  @Override
  public void freeSpace(long userId, long availableBytes, BlockStoreLocation location)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException {
    // TODO: consider whether to retry here
    freeSpaceInternal(userId, availableBytes, location);
  }

  @Override
  public void cleanupUser(long userId) {
    // Release all locks the user is holding.
    mLockManager.cleanupUser(userId);

    // Collect a list of temp blocks the given user owns and abort all of them with best effort
    List<TempBlockMeta> tempBlocksToRemove;
    mMetadataReadLock.lock();
    try {
      tempBlocksToRemove = mMetaManager.getUserTempBlocks(userId);
    } finally {
      mMetadataReadLock.unlock();
    }
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      try {
        abortBlockInternal(userId, tempBlockMeta.getBlockId());
      } catch (Exception e) {
        LOG.error("Failed to cleanup tempBlock " + tempBlockMeta.getBlockId() + " due to "
            + e.getMessage());
      }
    }

    // A user may create multiple temporary directories for temp blocks, in different StorageTier
    // and StorageDir. Go through all the storage directories and delete the user folders which
    // should be empty
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        String userFolderPath = PathUtils.concatPath(dir.getDirPath(), userId);
        try {
          if (new File(userFolderPath).exists()) {
            FileUtils.delete(userFolderPath);
          }
        } catch (IOException ioe) {
          // This error means we could not delete the directory but should not affect the
          // correctness of the method since the data has already been deleted. It is not
          // necessary to throw an exception here.
          LOG.error("Failed to clean up user: {} with directory: {}", userId, userFolderPath);
        }
      }
    }
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    mMetadataReadLock.lock();
    boolean hasBlock = mMetaManager.hasBlockMeta(blockId);
    mMetadataReadLock.unlock();
    return hasBlock;
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    mMetadataReadLock.lock();
    BlockStoreMeta storeMeta = mMetaManager.getBlockStoreMeta();
    mMetadataReadLock.unlock();
    return storeMeta;
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    synchronized (mBlockStoreEventListeners) {
      mBlockStoreEventListeners.add(listener);
    }
  }

  /**
   * Checks if a blockId is available for a new temp block. This method must be enclosed by
   * {@link #mMetadataLock}.
   *
   * @param blockId the ID of block
   * @throws AlreadyExistsException if blockId already exists
   */
  private void checkTempBlockIdAvailable(long blockId) throws AlreadyExistsException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new AlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
    }
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new AlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
  }

  /**
   * Checks if blockId is a temporary block and owned by userId. This method must be enclosed by
   * {@link #mMetadataLock}.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @throws NotFoundException if blockId can not be found in temporary blocks
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws InvalidStateException if blockId is not owned by userId
   */
  private void checkTempBlockOwnedByUser(long userId, long blockId) throws NotFoundException,
      AlreadyExistsException, InvalidStateException {
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new AlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    long ownerUserId = tempBlockMeta.getUserId();
    if (ownerUserId != userId) {
      throw new InvalidStateException(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_USER, blockId,
          ownerUserId, userId);
    }
  }

  /**
   * Aborts a temp block.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @throws NotFoundException if blockId can not be found in temporary blocks
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws InvalidStateException if blockId is not owned by userId
   * @throws IOException if I/O errors occur when deleting the block file
   */
  private void abortBlockInternal(long userId, long blockId) throws NotFoundException,
      AlreadyExistsException, InvalidStateException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      String path;
      TempBlockMeta tempBlockMeta;
      mMetadataReadLock.lock();
      try {
        checkTempBlockOwnedByUser(userId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        path = tempBlockMeta.getPath();
      } finally {
        mMetadataReadLock.unlock();
      }

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.delete(path);

      mMetadataWriteLock.lock();
      try {
        mMetaManager.abortTempBlockMeta(tempBlockMeta);
      } catch (NotFoundException nfe) {
        throw Throwables.propagate(nfe); // We shall never reach here
      } finally {
        mMetadataWriteLock.unlock();
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Commits a temp block.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @throws NotFoundException if blockId can not be found in temporary blocks
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws InvalidStateException if blockId is not owned by userId
   * @throws IOException if I/O errors occur when deleting the block file
   * @return destination location to move the block
   */
  private BlockStoreLocation commitBlockInternal(long userId, long blockId)
      throws AlreadyExistsException, InvalidStateException, NotFoundException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      // When committing TempBlockMeta, the final BlockMeta calculates the block size according to
      // the actual file size of this TempBlockMeta. Therefore, commitTempBlockMeta must happen
      // after moving actual block file to its committed path.
      BlockStoreLocation loc;
      String srcPath;
      String dstPath;
      TempBlockMeta tempBlockMeta;
      mMetadataReadLock.lock();
      try {
        checkTempBlockOwnedByUser(userId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        srcPath = tempBlockMeta.getPath();
        dstPath = tempBlockMeta.getCommitPath();
        loc = tempBlockMeta.getBlockLocation();
      } finally {
        mMetadataReadLock.unlock();
      }

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcPath, dstPath);

      mMetadataWriteLock.lock();
      try {
        mMetaManager.commitTempBlockMeta(tempBlockMeta);
      } catch (AlreadyExistsException aee) {
        throw Throwables.propagate(aee); // we shall never reach here
      } catch (NotFoundException nfe) {
        throw Throwables.propagate(nfe); // we shall never reach here
      } catch (OutOfSpaceException ose) {
        throw Throwables.propagate(ose); // we shall never reach here
      } finally {
        mMetadataWriteLock.unlock();
      }
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
   * @param newBlock true if this temp block is created for a new block
   * @return a temp block created if successful, or null if allocation failed (instead of throwing
   *         OutOfSpaceException because allocation failure could be an expected case)
   * @throws AlreadyExistsException if there is already a block with the same block id
   */
  private TempBlockMeta createBlockMetaInternal(long userId, long blockId,
      BlockStoreLocation location, long initialBlockSize, boolean newBlock)
      throws AlreadyExistsException {
    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    mMetadataWriteLock.lock();
    try {
      if (newBlock) {
        checkTempBlockIdAvailable(blockId);
      }
      StorageDirView dirView =
          mAllocator.allocateBlockWithView(userId, initialBlockSize, location, getUpdatedView());
      if (dirView == null) {
        // Allocator fails to find a proper place for this new block.
        return null;
      }
      // TODO: Add tempBlock to corresponding storageDir and remove the use of
      // StorageDirView.createTempBlockMeta
      TempBlockMeta tempBlock = dirView.createTempBlockMeta(userId, blockId, initialBlockSize);
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
      mMetadataWriteLock.unlock();
    }
  }

  /**
   * Increases the temp block size only if this temp block's parent dir has enough available space.
   *
   * @param blockId block Id
   * @param additionalBytes additional bytes to request for this block
   * @return a pair of boolean and BlockStoreLocation. The boolean indicates if the operation
   *         succeeds and the BlockStoreLocation denotes where to free more space if it fails.
   * @throws NotFoundException if this block is not found
   */
  private Pair<Boolean, BlockStoreLocation> requestSpaceInternal(long blockId, long additionalBytes)
      throws NotFoundException {
    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    mMetadataWriteLock.lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      if (tempBlockMeta.getParentDir().getAvailableBytes() < additionalBytes) {
        return new Pair<Boolean, BlockStoreLocation>(false, tempBlockMeta.getBlockLocation());
      }
      // Increase the size of this temp block
      try {
        mMetaManager.resizeTempBlockMeta(tempBlockMeta,
            tempBlockMeta.getBlockSize() + additionalBytes);
      } catch (InvalidStateException ise) {
        throw Throwables.propagate(ise); // we shall never reach here
      }
      return new Pair<Boolean, BlockStoreLocation>(true, null);
    } finally {
      mMetadataWriteLock.unlock();
    }
  }

  /**
   * Tries to get an eviction plan to free a certain amount of space in the given location, and
   * carries out this plan with the best effort.
   *
   * @param userId the user Id
   * @param availableBytes amount of space in bytes to free
   * @param location location of space
   * @throws OutOfSpaceException if it is impossible to achieve the free requirement
   * @throws IOException if I/O errors occur when removing or moving block files
   */
  private void freeSpaceInternal(long userId, long availableBytes, BlockStoreLocation location)
      throws OutOfSpaceException, IOException {
    EvictionPlan plan;
    mMetadataReadLock.lock();
    try {
      plan = mEvictor.freeSpaceWithView(availableBytes, location, getUpdatedView());
      // Absent plan means failed to evict enough space.
      if (null == plan) {
        throw new OutOfSpaceException(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE);
      }
    } finally {
      mMetadataReadLock.unlock();
    }

    // 1. remove blocks to make room.
    for (long blockId : plan.toEvict()) {
      try {
        removeBlockInternal(userId, blockId);
      } catch (InvalidStateException ise) {
        // Evictor is not working properly
        LOG.error("Failed to evict blockId " + blockId + ", this is temp block");
        continue;
      } catch (NotFoundException nfe) {
        LOG.info("Failed to evict blockId " + blockId + ", it could be already deleted");
        continue;
      }
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
    List<Integer> dstTierAlias = new ArrayList<Integer>(blocksGroupedByDestTier.keySet());
    Collections.sort(dstTierAlias, Collections.reverseOrder());
    // 2.3. move blocks in the order of their dst tiers.
    for (int alias : dstTierAlias) {
      Set<Pair<Long, BlockStoreLocation>> toMove = blocksGroupedByDestTier.get(alias);
      for (Pair<Long, BlockStoreLocation> entry : toMove) {
        long blockId = entry.getFirst();
        BlockStoreLocation newLocation = entry.getSecond();
        MoveBlockResult moveResult;
        try {
          // TODO: this should also specify the src location
          moveResult = moveBlockInternal(userId, blockId, newLocation);
        } catch (InvalidStateException ise) {
          // Evictor is not working properly
          LOG.error("Failed to evict blockId " + blockId + ", this is temp block");
          continue;
        } catch (AlreadyExistsException aee) {
          continue;
        } catch (NotFoundException nfe) {
          LOG.info("Failed to move blockId " + blockId + ", it could be already deleted");
          continue;
        }
        if (moveResult.success()) {
          synchronized (mBlockStoreEventListeners) {
            for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
              listener.onMoveBlockByWorker(userId, blockId, moveResult.srcLocation(), newLocation);
            }
          }
        }
      }
    }
  }

  /**
   * Get the most updated view with most recent information on pinned inodes, and currently locked
   * blocks.
   *
   * @return BlockMetadataManagerView, an updated view with most recent information.
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
   * method will not trigger any eviction. Returns MoveBlockResult.
   *
   * @param userId user Id
   * @param blockId block Id
   * @param newLocation new location to move this block
   * @return the resulting information about the move operation
   * @throws NotFoundException if block is not found
   * @throws AlreadyExistsException if a block with same Id already exists in new location
   * @throws InvalidStateException if the block to move is a temp block
   * @throws IOException if I/O errors occur when moving block file
   */
  private MoveBlockResult moveBlockInternal(long userId, long blockId,
      BlockStoreLocation newLocation) throws NotFoundException, AlreadyExistsException,
      InvalidStateException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      long blockSize;
      String srcFilePath;
      String dstFilePath;
      BlockMeta srcBlockMeta;
      BlockStoreLocation srcLocation;
      BlockStoreLocation dstLocation;

      mMetadataReadLock.lock();
      try {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
      } finally {
        mMetadataReadLock.unlock();
      }

      TempBlockMeta dstTempBlock =
          createBlockMetaInternal(userId, blockId, newLocation, blockSize, false);
      if (dstTempBlock == null) {
        return new MoveBlockResult(false, blockSize, null, null);
      }
      dstLocation = dstTempBlock.getBlockLocation();
      dstFilePath = dstTempBlock.getCommitPath();

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcFilePath, dstFilePath);

      mMetadataWriteLock.lock();
      try {
        // If this metadata update fails, we panic for now.
        // TODO: implement rollback scheme to recover from IO failures
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
      } catch (AlreadyExistsException aee) {
        throw Throwables.propagate(aee); // we shall never reach here
      } catch (NotFoundException nfe) {
        throw Throwables.propagate(nfe); // we shall never reach here
      } catch (OutOfSpaceException ose) {
        // Only possible if userId gets cleaned between createBlockMetaInternal and moveBlockMeta.
        throw Throwables.propagate(ose);
      } finally {
        mMetadataWriteLock.unlock();
      }

      return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Removes a block.
   *
   * @param userId user Id
   * @param blockId block Id
   * @throws InvalidStateException if the block to remove is a temp block
   * @throws NotFoundException if this block can not be found
   * @throws IOException if I/O errors occur when removing this block file
   */
  private void removeBlockInternal(long userId, long blockId) throws InvalidStateException,
      NotFoundException, IOException {
    long lockId = mLockManager.lockBlock(userId, blockId, BlockLockType.WRITE);
    try {
      String filePath;
      BlockMeta blockMeta;
      mMetadataReadLock.lock();
      try {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidStateException(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK, blockId);
        }
        blockMeta = mMetaManager.getBlockMeta(blockId);
        filePath = blockMeta.getPath();
      } finally {
        mMetadataReadLock.unlock();
      }

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.delete(filePath);

      mMetadataWriteLock.lock();
      try {
        mMetaManager.removeBlockMeta(blockMeta);
      } catch (NotFoundException nfe) {
        throw Throwables.propagate(nfe); // we shall never reach here
      } finally {
        mMetadataWriteLock.unlock();
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * updates the pinned blocks
   *
   * @param inodes a set of IDs inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }

  /**
   * A wrapper on necessary info after a move block operation
   */
  private static class MoveBlockResult {
    /** Whether this move operation succeeds */
    private final boolean mSuccess;
    /** Size of this block in bytes */
    private final long mBlockSize;
    /** Source location of this block to move */
    private final BlockStoreLocation mSrcLocation;
    /** Destination location of this block to move */
    private final BlockStoreLocation mDstLocation;

    MoveBlockResult(boolean success, long blockSize, BlockStoreLocation srcLocation,
        BlockStoreLocation dstLocation) {
      mSuccess = success;
      mBlockSize = blockSize;
      mSrcLocation = srcLocation;
      mDstLocation = dstLocation;
    }

    boolean success() {
      return mSuccess;
    }

    long blockSize() {
      return mBlockSize;
    }

    BlockStoreLocation srcLocation() {
      return mSrcLocation;
    }

    BlockStoreLocation dstLocation() {
      return mDstLocation;
    }
  }
}
