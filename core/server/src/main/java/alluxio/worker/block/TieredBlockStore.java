/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.collections.Pair;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.evictor.BlockTransferInfo;
import alluxio.worker.block.evictor.EvictionPlan;
import alluxio.worker.block.evictor.Evictor;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.NotThreadSafe;

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
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  // TODO(bin): Change maxRetry to be configurable.
  private static final int MAX_RETRIES = 3;

  private final Configuration mConfiguration;
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;
  private final Evictor mEvictor;

  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new ArrayList<BlockStoreEventListener>();

  /** A set of pinned inodes fetched from the master. */
  private final Set<Long> mPinnedInodes = new HashSet<Long>();

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  /** ReadLock provided by {@link #mMetadataReadLock} to guard metadata read operations. */
  private final Lock mMetadataReadLock = mMetadataLock.readLock();

  /** WriteLock provided by {@link #mMetadataReadLock} to guard metadata write operations. */
  private final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  /** Association between storage tier aliases and ordinals. */
  private final StorageTierAssoc mStorageTierAssoc;

  /**
   * Creates a new instance of {@link TieredBlockStore}.
   */
  public TieredBlockStore() {
    mConfiguration = WorkerContext.getConf();
    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    mLockManager = new BlockLockManager();

    BlockMetadataManagerView initManagerView = new BlockMetadataManagerView(mMetaManager,
        Collections.<Long>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.create(mConfiguration, initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    initManagerView = new BlockMetadataManagerView(mMetaManager, Collections.<Long>emptySet(),
        Collections.<Long>emptySet());
    mEvictor = Evictor.Factory.create(mConfiguration, initManagerView, mAllocator);
    if (mEvictor instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    }

    mStorageTierAssoc = new WorkerStorageTierAssoc(mConfiguration);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
    mMetadataReadLock.lock();
    boolean hasBlock = mMetaManager.hasBlockMeta(blockId);
    mMetadataReadLock.unlock();
    if (hasBlock) {
      return lockId;
    }
    mLockManager.unlockBlock(lockId);
    throw new BlockDoesNotExistException(
        ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION, blockId, sessionId);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mLockManager.unlockBlock(lockId);
  }

  @Override
  public void unlockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mLockManager.unlockBlock(sessionId, blockId);
  }

  @Override
  public BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, IOException {
    // NOTE: a temp block is supposed to only be visible by its own writer, unnecessary to acquire
    // block lock here since no sharing
    // TODO(bin): Handle the case where multiple writers compete for the same block.
    mMetadataReadLock.lock();
    try {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      return new LocalFileBlockWriter(tempBlockMeta.getPath());
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public BlockReader getBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mLockManager.validateLock(sessionId, blockId, lockId);
    mMetadataReadLock.lock();
    try {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      return new LocalFileBlockReader(blockMeta.getPath());
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public TempBlockMeta createBlockMeta(long sessionId, long blockId, BlockStoreLocation location,
      long initialBlockSize)
          throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      TempBlockMeta tempBlockMeta =
          createBlockMetaInternal(sessionId, blockId, location, initialBlockSize, true);
      if (tempBlockMeta != null) {
        return tempBlockMeta;
      }
      if (i < MAX_RETRIES) {
        // Failed to create a temp block, so trigger Evictor to make some space.
        // NOTE: a successful {@link freeSpaceInternal} here does not ensure the subsequent
        // allocation also successful, because these two operations are not atomic.
        freeSpaceInternal(sessionId, initialBlockSize, location);
      }
    }
    // TODO(bin): We are probably seeing a rare transient failure, maybe define and throw some
    // other types of exception to indicate this case.
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION,
        initialBlockSize, MAX_RETRIES, blockId);
  }

  // TODO(bin): Make this method to return a snapshot.
  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    mMetadataReadLock.lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    mLockManager.validateLock(sessionId, blockId, lockId);
    mMetadataReadLock.lock();
    try {
      return mMetaManager.getBlockMeta(blockId);
    } finally {
      mMetadataReadLock.unlock();
    }
  }

  @Override
  public void commitBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    BlockStoreLocation loc = commitBlockInternal(sessionId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onCommitBlock(sessionId, blockId, loc);
      }
    }
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    abortBlockInternal(sessionId, blockId);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAbortBlock(sessionId, blockId);
      }
    }
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      Pair<Boolean, BlockStoreLocation> requestResult =
          requestSpaceInternal(blockId, additionalBytes);
      if (requestResult.getFirst()) {
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(sessionId, additionalBytes, requestResult.getSecond());
      }
    }
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION,
        additionalBytes, MAX_RETRIES, blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    moveBlock(sessionId, blockId, BlockStoreLocation.anyTier(), newLocation);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation)
          throws BlockDoesNotExistException, BlockAlreadyExistsException,
          InvalidWorkerStateException, WorkerOutOfSpaceException, IOException {
    for (int i = 0; i < MAX_RETRIES + 1; i++) {
      MoveBlockResult moveResult = moveBlockInternal(sessionId, blockId, oldLocation, newLocation);
      if (moveResult.getSuccess()) {
        synchronized (mBlockStoreEventListeners) {
          for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
            listener.onMoveBlockByClient(sessionId, blockId, moveResult.getSrcLocation(),
                moveResult.getDstLocation());
          }
        }
        return;
      }
      if (i < MAX_RETRIES) {
        freeSpaceInternal(sessionId, moveResult.getBlockSize(), newLocation);
      }
    }
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE, newLocation,
        blockId, MAX_RETRIES);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    removeBlock(sessionId, blockId, BlockStoreLocation.anyTier());
  }

  @Override
  public void removeBlock(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    removeBlockInternal(sessionId, blockId, location);
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onRemoveBlockByClient(sessionId, blockId);
      }
    }
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mMetadataReadLock.lock();
    boolean hasBlock = mMetaManager.hasBlockMeta(blockId);
    mMetadataReadLock.unlock();
    if (!hasBlock) {
      throw new BlockDoesNotExistException(ExceptionMessage.NO_BLOCK_ID_FOUND, blockId);
    }
    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onAccessBlock(sessionId, blockId);
      }
    }
  }

  @Override
  public void freeSpace(long sessionId, long availableBytes, BlockStoreLocation location)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    // TODO(bin): Consider whether to retry here.
    freeSpaceInternal(sessionId, availableBytes, location);
  }

  @Override
  public void cleanupSession(long sessionId) {
    // Release all locks the session is holding.
    mLockManager.cleanupSession(sessionId);

    // Collect a list of temp blocks the given session owns and abort all of them with best effort
    List<TempBlockMeta> tempBlocksToRemove;
    mMetadataReadLock.lock();
    try {
      tempBlocksToRemove = mMetaManager.getSessionTempBlocks(sessionId);
    } finally {
      mMetadataReadLock.unlock();
    }
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      try {
        abortBlockInternal(sessionId, tempBlockMeta.getBlockId());
      } catch (Exception e) {
        LOG.error("Failed to cleanup tempBlock {} due to {}", tempBlockMeta.getBlockId(),
            e.getMessage());
      }
    }

    // A session may create multiple temporary directories for temp blocks, in different StorageTier
    // and StorageDir. Go through all the storage directories and delete the session folders which
    // should be empty
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        String sessionFolderPath = PathUtils.concatPath(dir.getDirPath(), sessionId);
        try {
          if (new File(sessionFolderPath).exists()) {
            Files.delete(Paths.get(sessionFolderPath));
          }
        } catch (IOException e) {
          // This error means we could not delete the directory but should not affect the
          // correctness of the method since the data has already been deleted. It is not
          // necessary to throw an exception here.
          LOG.error("Failed to clean up session: {} with directory: {}", sessionId,
              sessionFolderPath);
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
   * Checks if a block id is available for a new temp block. This method must be enclosed by
   * {@link #mMetadataLock}.
   *
   * @param blockId the id of block
   * @throws BlockAlreadyExistsException if block id already exists
   */
  private void checkTempBlockIdAvailable(long blockId) throws BlockAlreadyExistsException {
    if (mMetaManager.hasTempBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_EXISTS, blockId);
    }
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
  }

  /**
   * Checks if block id is a temporary block and owned by session id. This method must be enclosed
   * by {@link #mMetadataLock}.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   */
  private void checkTempBlockOwnedBySession(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException {
    if (mMetaManager.hasBlockMeta(blockId)) {
      throw new BlockAlreadyExistsException(ExceptionMessage.TEMP_BLOCK_ID_COMMITTED, blockId);
    }
    TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    long ownerSessionId = tempBlockMeta.getSessionId();
    if (ownerSessionId != sessionId) {
      throw new InvalidWorkerStateException(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION,
          blockId, ownerSessionId, sessionId);
    }
  }

  /**
   * Aborts a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   * @throws IOException if I/O errors occur when deleting the block file
   */
  private void abortBlockInternal(long sessionId, long blockId) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      String path;
      TempBlockMeta tempBlockMeta;
      mMetadataReadLock.lock();
      try {
        checkTempBlockOwnedBySession(sessionId, blockId);
        tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
        path = tempBlockMeta.getPath();
      } finally {
        mMetadataReadLock.unlock();
      }

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      Files.delete(Paths.get(path));

      mMetadataWriteLock.lock();
      try {
        mMetaManager.abortTempBlockMeta(tempBlockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // We shall never reach here
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
   * @param sessionId the id of session
   * @param blockId the id of block
   * @return destination location to move the block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   * @throws IOException if I/O errors occur when deleting the block file
   */
  private BlockStoreLocation commitBlockInternal(long sessionId, long blockId)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
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
        checkTempBlockOwnedBySession(sessionId, blockId);
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
      } catch (BlockAlreadyExistsException e) {
        throw Throwables.propagate(e); // we shall never reach here
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // we shall never reach here
      } catch (WorkerOutOfSpaceException e) {
        throw Throwables.propagate(e); // we shall never reach here
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
   * @param sessionId session Id
   * @param blockId block Id
   * @param location location to create the block
   * @param initialBlockSize initial block size in bytes
   * @param newBlock true if this temp block is created for a new block
   * @return a temp block created if successful, or null if allocation failed (instead of throwing
   *         {@link WorkerOutOfSpaceException} because allocation failure could be an expected case)
   * @throws BlockAlreadyExistsException if there is already a block with the same block id
   */
  private TempBlockMeta createBlockMetaInternal(long sessionId, long blockId,
      BlockStoreLocation location, long initialBlockSize, boolean newBlock)
          throws BlockAlreadyExistsException {
    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    mMetadataWriteLock.lock();
    try {
      if (newBlock) {
        checkTempBlockIdAvailable(blockId);
      }
      StorageDirView dirView =
          mAllocator.allocateBlockWithView(sessionId, initialBlockSize, location, getUpdatedView());
      if (dirView == null) {
        // Allocator fails to find a proper place for this new block.
        return null;
      }
      // TODO(carson): Add tempBlock to corresponding storageDir and remove the use of
      // StorageDirView.createTempBlockMeta.
      TempBlockMeta tempBlock = dirView.createTempBlockMeta(sessionId, blockId, initialBlockSize);
      try {
        // Add allocated temp block to metadata manager. This should never fail if allocator
        // correctly assigns a StorageDir.
        mMetaManager.addTempBlockMeta(tempBlock);
      } catch (WorkerOutOfSpaceException e) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: {} bytes allocated at {} by allocator, "
            + "but addTempBlockMeta failed", initialBlockSize, location);
        throw Throwables.propagate(e);
      } catch (BlockAlreadyExistsException e) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: {} bytes allocated at {} by allocator, "
            + "but addTempBlockMeta failed", initialBlockSize, location);
        throw Throwables.propagate(e);
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
   * @return a pair of boolean and {@link BlockStoreLocation}. The boolean indicates if the
   *         operation succeeds and the {@link BlockStoreLocation} denotes where to free more space
   *         if it fails.
   * @throws BlockDoesNotExistException if this block is not found
   */
  private Pair<Boolean, BlockStoreLocation> requestSpaceInternal(long blockId, long additionalBytes)
      throws BlockDoesNotExistException {
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
      } catch (InvalidWorkerStateException e) {
        throw Throwables.propagate(e); // we shall never reach here
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
   * @param sessionId the session Id
   * @param availableBytes amount of space in bytes to free
   * @param location location of space
   * @throws WorkerOutOfSpaceException if it is impossible to achieve the free requirement
   * @throws IOException if I/O errors occur when removing or moving block files
   */
  private void freeSpaceInternal(long sessionId, long availableBytes, BlockStoreLocation location)
      throws WorkerOutOfSpaceException, IOException {
    EvictionPlan plan;
    mMetadataReadLock.lock();
    try {
      plan = mEvictor.freeSpaceWithView(availableBytes, location, getUpdatedView());
      // Absent plan means failed to evict enough space.
      if (plan == null) {
        throw new WorkerOutOfSpaceException(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE);
      }
    } finally {
      mMetadataReadLock.unlock();
    }

    // 1. remove blocks to make room.
    for (Pair<Long, BlockStoreLocation> blockInfo : plan.toEvict()) {
      try {
        removeBlockInternal(sessionId, blockInfo.getFirst(), blockInfo.getSecond());
      } catch (InvalidWorkerStateException e) {
        // Evictor is not working properly
        LOG.error("Failed to evict blockId {}, this is temp block", blockInfo.getFirst());
        continue;
      } catch (BlockDoesNotExistException e) {
        LOG.info("Failed to evict blockId {}, it could be already deleted", blockInfo.getFirst());
        continue;
      }
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onRemoveBlockByWorker(sessionId, blockInfo.getFirst());
        }
      }
    }
    // 2. transfer blocks among tiers.
    // 2.1. group blocks move plan by the destination tier.
    Map<String, Set<BlockTransferInfo>> blocksGroupedByDestTier =
        new HashMap<String, Set<BlockTransferInfo>>();
    for (BlockTransferInfo entry : plan.toMove()) {
      String alias = entry.getDstLocation().tierAlias();
      if (!blocksGroupedByDestTier.containsKey(alias)) {
        blocksGroupedByDestTier.put(alias, new HashSet<BlockTransferInfo>());
      }
      blocksGroupedByDestTier.get(alias).add(entry);
    }
    // 2.2. move blocks in the order of their dst tiers, from bottom to top
    for (int tierOrdinal = mStorageTierAssoc.size() - 1; tierOrdinal >= 0; --tierOrdinal) {
      Set<BlockTransferInfo> toMove =
          blocksGroupedByDestTier.get(mStorageTierAssoc.getAlias(tierOrdinal));
      if (toMove == null) {
        toMove = new HashSet<BlockTransferInfo>();
      }
      for (BlockTransferInfo entry : toMove) {
        long blockId = entry.getBlockId();
        BlockStoreLocation oldLocation = entry.getSrcLocation();
        BlockStoreLocation newLocation = entry.getDstLocation();
        MoveBlockResult moveResult;
        try {
          moveResult = moveBlockInternal(sessionId, blockId, oldLocation, newLocation);
        } catch (InvalidWorkerStateException e) {
          // Evictor is not working properly
          LOG.error("Failed to evict blockId {}, this is temp block", blockId);
          continue;
        } catch (BlockAlreadyExistsException e) {
          continue;
        } catch (BlockDoesNotExistException e) {
          LOG.info("Failed to move blockId {}, it could be already deleted", blockId);
          continue;
        }
        if (moveResult.getSuccess()) {
          synchronized (mBlockStoreEventListeners) {
            for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
              listener.onMoveBlockByWorker(sessionId, blockId, moveResult.getSrcLocation(),
                  newLocation);
            }
          }
        }
      }
    }
  }

  /**
   * Gets the most updated view with most recent information on pinned inodes, and currently locked
   * blocks.
   *
   * @return {@link BlockMetadataManagerView}, an updated view with most recent information
   */
  private BlockMetadataManagerView getUpdatedView() {
    // TODO(calvin): Update the view object instead of creating new one every time.
    synchronized (mPinnedInodes) {
      return new BlockMetadataManagerView(mMetaManager, mPinnedInodes,
          mLockManager.getLockedBlocks());
    }
  }

  /**
   * Moves a block to new location only if allocator finds available space in newLocation. This
   * method will not trigger any eviction. Returns {@link MoveBlockResult}.
   *
   * @param sessionId session Id
   * @param blockId block Id
   * @param oldLocation the source location of the block
   * @param newLocation new location to move this block
   * @return the resulting information about the move operation
   * @throws BlockDoesNotExistException if block is not found
   * @throws BlockAlreadyExistsException if a block with same Id already exists in new location
   * @throws InvalidWorkerStateException if the block to move is a temp block
   * @throws IOException if I/O errors occur when moving block file
   */
  private MoveBlockResult moveBlockInternal(long sessionId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation)
          throws BlockDoesNotExistException, BlockAlreadyExistsException,
          InvalidWorkerStateException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
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
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
      } finally {
        mMetadataReadLock.unlock();
      }

      if (!srcLocation.belongsTo(oldLocation)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            oldLocation);
      }
      TempBlockMeta dstTempBlock =
          createBlockMetaInternal(sessionId, blockId, newLocation, blockSize, false);
      if (dstTempBlock == null) {
        return new MoveBlockResult(false, blockSize, null, null);
      }

      // When `newLocation` is some specific location, the `newLocation` and the `dstLocation` are
      // just the same; while for `newLocation` with a wildcard significance, the `dstLocation`
      // is a specific one with specific tier and dir which belongs to newLocation.
      dstLocation = dstTempBlock.getBlockLocation();

      // When the dstLocation belongs to srcLocation, simply abort the tempBlockMeta just created
      // internally from the newLocation and return success with specific block location.
      if (dstLocation.belongsTo(srcLocation)) {
        mMetaManager.abortTempBlockMeta(dstTempBlock);
        return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
      }
      dstFilePath = dstTempBlock.getCommitPath();

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcFilePath, dstFilePath);

      mMetadataWriteLock.lock();
      try {
        // If this metadata update fails, we panic for now.
        // TODO(bin): Implement rollback scheme to recover from IO failures.
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
      } catch (BlockAlreadyExistsException e) {
        throw Throwables.propagate(e); // we shall never reach here
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // we shall never reach here
      } catch (WorkerOutOfSpaceException e) {
        // Only possible if session id gets cleaned between createBlockMetaInternal and
        // moveBlockMeta.
        throw Throwables.propagate(e);
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
   * @param sessionId session Id
   * @param blockId block Id
   * @param location the source location of the block
   * @throws InvalidWorkerStateException if the block to remove is a temp block
   * @throws BlockDoesNotExistException if this block can not be found
   * @throws IOException if I/O errors occur when removing this block file
   */
  private void removeBlockInternal(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      String filePath;
      BlockMeta blockMeta;
      mMetadataReadLock.lock();
      try {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK, blockId);
        }
        blockMeta = mMetaManager.getBlockMeta(blockId);
        filePath = blockMeta.getPath();
      } finally {
        mMetadataReadLock.unlock();
      }

      if (!blockMeta.getBlockLocation().belongsTo(location)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            location);
      }
      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      Files.delete(Paths.get(filePath));

      mMetadataWriteLock.lock();
      try {
        mMetaManager.removeBlockMeta(blockMeta);
      } catch (BlockDoesNotExistException e) {
        throw Throwables.propagate(e); // we shall never reach here
      } finally {
        mMetadataWriteLock.unlock();
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Updates the pinned blocks.
   *
   * @param inodes a set of ids inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }

  /**
   * A wrapper on necessary info after a move block operation.
   */
  private static class MoveBlockResult {
    /** Whether this move operation succeeds. */
    private final boolean mSuccess;
    /** Size of this block in bytes. */
    private final long mBlockSize;
    /** Source location of this block to move. */
    private final BlockStoreLocation mSrcLocation;
    /** Destination location of this block to move. */
    private final BlockStoreLocation mDstLocation;

    /**
     * Creates a new instance of {@link MoveBlockResult}.
     *
     * @param success success indication
     * @param blockSize block size
     * @param srcLocation source location
     * @param dstLocation destination location
     */
    MoveBlockResult(boolean success, long blockSize, BlockStoreLocation srcLocation,
        BlockStoreLocation dstLocation) {
      mSuccess = success;
      mBlockSize = blockSize;
      mSrcLocation = srcLocation;
      mDstLocation = dstLocation;
    }

    /**
     * @return the success indicator
     */
    boolean getSuccess() {
      return mSuccess;
    }

    /**
     * @return the block size
     */
    long getBlockSize() {
      return mBlockSize;
    }

    /**
     * @return the source location
     */
    BlockStoreLocation getSrcLocation() {
      return mSrcLocation;
    }

    /**
     * @return the destination location
     */
    BlockStoreLocation getDstLocation() {
      return mDstLocation;
    }
  }
}
