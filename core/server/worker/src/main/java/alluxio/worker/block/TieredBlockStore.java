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

package alluxio.worker.block;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.master.block.BlockId;
import alluxio.resource.LockResource;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.StoreBlockReader;
import alluxio.worker.block.io.StoreBlockWriter;
import alluxio.worker.block.management.ManagementTaskCoordinator;
import alluxio.worker.block.management.DefaultStoreLoadTracker;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.annotator.BlockOrder;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
 * <li>Method {@link #createBlock} does not acquire the block lock, because it only creates a
 * temp block which is only visible to its writer before committed (thus no concurrent access).</li>
 * <li>Method {@link #abortBlock(long, long)} does not acquire the block lock, because only
 * temporary blocks can be aborted, and they are only visible to their writers (thus no concurrent
 * access).
 * <li>Eviction is done in {@link #freeSpaceInternal} and it is on the basis of best effort. For
 * operations that may trigger this eviction (e.g., move, create, requestSpace), retry is used</li>
 * </ul>
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public class TieredBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockStore.class);

  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;
  private final Allocator mAllocator;

  private final List<BlockStoreEventListener> mBlockStoreEventListeners = new ArrayList<>();

  /** A set of pinned inodes fetched from the master. */
  private final Set<Long> mPinnedInodes = new HashSet<>();

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  /** ReadLock provided by {@link #mMetadataLock} to guard metadata read operations. */
  private final Lock mMetadataReadLock = mMetadataLock.readLock();

  /** WriteLock provided by {@link #mMetadataLock} to guard metadata write operations. */
  private final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  /** Used to get iterators per locations. */
  private BlockIterator mBlockIterator;

  /** Management task coordinator. */
  private ManagementTaskCoordinator mTaskCoordinator;

  /**
   * Creates a new instance of {@link TieredBlockStore}.
   */
  public TieredBlockStore() {
    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    mLockManager = new BlockLockManager();

    mBlockIterator = mMetaManager.getBlockIterator();
    // Register listeners required by the block iterator.
    for (BlockStoreEventListener listener : mBlockIterator.getListeners()) {
      registerBlockStoreEventListener(listener);
    }

    BlockMetadataEvictorView initManagerView = new BlockMetadataEvictorView(mMetaManager,
        Collections.<Long>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.create(initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    // Initialize and start coordinator.
    mTaskCoordinator = new ManagementTaskCoordinator(this, mMetaManager,
        new DefaultStoreLoadTracker(), () -> getUpdatedView());
    mTaskCoordinator.start();
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    LOG.debug("lockBlock: sessionId={}, blockId={}", sessionId, blockId);
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
    boolean hasBlock;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      hasBlock = mMetaManager.hasBlockMeta(blockId);
    }
    if (hasBlock) {
      return lockId;
    }

    mLockManager.unlockBlock(lockId);
    throw new BlockDoesNotExistException(ExceptionMessage.NO_BLOCK_ID_FOUND, blockId);
  }

  @Override
  public long lockBlockNoException(long sessionId, long blockId) {
    LOG.debug("lockBlockNoException: sessionId={}, blockId={}", sessionId, blockId);
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.READ);
    boolean hasBlock;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      hasBlock = mMetaManager.hasBlockMeta(blockId);
    }
    if (hasBlock) {
      return lockId;
    }

    mLockManager.unlockBlockNoException(lockId);
    return BlockLockManager.INVALID_LOCK_ID;
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    LOG.debug("unlockBlock: lockId={}", lockId);
    mLockManager.unlockBlock(lockId);
  }

  @Override
  public boolean unlockBlock(long sessionId, long blockId) {
    LOG.debug("unlockBlock: sessionId={}, blockId={}", sessionId, blockId);
    return mLockManager.unlockBlock(sessionId, blockId);
  }

  @Override
  public BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    LOG.debug("getBlockWriter: sessionId={}, blockId={}", sessionId, blockId);
    // NOTE: a temp block is supposed to only be visible by its own writer, unnecessary to acquire
    // block lock here since no sharing
    // TODO(bin): Handle the case where multiple writers compete for the same block.
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      checkTempBlockOwnedBySession(sessionId, blockId);
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      return new StoreBlockWriter(tempBlockMeta);
    }
  }

  @Override
  public BlockReader getBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    LOG.debug("getBlockReader: sessionId={}, blockId={}, lockId={}", sessionId, blockId, lockId);
    mLockManager.validateLock(sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);
      return new StoreBlockReader(sessionId, blockMeta);
    }
  }

  @Override
  public TempBlockMeta createBlock(long sessionId, long blockId, AllocateOptions options)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    LOG.debug("createBlock: sessionId={}, blockId={}, options={}", sessionId, blockId, options);
    TempBlockMeta tempBlockMeta = createBlockMetaInternal(sessionId, blockId, true, options);
    if (tempBlockMeta != null) {
      createBlockFile(tempBlockMeta.getPath());
      return tempBlockMeta;
    }
    // TODO(bin): We are probably seeing a rare transient failure, maybe define and throw some
    // other types of exception to indicate this case.
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_ALLOCATION,
        options.getSize(), options.getLocation(), blockId);
  }

  // TODO(bin): Make this method to return a snapshot.
  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    LOG.debug("getVolatileBlockMeta: blockId={}", blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId);
    }
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    LOG.debug("getBlockMeta: sessionId={}, blockId={}, lockId={}", sessionId, blockId, lockId);
    mLockManager.validateLock(sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId);
    }
  }

  @Override
  public TempBlockMeta getTempBlockMeta(long sessionId, long blockId) {
    LOG.debug("getTempBlockMeta: sessionId={}, blockId={}", sessionId, blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getTempBlockMetaOrNull(blockId);
    }
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    LOG.debug("commitBlock: sessionId={}, blockId={}, pinOnCreate={}",
        sessionId, blockId, pinOnCreate);
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      BlockStoreLocation loc = commitBlockInternal(sessionId, blockId, pinOnCreate);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onCommitBlock(sessionId, blockId, loc);
        }
      }
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  @Override
  public long commitBlockLocked(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    LOG.debug("commitBlock: sessionId={}, blockId={}, pinOnCreate={}",
        sessionId, blockId, pinOnCreate);
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);
    try {
      BlockStoreLocation loc = commitBlockInternal(sessionId, blockId, pinOnCreate);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onCommitBlock(sessionId, blockId, loc);
        }
      }
    } catch (Exception e) {
      // Unlock if exception is thrown.
      mLockManager.unlockBlock(lockId);
      throw e;
    }
    return lockId;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    LOG.debug("abortBlock: sessionId={}, blockId={}", sessionId, blockId);
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
    LOG.debug("requestSpace: sessionId={}, blockId={}, additionalBytes={}", sessionId, blockId,
        additionalBytes);

    // NOTE: a temp block is only visible to its own writer, unnecessary to acquire
    // block lock here since no sharing
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);

      StorageDirView allocationDir = allocateSpace(sessionId,
          AllocateOptions.forRequestSpace(additionalBytes, tempBlockMeta.getBlockLocation()));
      if (allocationDir == null) {
        throw new WorkerOutOfSpaceException(String.format(
            "Can't reserve more space for block: %d under session: %d.", blockId, sessionId));
      }

      if (!allocationDir.toBlockStoreLocation().equals(tempBlockMeta.getBlockLocation())) {
        // If reached here, allocateSpace() failed to enforce 'forceLocation' flag.
        throw new IllegalStateException(
            String.format("Allocation error: location enforcement failed for location: %s",
                allocationDir.toBlockStoreLocation()));
      }

      // Increase the size of this temp block
      try {
        mMetaManager.resizeTempBlockMeta(
            tempBlockMeta, tempBlockMeta.getBlockSize() + additionalBytes);
      } catch (InvalidWorkerStateException e) {
        throw Throwables.propagate(e); // we shall never reach here
      }
    }
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    moveBlock(sessionId, blockId, BlockStoreLocation.anyTier(), moveOptions);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, BlockStoreLocation oldLocation,
      AllocateOptions moveOptions)
          throws BlockDoesNotExistException, BlockAlreadyExistsException,
          InvalidWorkerStateException, WorkerOutOfSpaceException, IOException {
    LOG.debug("moveBlock: sessionId={}, blockId={}, oldLocation={}, options={}", sessionId,
        blockId, oldLocation, moveOptions);
    MoveBlockResult result = moveBlockInternal(sessionId, blockId, oldLocation, moveOptions);
    if (result.getSuccess()) {
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onMoveBlockByClient(sessionId, blockId, result.getSrcLocation(),
              result.getDstLocation());
        }
      }
      return;
    }
    // TODO(bin): We are probably seeing a rare transient failure, maybe define and throw some
    // other types of exception to indicate this case.
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE,
        moveOptions.getLocation(), blockId);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    removeBlock(sessionId, blockId, BlockStoreLocation.anyTier());
  }

  @Override
  public void removeBlock(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    LOG.debug("removeBlock: sessionId={}, blockId={}, location={}", sessionId, blockId, location);
    long lockId = mLockManager.lockBlock(sessionId, blockId, BlockLockType.WRITE);

    BlockMeta blockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      if (mMetaManager.hasTempBlockMeta(blockId)) {
        throw new InvalidWorkerStateException(ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK, blockId);
      }

      blockMeta = mMetaManager.getBlockMeta(blockId);

      if (!blockMeta.getBlockLocation().belongsTo(location)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            location);
      }
    } catch (Exception e) {
      mLockManager.unlockBlock(lockId);
      throw e;
    }

    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      removeBlockInternal(blockMeta);
    } finally {
      mLockManager.unlockBlock(lockId);
    }

    synchronized (mBlockStoreEventListeners) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        listener.onRemoveBlockByClient(sessionId, blockId);
        listener.onRemoveBlock(sessionId, blockId, blockMeta.getBlockLocation());
      }
    }
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    LOG.debug("accessBlock: sessionId={}, blockId={}", sessionId, blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId);

      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onAccessBlock(sessionId, blockId);
          listener.onAccessBlock(sessionId, blockId, blockMeta.getBlockLocation());
        }
      }
    }
  }

  /**
   * Free space is the entry for immediate block deletion in order to open up space for
   * new or ongoing blocks.
   *
   * - New blocks creations will not try to free space until all tiers are out of space.
   * - Ongoing blocks could end up freeing space oftenly, when the file's origin location is
   * low on space.
   *
   * This method is synchronized in order to prevent race in its only client, allocations.
   * If not synchronized, new allocations could steal space reserved by ongoing ones.
   * Removing synchronized requires implementing retries to this call along with an optimal
   * locking strategy for fairness.
   *
   * TODO(ggezer): Remove synchronized.
   * TODO(ggezer): Make it a private API.
   */
  @Override
  public synchronized void freeSpace(long sessionId, long minContiguousBytes,
      long minAvailableBytes, BlockStoreLocation location)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    LOG.debug("freeSpace: sessionId={}, minContiguousBytes={}, minAvailableBytes={}, location={}",
        sessionId, minAvailableBytes, minAvailableBytes, location);
    freeSpaceInternal(sessionId, minContiguousBytes, minAvailableBytes, location);
  }

  @Override
  public void cleanupSession(long sessionId) {
    LOG.debug("cleanupSession: sessionId={}", sessionId);
    // Release all locks the session is holding.
    mLockManager.cleanupSession(sessionId);

    // Collect a list of temp blocks the given session owns and abort all of them with best effort
    List<TempBlockMeta> tempBlocksToRemove;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      tempBlocksToRemove = mMetaManager.getSessionTempBlocks(sessionId);
    }
    for (TempBlockMeta tempBlockMeta : tempBlocksToRemove) {
      try {
        LOG.warn("Clean up expired temporary block {} from session {}.", tempBlockMeta.getBlockId(),
            sessionId);
        abortBlockInternal(sessionId, tempBlockMeta.getBlockId());
      } catch (Exception e) {
        LOG.error("Failed to cleanup tempBlock {} due to {}", tempBlockMeta.getBlockId(),
            e.getMessage());
      }
    }
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    LOG.debug("hasBlockMeta: blockId={}", blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.hasBlockMeta(blockId);
    }
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    // Removed DEBUG logging because this is very noisy
    // LOG.debug("getBlockStoreMeta:");
    BlockStoreMeta storeMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      storeMeta = mMetaManager.getBlockStoreMeta();
    }
    return storeMeta;
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    // Removed DEBUG logging because this is very noisy
    // LOG.debug("getBlockStoreMetaFull:");
    BlockStoreMeta storeMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      storeMeta = mMetaManager.getBlockStoreMetaFull();
    }
    return storeMeta;
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    LOG.debug("registerBlockStoreEventListener: listener={}", listener);
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
   */
  private void abortBlockInternal(long sessionId, long blockId) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException, IOException {

    String path;
    TempBlockMeta tempBlockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      checkTempBlockOwnedBySession(sessionId, blockId);
      tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      path = tempBlockMeta.getPath();
    }

    // The metadata lock is released during heavy IO. The temp block is private to one session, so
    // we do not lock it.
    Files.delete(Paths.get(path));

    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      mMetaManager.abortTempBlockMeta(tempBlockMeta);
    } catch (BlockDoesNotExistException e) {
      throw Throwables.propagate(e); // We shall never reach here
    }
  }

  /**
   * Commits a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @param pinOnCreate is block pinned on create
   * @return destination location to move the block
   * @throws BlockDoesNotExistException if block id can not be found in temporary blocks
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws InvalidWorkerStateException if block id is not owned by session id
   */
  private BlockStoreLocation commitBlockInternal(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, InvalidWorkerStateException, BlockDoesNotExistException,
      IOException {
    // When committing TempBlockMeta, the final BlockMeta calculates the block size according to
    // the actual file size of this TempBlockMeta. Therefore, commitTempBlockMeta must happen
    // after moving actual block file to its committed path.
    BlockStoreLocation loc;
    String srcPath;
    String dstPath;
    TempBlockMeta tempBlockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      checkTempBlockOwnedBySession(sessionId, blockId);
      tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
      srcPath = tempBlockMeta.getPath();
      dstPath = tempBlockMeta.getCommitPath();
      loc = tempBlockMeta.getBlockLocation();
    }

    // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
    FileUtils.move(srcPath, dstPath);

    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      mMetaManager.commitTempBlockMeta(tempBlockMeta);
    } catch (BlockAlreadyExistsException | BlockDoesNotExistException
        | WorkerOutOfSpaceException e) {
      throw Throwables.propagate(e); // we shall never reach here
    }

    // Check if block is pinned on commit
    if (pinOnCreate) {
      addToPinnedInodes(BlockId.getFileId(blockId));
    }

    return loc;
  }

  private StorageDirView allocateSpace(long sessionId, AllocateOptions options) {
    StorageDirView dirView = null;
    BlockMetadataView allocatorView =
        new BlockMetadataAllocatorView(mMetaManager, options.canUseReservedSpace());
    try {
      // Allocate from given location.
      dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
          options.getLocation(), allocatorView, false);
      if (dirView != null) {
        return dirView;
      }

      if (options.isForceLocation()) {
        if (options.isEvictionAllowed()) {
          LOG.debug("Free space for block expansion: freeing {} bytes on {}. ",
                  options.getSize(), options.getLocation());
          freeSpace(sessionId, options.getSize(), options.getSize(), options.getLocation());
          // Block expansion are forcing the location. We do not want the review's opinion.
          dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
              options.getLocation(), allocatorView.refreshView(), true);
          LOG.debug("Allocation after freeing space for block expansion: {}", dirView);
          if (dirView == null) {
            LOG.error("Target tier: {} has no evictable space to store {} bytes for session: {}",
                options.getLocation(), options.getSize(), sessionId);
            return null;
          }
        } else {
          LOG.error("Target tier: {} has no available space to store {} bytes for session: {}",
              options.getLocation(), options.getSize(), sessionId);
          return null;
        }
      } else {
        LOG.debug("Allocate to anyTier for {} bytes on {}", options.getSize(),
                options.getLocation());
        dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
            BlockStoreLocation.anyTier(), allocatorView, false);

        if (dirView != null) {
          return dirView;
        }

        if (options.isEvictionAllowed()) {
          // There is no space left on worker.
          // Free more than requested by configured free-ahead size.
          long freeAheadBytes =
              ServerConfiguration.getBytes(PropertyKey.WORKER_TIERED_STORE_FREE_AHEAD_BYTES);
          long toFreeBytes = options.getSize() + freeAheadBytes;
          LOG.debug("Allocation on anyTier failed. Free space for {} bytes on anyTier",
                  toFreeBytes);
          freeSpace(sessionId, options.getSize(), toFreeBytes,
              BlockStoreLocation.anyTier());
          // Skip the review as we want the allocation to be in the place we just freed
          dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
              BlockStoreLocation.anyTier(), allocatorView.refreshView(), true);
          LOG.debug("Allocation after freeing space for block creation: {}", dirView);
        }
      }
    } catch (Exception e) {
      LOG.error("Allocation failure. Options: {}. Error: {}", options, e);
      return null;
    }

    return dirView;
  }

  /**
   * Creates a temp block meta only if allocator finds available space. This method will not trigger
   * any eviction.
   *
   * @param sessionId session id
   * @param blockId block id
   * @param newBlock true if this temp block is created for a new block
   * @param options block allocation options
   * @return a temp block created if successful, or null if allocation failed (instead of throwing
   *         {@link WorkerOutOfSpaceException} because allocation failure could be an expected case)
   * @throws BlockAlreadyExistsException if there is already a block with the same block id
   */
  private TempBlockMeta createBlockMetaInternal(long sessionId, long blockId, boolean newBlock,
      AllocateOptions options) throws BlockAlreadyExistsException {
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      // NOTE: a temp block is supposed to be visible for its own writer,
      // unnecessary to acquire block lock here since no sharing.
      if (newBlock) {
        checkTempBlockIdAvailable(blockId);
      }

      // Allocate space.
      StorageDirView dirView = allocateSpace(sessionId, options);

      if (dirView == null) {
        return null;
      }

      // TODO(carson): Add tempBlock to corresponding storageDir and remove the use of
      // StorageDirView.createTempBlockMeta.
      TempBlockMeta tempBlock = dirView.createTempBlockMeta(sessionId, blockId, options.getSize());
      try {
        // Add allocated temp block to metadata manager. This should never fail if allocator
        // correctly assigns a StorageDir.
        mMetaManager.addTempBlockMeta(tempBlock);
      } catch (WorkerOutOfSpaceException | BlockAlreadyExistsException e) {
        // If we reach here, allocator is not working properly
        LOG.error("Unexpected failure: {} bytes allocated at {} by allocator, "
            + "but addTempBlockMeta failed", options.getSize(), options.getLocation());
        throw Throwables.propagate(e);
      }
      return tempBlock;
    }
  }

  /**
   * Tries to free a certain amount of space in the given location.
   *
   * @param sessionId the session id
   * @param minContiguousBytes the minimum amount of contiguous space in bytes to set available
   * @param minAvailableBytes the minimum amount of space in bytes to set available
   * @param location location of space
   * @throws WorkerOutOfSpaceException if it is impossible to achieve minimum space requirement
   */
  private void freeSpaceInternal(long sessionId, long minContiguousBytes, long minAvailableBytes,
      BlockStoreLocation location) throws WorkerOutOfSpaceException, IOException {
    // TODO(ggezer): Too much memory pressure when pinned-inodes list is large.
    BlockMetadataEvictorView evictorView = getUpdatedView();
    LOG.debug(
        "freeSpaceInternal - locAvailableBytes: {}, minContiguousBytes: {}, minAvailableBytes: {}",
        evictorView.getAvailableBytes(location), minContiguousBytes, minAvailableBytes);
    boolean contiguousSpaceFound = false;
    boolean availableBytesFound = false;

    int blocksIterated = 0;
    int blocksRemoved = 0;
    int spaceFreed = 0;

    // List of all dirs that belong to the given location.
    List<StorageDirView> dirViews = evictorView.getDirs(location);

    Iterator<Long> evictionCandidates = mBlockIterator.getIterator(location, BlockOrder.Natural);
    while (true) {
      // Check if minContiguousBytes is satisfied.
      if (!contiguousSpaceFound) {
        for (StorageDirView dirView : dirViews) {
          if (dirView.getAvailableBytes() >= minContiguousBytes) {
            contiguousSpaceFound = true;
            break;
          }
        }
      }

      // Check minAvailableBytes is satisfied.
      if (!availableBytesFound) {
        if (evictorView.getAvailableBytes(location) >= minAvailableBytes) {
          availableBytesFound = true;
        }
      }

      if (contiguousSpaceFound && availableBytesFound) {
        break;
      }

      if (!evictionCandidates.hasNext()) {
        break;
      }

      long blockToDelete = evictionCandidates.next();
      blocksIterated++;
      if (evictorView.isBlockEvictable(blockToDelete)) {
        try {
          BlockMeta blockMeta = mMetaManager.getBlockMeta(blockToDelete);
          removeBlockInternal(blockMeta);
          blocksRemoved++;
          synchronized (mBlockStoreEventListeners) {
            for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
              listener.onRemoveBlockByClient(sessionId, blockMeta.getBlockId());
              listener.onRemoveBlock(sessionId, blockMeta.getBlockId(),
                  blockMeta.getBlockLocation());
            }
          }
          spaceFreed += blockMeta.getBlockSize();
        } catch (BlockDoesNotExistException e) {
          LOG.warn("Failed to evict blockId {}, it could be already deleted", blockToDelete);
          continue;
        }
      }
    }

    if (!contiguousSpaceFound || !availableBytesFound) {
      LOG.error(
          "Failed to free space. Min contiguous requested: {}, Min available requested: {}, "
              + "Blocks iterated: {}, Blocks removed: {}, " + "Space freed: {}",
          minContiguousBytes, minAvailableBytes, blocksIterated, blocksRemoved, spaceFreed);

      throw new WorkerOutOfSpaceException(ExceptionMessage.NO_EVICTION_PLAN_TO_FREE_SPACE
          .getMessage(minAvailableBytes, location.tierAlias()));
    }
  }

  /**
   * Gets the most updated view with most recent information on pinned inodes, and currently locked
   * blocks.
   *
   * @return {@link BlockMetadataEvictorView}, an updated view with most recent information
   */
  private BlockMetadataEvictorView getUpdatedView() {
    // TODO(calvin): Update the view object instead of creating new one every time.
    synchronized (mPinnedInodes) {
      return new BlockMetadataEvictorView(mMetaManager, mPinnedInodes,
          mLockManager.getLockedBlocks());
    }
  }

  /**
   * Moves a block to new location only if allocator finds available space in newLocation. This
   * method will not trigger any eviction. Returns {@link MoveBlockResult}.
   *
   * @param sessionId session id
   * @param blockId block id
   * @param oldLocation the source location of the block
   * @param moveOptions the allocate options for the move
   * @return the resulting information about the move operation
   * @throws BlockDoesNotExistException if block is not found
   * @throws BlockAlreadyExistsException if a block with same id already exists in new location
   * @throws InvalidWorkerStateException if the block to move is a temp block
   */
  private MoveBlockResult moveBlockInternal(long sessionId, long blockId,
      BlockStoreLocation oldLocation, AllocateOptions moveOptions)
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

      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
        // Update moveOptions with the block size.
        moveOptions.setSize(blockSize);
      }

      if (!srcLocation.belongsTo(oldLocation)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            oldLocation);
      }
      if (srcLocation.belongsTo(moveOptions.getLocation())) {
        return new MoveBlockResult(true, blockSize, srcLocation, srcLocation);
      }

      TempBlockMeta dstTempBlock = createBlockMetaInternal(sessionId, blockId, false, moveOptions);
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

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        // If this metadata update fails, we panic for now.
        // TODO(bin): Implement rollback scheme to recover from IO failures.
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
      } catch (BlockAlreadyExistsException | BlockDoesNotExistException
          | WorkerOutOfSpaceException e) {
        // WorkerOutOfSpaceException is only possible if session id gets cleaned between
        // createBlockMetaInternal and moveBlockMeta.
        throw Throwables.propagate(e); // we shall never reach here
      }

      return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
    } finally {
      mLockManager.unlockBlock(lockId);
    }
  }

  /**
   * Removes a block physically and from metadata.
   *
   * @param blockMeta block metadata
   *
   * @throws InvalidWorkerStateException if the block to remove is a temp block
   * @throws BlockDoesNotExistException if this block can not be found
   */
  private void removeBlockInternal(BlockMeta blockMeta)
      throws BlockDoesNotExistException, IOException {
    String filePath = blockMeta.getPath();
    Files.delete(Paths.get(filePath));
    mMetaManager.removeBlockMeta(blockMeta);
  }

  /**
   * Creates a file to represent a block denoted by the given block path. This file will be owned
   * by the Alluxio worker but have 777 permissions so processes under users different from the
   * user that launched the Alluxio worker can read and write to the file. The tiered storage
   * directory has the sticky bit so only the worker user can delete or rename files it creates.
   *
   * @param blockPath the block path to create
   */
  // TODO(peis): Consider using domain socket to avoid setting the permission to 777.
  private static void createBlockFile(String blockPath) throws IOException {
    FileUtils.createBlockPath(blockPath,
        ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS));
    FileUtils.createFile(blockPath);
    FileUtils.changeLocalFileToFullPermission(blockPath);
    LOG.debug("Created new file block, block path: {}", blockPath);
  }

  /**
   * Updates the pinned blocks.
   *
   * @param inodes a set of ids inodes that are pinned
   */
  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    LOG.debug("updatePinnedInodes: inodes={}", inodes);
    synchronized (mPinnedInodes) {
      mPinnedInodes.clear();
      mPinnedInodes.addAll(Preconditions.checkNotNull(inodes));
    }
  }

  /**
   * Add a single inode to set of pinned ids.
   *
   * @param inode an inode that is pinned
   */
  private void addToPinnedInodes(Long inode) {
    LOG.debug("addToPinnedInodes: inode={}", inode);
    synchronized (mPinnedInodes) {
      mPinnedInodes.add(Preconditions.checkNotNull(inode));
    }
  }

  @Override
  public boolean checkStorage() {
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      List<StorageDir> dirsToRemove = new ArrayList<>();
      for (StorageTier tier : mMetaManager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          String path = dir.getDirPath();
          if (!FileUtils.isStorageDirAccessible(path)) {
            LOG.error("Storage check failed for path {}. The directory will be excluded.", path);
            dirsToRemove.add(dir);
          }
        }
      }
      dirsToRemove.forEach(this::removeDir);
      return !dirsToRemove.isEmpty();
    }
  }

  /**
   * Removes a storage directory.
   *
   * @param dir storage directory to be removed
   */
  public void removeDir(StorageDir dir) {
    // TODO(feng): Add a command for manually removing directory
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      String tierAlias = dir.getParentTier().getTierAlias();
      dir.getParentTier().removeStorageDir(dir);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          dir.getBlockIds().forEach(listener::onBlockLost);
          listener.onStorageLost(tierAlias, dir.getDirPath());
          listener.onStorageLost(dir.toBlockStoreLocation());
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    mTaskCoordinator.close();
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
