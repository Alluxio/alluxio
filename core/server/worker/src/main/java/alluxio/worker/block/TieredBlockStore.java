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
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.io.StoreBlockReader;
import alluxio.worker.block.io.StoreBlockWriter;
import alluxio.worker.block.management.ManagementTaskCoordinator;
import alluxio.worker.block.management.DefaultStoreLoadTracker;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.order.BlockIterator;
import alluxio.worker.block.order.BlockOrder;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
  private BlockIterator mIterationProvider;

  /** Management task coordinator. */
  private ManagementTaskCoordinator mTaskCoordinator;

  /**
   * Creates a new instance of {@link TieredBlockStore}.
   */
  public TieredBlockStore() {
    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    mLockManager = new BlockLockManager();

    mIterationProvider = mMetaManager.getBlockIterator();
    registerBlockStoreEventListener(mIterationProvider);

    BlockMetadataEvictorView initManagerView = new BlockMetadataEvictorView(mMetaManager,
        Collections.<Long>emptySet(), Collections.<Long>emptySet());
    mAllocator = Allocator.Factory.create(initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    // TODO(ggezer): TV2 - Implement evictor emulation.
    // initManagerView = new BlockMetadataEvictorView(mMetaManager, Collections.<Long>emptySet(),
    //         Collections.<Long>emptySet());
    // mEvictor = Evictor.Factory.create(initManagerView, mAllocator);
    // if (mEvictor instanceof BlockStoreEventListener) {
    //   registerBlockStoreEventListener((BlockStoreEventListener) mEvictor);
    // }

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
  public TempBlockMeta createBlock(long sessionId, long blockId, BlockAllocationOptions options)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    LOG.debug("createBlock: sessionId={}, blockId={}, options={}", sessionId, blockId, options);
    // TODO(ggezer): TV2 - Re-purpose write-to-medium feature to set forcedLocation flag.
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
    LOG.debug("commitBlock: sessionId={}, blockId={}", sessionId, blockId);
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
    LOG.debug("commitBlock: sessionId={}, blockId={}", sessionId, blockId);
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

    // NOTE: a temp block is supposed to be visible for its own writer, unnecessary to acquire
    // block lock here since no sharing
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);

      StorageDirView allocationDir = allocateSpace(sessionId, BlockAllocationOptions
          .defaultsForRequest(additionalBytes, tempBlockMeta.getBlockLocation()));
      if (allocationDir == null) {
        throw new WorkerOutOfSpaceException(String.format(
            "Can't reserve more space for block: %d under session: %d.", blockId, sessionId));
      }

      if (!allocationDir.toBlockStoreLocation().equals(tempBlockMeta.getBlockLocation())) {
        // If reached here, allocateSpace() failed to enforce 'forceLocation' flag.
        throw new RuntimeException("Allocation error: location enforcement failed");
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
    LOG.debug("moveBlock: sessionId={}, blockId={}, oldLocation={}, newLocation={}", sessionId,
        blockId, oldLocation, newLocation);
    MoveBlockResult result = moveBlockInternal(sessionId, blockId, oldLocation, newLocation);
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
    throw new WorkerOutOfSpaceException(ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE, newLocation,
        blockId);
  }

  @Override
  public void swapBlocks(long sessionId, long blockId1, long blockId2)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    BlockMeta blockMeta1;
    BlockMeta blockMeta2;
    boolean doFsSwap = false;

    long lockId1 = mLockManager.lockBlock(sessionId, blockId1, BlockLockType.READ);
    long lockId2 = mLockManager.lockBlock(sessionId, blockId2, BlockLockType.READ);
    try {
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId1) || mMetaManager.hasTempBlockMeta(blockId2)) {
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId1);
        }
        blockMeta1 = mMetaManager.getBlockMeta(blockId1);
        blockMeta2 = mMetaManager.getBlockMeta(blockId2);
      }

      if (ServerConfiguration.getBoolean(PropertyKey.WORKER_MANAGEMENT_SWAP_USING_FS)) {
        Path dirPath1 = Paths.get(blockMeta1.getParentDir().getDirPath());
        Path dirPath2 = Paths.get(blockMeta2.getParentDir().getDirPath());
        if (Objects.equals(dirPath1.getRoot(), dirPath2.getRoot())) {
          doFsSwap = true;
        }
      }

      // Allocate swap space from directories for swapping blocks.
      if (!blockMeta1.getParentDir().allocateFromReserve(blockMeta2.getBlockSize())
          || !blockMeta2.getParentDir().allocateFromReserve(blockMeta1.getBlockSize())) {
        throw new WorkerOutOfSpaceException("No reserved space left for swapping.");
      }

      if (!doFsSwap) {
        copyBlockFiles(sessionId, blockMeta1, blockMeta2);
      }
    } finally {
      mLockManager.unlockBlock(lockId1);
      mLockManager.unlockBlock(lockId2);
    }

    long writeLock1 = mLockManager.lockBlock(sessionId, blockId1, BlockLockType.WRITE);
    long writeLock2 = mLockManager.lockBlock(sessionId, blockId2, BlockLockType.WRITE);
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      if (doFsSwap) {
        String swapPath = new TempBlockMeta(sessionId, blockId1, blockMeta1.getBlockSize(),
            blockMeta2.getParentDir()).getPath();
        // Ensure swap path.
        Path swapParentPath = Paths.get(swapPath).getParent();
        if (swapParentPath != null) {
          FileUtils.createDir(swapParentPath.toString());
        }
        // 3-way swap files.
        FileUtils.move(blockMeta1.getPath(), swapPath);
        FileUtils.move(blockMeta2.getPath(), blockMeta1.getPath());
        FileUtils.move(swapPath, blockMeta2.getPath());
      } else {
        String swapPath1 = new TempBlockMeta(sessionId, blockMeta1.getBlockId(),
            blockMeta1.getBlockSize(), blockMeta2.getParentDir()).getPath();
        String swapPath2 = new TempBlockMeta(sessionId, blockMeta2.getBlockId(),
            blockMeta2.getBlockSize(), blockMeta1.getParentDir()).getPath();
        FileUtils.delete(blockMeta1.getPath());
        FileUtils.delete(blockMeta2.getPath());
        FileUtils.move(swapPath2, blockMeta1.getPath());
        FileUtils.move(swapPath1, blockMeta2.getPath());
      }
      mMetaManager.swapBlocks(blockMeta1, blockMeta2);
      synchronized (mBlockStoreEventListeners) {
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          listener.onMoveBlockByWorker(sessionId, blockMeta1.getBlockId(),
              blockMeta1.getBlockLocation(), blockMeta2.getBlockLocation());
          listener.onMoveBlockByWorker(sessionId, blockMeta2.getBlockId(),
              blockMeta2.getBlockLocation(), blockMeta1.getBlockLocation());
        }
      }
    } finally {
      mLockManager.unlockBlock(writeLock1);
      mLockManager.unlockBlock(writeLock2);
      // Release back the allocation.
      blockMeta1.getParentDir().releaseToReserve(blockMeta1.getBlockSize());
      blockMeta2.getParentDir().releaseToReserve(blockMeta2.getBlockSize());
    }
  }

  private void copyBlockFiles(long sessionId, BlockMeta blockMeta1, BlockMeta blockMeta2)
      throws IOException {
    String swapPathStr1 = new TempBlockMeta(sessionId, blockMeta1.getBlockId(),
        blockMeta1.getBlockSize(), blockMeta2.getParentDir()).getPath();
    String swapPathStr2 = new TempBlockMeta(sessionId, blockMeta2.getBlockId(),
        blockMeta2.getBlockSize(), blockMeta1.getParentDir()).getPath();

    // Ensure swap paths.
    Path swapParentPath1 = Paths.get(swapPathStr1).getParent();
    Path swapParentPath2 = Paths.get(swapPathStr2).getParent();
    if (swapParentPath1 != null) {
      FileUtils.createDir(swapParentPath1.toString());
    }
    if (swapParentPath2 != null) {
      FileUtils.createDir(swapParentPath2.toString());
    }

    BlockReader reader1 = new StoreBlockReader(sessionId, blockMeta1);
    LocalFileBlockWriter writer1 = new LocalFileBlockWriter(swapPathStr1);

    BlockReader reader2 = new StoreBlockReader(sessionId, blockMeta2);
    LocalFileBlockWriter writer2 = new LocalFileBlockWriter(swapPathStr2);

    // Transfer first file.
    ByteBuf buffer1 = PooledByteBufAllocator.DEFAULT.buffer((int) blockMeta1.getBlockSize());
    reader1.transferTo(buffer1);
    writer1.append(buffer1);

    // Transfer second file.
    ByteBuf buffer2 = PooledByteBufAllocator.DEFAULT.buffer((int) blockMeta1.getBlockSize());
    reader2.transferTo(buffer2);
    writer2.append(buffer2);
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

  @Override
  public synchronized void freeSpace(long sessionId, long minAvailableBytes, long maxAvailableBytes,
      BlockStoreLocation location)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    LOG.debug("freeSpace: sessionId={}, minAvailableBytes={}, maxAvailableBytes={}, location={}",
        sessionId, minAvailableBytes, maxAvailableBytes, location);
    freeSpaceInternal(sessionId, minAvailableBytes, maxAvailableBytes, location);
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

  private StorageDirView allocateSpace(long sessionId, BlockAllocationOptions options) {
    StorageDirView dirView = null;
    BlockMetadataView allocatorView = new BlockMetadataAllocatorView(mMetaManager);
    try {
      // Allocate from given location.
      dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
          options.getLocation(), allocatorView);

      if (dirView != null) {
        return dirView;
      }

      if (options.isForceLocation()) {
        if (options.isEvictionAllowed()) {
          freeSpace(sessionId, options.getSize(), options.getSize(), options.getLocation());
          dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
              options.getLocation(), allocatorView.refreshView());

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
        dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
            BlockStoreLocation.anyTier(), allocatorView);

        if (dirView != null) {
          return dirView;
        }

        if (options.isEvictionAllowed()) {
          // There is no space left on worker.
          // Free more than requested by configured free-ahead size.
          long freeAheadBytes =
              ServerConfiguration.getBytes(PropertyKey.WORKER_TIERED_STORE_FREE_AHEAD_BYTES);
          freeSpace(sessionId, options.getSize(), options.getSize() + freeAheadBytes,
              BlockStoreLocation.anyTier());

          dirView = mAllocator.allocateBlockWithView(sessionId, options.getSize(),
              BlockStoreLocation.anyTier(), allocatorView.refreshView());
        }
      }
    } catch (Exception e) {
      LOG.error("Allocation failure. Options: {}. Error: {}", options, e);
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
      BlockAllocationOptions options) throws BlockAlreadyExistsException {
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
   * @param minAvailableBytes the minimum amount of space in bytes to set available
   * @param maxAvailableBytes the maximum amount of space in bytes to set available
   * @param location location of space
   * @throws WorkerOutOfSpaceException if it is impossible to achieve minimum space requirement
   */
  private void freeSpaceInternal(long sessionId, long minAvailableBytes, long maxAvailableBytes,
      BlockStoreLocation location) throws WorkerOutOfSpaceException, IOException {
    // TODO(ggezer): TV2 - Too much memory pressure when pinned-inodes list is large.
    BlockMetadataEvictorView evictorView = getUpdatedView();
    long minBytesToFree = minAvailableBytes - evictorView.getAvailableBytes(location);
    long maxBytesToFree = maxAvailableBytes - evictorView.getAvailableBytes(location);

    int blocksIterated = 0;
    int blocksRemoved = 0;
    int spaceFreed = 0;

    LOG.debug(
        "Failed to free space. Space requested: {}-{}, Blocks iterated: {}, Blocks removed: {},"
            + " Freed Space: {}, Remaining: {}",
        minAvailableBytes, maxAvailableBytes, blocksIterated, blocksRemoved, spaceFreed,
        maxBytesToFree);

    Iterator<Long> evictionCandidates =
        mIterationProvider.getIterator(location, BlockOrder.Natural);
    while (evictionCandidates.hasNext() && maxBytesToFree > 0) {
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
          minBytesToFree -= blockMeta.getBlockSize();
          maxBytesToFree -= blockMeta.getBlockSize();
          spaceFreed += blockMeta.getBlockSize();
        } catch (BlockDoesNotExistException e) {
          LOG.info("Failed to evict blockId {}, it could be already deleted", blockToDelete);
          continue;
        }
      }
    }

    if (minBytesToFree > 0) {
      LOG.debug("Failed to free minimum requested space. "
              + "Space requested: {}, Blocks iterated: {}, Blocks removed: {}, "
              + "Freed Space: {}, Remaining: {}",
          minAvailableBytes, blocksIterated, blocksRemoved, spaceFreed, minBytesToFree);

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
   * @param newLocation new location to move this block
   * @return the resulting information about the move operation
   * @throws BlockDoesNotExistException if block is not found
   * @throws BlockAlreadyExistsException if a block with same id already exists in new location
   * @throws InvalidWorkerStateException if the block to move is a temp block
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

      try (LockResource r = new LockResource(mMetadataReadLock)) {
        if (mMetaManager.hasTempBlockMeta(blockId)) {
          throw new InvalidWorkerStateException(ExceptionMessage.MOVE_UNCOMMITTED_BLOCK, blockId);
        }
        srcBlockMeta = mMetaManager.getBlockMeta(blockId);
        srcLocation = srcBlockMeta.getBlockLocation();
        srcFilePath = srcBlockMeta.getPath();
        blockSize = srcBlockMeta.getBlockSize();
      }

      if (!srcLocation.belongsTo(oldLocation)) {
        throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION, blockId,
            oldLocation);
      }
      if (srcLocation.belongsTo(newLocation)) {
        return new MoveBlockResult(true, blockSize, srcLocation, srcLocation);
      }

      TempBlockMeta dstTempBlock = createBlockMetaInternal(sessionId, blockId, false,
          BlockAllocationOptions.defaultsForMove(blockSize, newLocation));
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
