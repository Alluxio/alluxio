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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.master.block.BlockId;
import alluxio.resource.LockResource;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.annotator.BlockIterator;
import alluxio.worker.block.annotator.BlockOrder;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.io.StoreBlockReader;
import alluxio.worker.block.management.DefaultStoreLoadTracker;
import alluxio.worker.block.management.ManagementTaskCoordinator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;

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
 * <li>Eviction is done in {@link #freeSpace} and it is on the basis of best effort. For
 * operations that may trigger this eviction (e.g., move, create, requestSpace), retry is used</li>
 * </ul>
 */
@ThreadSafe
public class TieredBlockStore implements LocalBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(TieredBlockStore.class);
  private static final Long REMOVE_BLOCK_TIMEOUT_MS = 60_000L;
  private static final long FREE_AHEAD_BYTETS =
      Configuration.getBytes(PropertyKey.WORKER_TIERED_STORE_FREE_AHEAD_BYTES);
  private final BlockMetadataManager mMetaManager;
  private final BlockLockManager mLockManager;

  private final BlockReaderFactory mBlockReaderFactory;

  private final BlockWriterFactory mBlockWriterFactory;

  private final TempBlockMetaFactory mTempBlockMetaFactory;

  private Allocator mAllocator;

  private final List<BlockStoreEventListener> mBlockStoreEventListeners =
      new CopyOnWriteArrayList<>();

  /** A set of pinned inodes fetched from the master. */
  private final Set<Long> mPinnedInodes = new HashSet<>();

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  /** ReadLock provided by {@link #mMetadataLock} to guard metadata read operations. */
  private final Lock mMetadataReadLock = mMetadataLock.readLock();

  /** WriteLock provided by {@link #mMetadataLock} to guard metadata write operations. */
  private final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  /** Management task coordinator. */
  private ManagementTaskCoordinator mTaskCoordinator;

  /**
   * Creates a new instance of {@link TieredBlockStore}.
   *
   * @param metaManager the block metadata manager
   * @param lockManager the lock manager
   * @param blockReaderFactory the block reader factory
   * @param blockWriterFactory the block writer factory
   * @param tempBlockMetaFactory the temp block meta factory
   */
  @Inject
  public TieredBlockStore(BlockMetadataManager metaManager,
      BlockLockManager lockManager,
      BlockReaderFactory blockReaderFactory,
      BlockWriterFactory blockWriterFactory,
      TempBlockMetaFactory tempBlockMetaFactory) {
    mMetaManager = metaManager;
    mLockManager = lockManager;
    mBlockReaderFactory = blockReaderFactory;
    mBlockWriterFactory = blockWriterFactory;
    mTempBlockMetaFactory = tempBlockMetaFactory;
  }

  @Override
  public void initialize() {
    BlockIterator blockIterator = mMetaManager.getBlockIterator();
    // Register listeners required by the block iterator.
    for (BlockStoreEventListener listener : blockIterator.getListeners()) {
      registerBlockStoreEventListener(listener);
    }

    BlockMetadataEvictorView initManagerView = new BlockMetadataEvictorView(mMetaManager,
        Collections.emptySet(), Collections.emptySet());
    mAllocator = Allocator.Factory.create(initManagerView);
    if (mAllocator instanceof BlockStoreEventListener) {
      registerBlockStoreEventListener((BlockStoreEventListener) mAllocator);
    }

    // Initialize and start coordinator.
    mTaskCoordinator = new ManagementTaskCoordinator(this, mMetaManager,
        new DefaultStoreLoadTracker(), this::getUpdatedView);
    mTaskCoordinator.start();
  }

  @Override
  public Optional<BlockLock> pinBlock(long sessionId, long blockId) {
    LOG.debug("pinBlock: sessionId={}, blockId={}", sessionId, blockId);
    BlockLock lock = mLockManager.acquireBlockLock(sessionId, blockId, BlockLockType.READ);
    if (hasBlockMeta(blockId)) {
      return Optional.of(lock);
    }
    lock.close();
    return Optional.empty();
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId) throws IOException {
    LOG.debug("getBlockWriter: sessionId={}, blockId={}", sessionId, blockId);
    // NOTE: a temp block is supposed to only be visible by its own writer, unnecessary to acquire
    // block lock here since no sharing
    // TODO(bin): Handle the case where multiple writers compete for the same block.
    checkBlockDoesNotExist(blockId);
    return mBlockWriterFactory.createBlockWriter(checkAndGetTempBlockMeta(sessionId, blockId));
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset)
      throws IOException {
    LOG.debug("createBlockReader: sessionId={}, blockId={}, offset={}",
        sessionId, blockId, offset);
    Closeable blockLock = mLockManager.acquireBlockLock(sessionId, blockId, BlockLockType.READ);
    Optional<BlockMeta> blockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      blockMeta = mMetaManager.getBlockMeta(blockId);
    }
    if (!blockMeta.isPresent()) {
      blockLock.close();
      throw new BlockDoesNotExistRuntimeException(blockId);
    }
    try {
      BlockReader reader = new StoreBlockReader(sessionId, blockMeta.get());
      ((FileChannel) reader.getChannel()).position(offset);
      accessBlock(sessionId, blockId);
      return new DelegatingBlockReader(reader, blockLock);
    } catch (Exception e) {
      blockLock.close();
      throw new IOException(format("Failed to get local block reader, sessionId=%d, "
          + "blockId=%d, offset=%d", sessionId, blockId, offset), e);
    }
  }

  @Override
  public TempBlockMeta createBlock(long sessionId, long blockId, AllocateOptions options) {
    LOG.debug("createBlock: sessionId={}, blockId={}, options={}", sessionId, blockId, options);
    TempBlockMeta tempBlockMeta = createBlockMetaInternal(sessionId, blockId, true, options);
    createBlockFile(tempBlockMeta.getPath());
    return tempBlockMeta;
  }

  // TODO(bin): Make this method to return a snapshot.
  @Override
  public Optional<BlockMeta> getVolatileBlockMeta(long blockId) {
    LOG.debug("getVolatileBlockMeta: blockId={}", blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId);
    }
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    LOG.debug("getTempBlockMeta: blockId={}", blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getTempBlockMeta(blockId);
    }
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
    LOG.debug("commitBlock: sessionId={}, blockId={}, pinOnCreate={}",
        sessionId, blockId, pinOnCreate);
    try (BlockLock lock = mLockManager.acquireBlockLock(sessionId, blockId, BlockLockType.WRITE)) {
      BlockStoreLocation loc = commitBlockInternal(sessionId, blockId, pinOnCreate);
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onCommitBlock(blockId, loc);
        }
      }
    }
  }

  @Override
  public BlockLock commitBlockLocked(long sessionId, long blockId, boolean pinOnCreate) {
    LOG.debug("commitBlock: sessionId={}, blockId={}, pinOnCreate={}",
        sessionId, blockId, pinOnCreate);
    BlockLock lock = mLockManager.acquireBlockLock(sessionId, blockId, BlockLockType.WRITE);
    BlockStoreLocation loc;
    try {
      loc = commitBlockInternal(sessionId, blockId, pinOnCreate);
    } catch (RuntimeException e) {
      lock.close();
      throw e;
    }
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onCommitBlock(blockId, loc);
      }
    }
    return lock;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) {
    LOG.debug("abortBlock: sessionId={}, blockId={}", sessionId, blockId);
    abortBlockInternal(sessionId, blockId);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onAbortBlock(blockId);
      }
    }
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes) {
    LOG.debug("requestSpace: sessionId={}, blockId={}, additionalBytes={}", sessionId, blockId,
        additionalBytes);
    if (additionalBytes <= 0) {
      return;
    }
    // NOTE: a temp block is only visible to its own writer, unnecessary to acquire
    // block lock here since no sharing
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      TempBlockMeta tempBlockMeta = checkAndGetTempBlockMeta(sessionId, blockId);
      BlockStoreLocation location = tempBlockMeta.getBlockLocation();
      StorageDirView allocationDir = allocateSpace(sessionId,
          AllocateOptions.forRequestSpace(additionalBytes, location));
      checkState(allocationDir.toBlockStoreLocation().equals(location),
          format("Allocation error: location enforcement failed for location: %s",
              allocationDir.toBlockStoreLocation()));
      // Increase the size of this temp block
      mMetaManager.resizeTempBlockMeta(tempBlockMeta,
          tempBlockMeta.getBlockSize() + additionalBytes);
    }
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws IOException {
    LOG.debug("moveBlock: sessionId={}, blockId={}, options={}", sessionId,
        blockId, moveOptions);
    BlockMeta meta = getVolatileBlockMeta(blockId).orElseThrow(
        () -> new IllegalStateException(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId)));
    if (meta.getBlockLocation().belongsTo(moveOptions.getLocation())) {
      return;
    }
    // Execute the block move if necessary
    MoveBlockResult result = moveBlockInternal(sessionId, blockId, moveOptions);
    if (result.getSuccess()) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onMoveBlockByClient(blockId, result.getSrcLocation(),
              result.getDstLocation());
        }
      }
      return;
    }
    throw new ResourceExhaustedRuntimeException(
        ExceptionMessage.NO_SPACE_FOR_BLOCK_MOVE.getMessage(moveOptions.getLocation(), blockId),
        false);
  }

  @Override
  public void removeBlock(long sessionId, long blockId) throws IOException {
    LOG.debug("removeBlock: sessionId={}, blockId={}", sessionId, blockId);
    Optional<BlockMeta> blockMeta = removeBlockInternal(
        sessionId, blockId, REMOVE_BLOCK_TIMEOUT_MS);
    for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
      synchronized (listener) {
        listener.onRemoveBlockByClient(blockId);
        blockMeta.ifPresent(meta -> listener.onRemoveBlock(
            blockId, meta.getBlockLocation()));
      }
    }
  }

  @VisibleForTesting
  Optional<BlockMeta> removeBlockInternal(long sessionId, long blockId, long timeoutMs)
      throws IOException {
    Optional<BlockLock> optionalLock =
        mLockManager.tryAcquireBlockLock(sessionId, blockId, BlockLockType.WRITE,
            timeoutMs, TimeUnit.MILLISECONDS);
    if (!optionalLock.isPresent()) {
      throw new DeadlineExceededException(
          format("Can not acquire lock to remove block %d for session %d after %d ms",
              blockId, sessionId, REMOVE_BLOCK_TIMEOUT_MS));
    }

    try (BlockLock lock = optionalLock.get();
        LockResource r = new LockResource(mMetadataWriteLock)) {
      if (mMetaManager.hasTempBlockMeta(blockId)) {
        throw new IllegalStateException(
            ExceptionMessage.REMOVE_UNCOMMITTED_BLOCK.getMessage(blockId));
      }
      Optional<BlockMeta> blockMeta = mMetaManager.getBlockMeta(blockId);
      if (blockMeta.isPresent()) {
        removeBlockFileAndMeta(blockMeta.get());
      }
      return blockMeta;
    }
  }

  @Override
  public void accessBlock(long sessionId, long blockId) {
    LOG.debug("accessBlock: sessionId={}, blockId={}", sessionId, blockId);
    Optional<BlockMeta> blockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      blockMeta = mMetaManager.getBlockMeta(blockId);
    }
    if (blockMeta.isPresent()) {
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
          listener.onAccessBlock(blockId);
          listener.onAccessBlock(blockId, blockMeta.get().getBlockLocation());
        }
      }
    }
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
        LOG.error("Failed to cleanup tempBlock {}", tempBlockMeta.getBlockId(), e);
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
  public boolean hasTempBlockMeta(long blockId) {
    LOG.debug("hasBlockMeta: blockId={}", blockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.hasTempBlockMeta(blockId);
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
    mBlockStoreEventListeners.add(listener);
  }

  /**
   * Checks if block id is a temporary block and owned by session id. This method must be enclosed
   * by {@link #mMetadataLock}.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   */
  private TempBlockMeta checkAndGetTempBlockMeta(long sessionId, long blockId) {
    Optional<TempBlockMeta> tempBlockMeta;
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      tempBlockMeta = mMetaManager.getTempBlockMeta(blockId);
    }
    checkState(tempBlockMeta.isPresent(),
        ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(blockId));
    checkState(tempBlockMeta.get().getSessionId() == sessionId,
        ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION.getMessage(blockId,
            tempBlockMeta.get().getSessionId(), sessionId));
    return tempBlockMeta.get();
  }

  private void checkBlockDoesNotExist(long blockId) {
    checkState(!hasBlockMeta(blockId),
        ExceptionMessage.TEMP_BLOCK_ID_COMMITTED.getMessage(blockId));
  }

  private void checkTempBlockDoesNotExist(long blockId) {
    checkState(!hasTempBlockMeta(blockId), MessageFormat
        .format("Temp blockId {0,number,#} is not available, because it already exists", blockId));
  }

  /**
   * Aborts a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   */
  private void abortBlockInternal(long sessionId, long blockId) {
    checkBlockDoesNotExist(blockId);
    TempBlockMeta tempBlockMeta = checkAndGetTempBlockMeta(sessionId, blockId);

    // The metadata lock is released during heavy IO. The temp block is private to one session, so
    // we do not lock it.
    FileUtils.delete(tempBlockMeta.getPath());
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      mMetaManager.abortTempBlockMeta(tempBlockMeta);
    }
  }

  /**
   * Commits a temp block.
   *
   * @param sessionId the id of session
   * @param blockId the id of block
   * @param pinOnCreate is block pinned on create
   * @return destination location to move the block
   */
  private BlockStoreLocation commitBlockInternal(long sessionId, long blockId,
      boolean pinOnCreate) {
    if (mMetaManager.hasBlockMeta(blockId)) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure", blockId);
      return mMetaManager.getBlockMeta(blockId).get().getBlockLocation();
    }
    // When committing TempBlockMeta, the final BlockMeta calculates the block size according to
    // the actual file size of this TempBlockMeta. Therefore, commitTempBlockMeta must happen
    // after moving actual block file to its committed path.
    TempBlockMeta tempBlockMeta = checkAndGetTempBlockMeta(sessionId, blockId);
    String srcPath = tempBlockMeta.getPath();
    String dstPath = tempBlockMeta.getCommitPath();
    BlockStoreLocation loc = tempBlockMeta.getBlockLocation();

    // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
    try {
      FileUtils.move(srcPath, dstPath);
    } catch (IOException e) {
      // TODO(jianjian) move IOException handling once we figure out how MoveResult works
      throw AlluxioRuntimeException.from(e);
    }

    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      mMetaManager.commitTempBlockMeta(tempBlockMeta);
    }

    // Check if block is pinned on commit
    if (pinOnCreate) {
      addToPinnedInodes(BlockId.getFileId(blockId));
    }

    return loc;
  }

  private StorageDirView allocateSpace(long sessionId, AllocateOptions options) {
    StorageDirView dirView;
    BlockMetadataView allocatorView =
        new BlockMetadataAllocatorView(mMetaManager, options.canUseReservedSpace());
    // Convenient way to break on failure cases, no intention to loop
    while (true) {
      if (options.isForceLocation()) {
        // Try allocating from given location. Skip the review because the location is forced.
        dirView = mAllocator.allocateBlockWithView(options.getSize(),
            options.getLocation(), allocatorView, true);
        if (dirView != null) {
          return dirView;
        }
        if (options.isEvictionAllowed()) {
          LOG.debug("Free space for block expansion: freeing {} bytes on {}. ",
                  options.getSize(), options.getLocation());
          freeSpace(sessionId, options.getSize(), options.getSize(), options.getLocation());
          // Block expansion are forcing the location. We do not want the review's opinion.
          dirView = mAllocator.allocateBlockWithView(options.getSize(),
              options.getLocation(), allocatorView.refreshView(), true);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Allocation after freeing space for block expansion, {}", dirView == null
                ? "no available dir." : "available bytes in dir: " + dirView.getAvailableBytes());
          }
          if (dirView == null) {
            LOG.error("Target tier: {} has no evictable space to store {} bytes for session: {}",
                options.getLocation(), options.getSize(), sessionId);
            break;
          }
        } else {
          // We are not evicting in the target tier so having no available space just
          // means the tier is currently full.
          LOG.warn("Target tier: {} has no available space to store {} bytes for session: {}",
              options.getLocation(), options.getSize(), sessionId);
          break;
        }
      } else {
        // Try allocating from given location. This may be rejected by the review logic.
        dirView = mAllocator.allocateBlockWithView(options.getSize(),
            options.getLocation(), allocatorView, false);
        if (dirView != null) {
          return dirView;
        }
        LOG.debug("Allocate to anyTier for {} bytes on {}", options.getSize(),
                options.getLocation());
        dirView = mAllocator.allocateBlockWithView(options.getSize(),
            BlockStoreLocation.anyTier(), allocatorView, false);
        if (dirView != null) {
          return dirView;
        }

        if (options.isEvictionAllowed()) {
          // There is no space left on worker.
          // Free more than requested by configured free-ahead size.
          long toFreeBytes = options.getSize() + FREE_AHEAD_BYTETS;
          LOG.debug("Allocation on anyTier failed. Free space for {} bytes on anyTier",
                  toFreeBytes);
          freeSpace(sessionId, options.getSize(), toFreeBytes,
              BlockStoreLocation.anyTier());
          // Skip the review as we want the allocation to be in the place we just freed
          dirView = mAllocator.allocateBlockWithView(options.getSize(),
              BlockStoreLocation.anyTier(), allocatorView.refreshView(), true);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Allocation after freeing space for block creation, {} ", dirView == null
                ? "no available dir." : "available bytes in dir: " + dirView.getAvailableBytes());
          }
        }
      }
      if (dirView == null) {
        break;
      }
      return dirView;
    }
    throw new ResourceExhaustedRuntimeException(
        format("Allocation failure. Options: %s. Error:", options), false);
  }

  /**
   * Creates a temp block meta only if allocator finds available space. This method will not trigger
   * any eviction.
   *
   * @param sessionId session id
   * @param blockId block id
   * @param newBlock true if this temp block is created for a new block
   * @param options block allocation options
   * @return a temp block created if successful
   */
  private TempBlockMeta createBlockMetaInternal(long sessionId, long blockId, boolean newBlock,
      AllocateOptions options) {
    if (newBlock) {
      checkBlockDoesNotExist(blockId);
      checkTempBlockDoesNotExist(blockId);
    }
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      // NOTE: a temp block is supposed to be visible for its own writer,
      // unnecessary to acquire block lock here since no sharing.
      // Allocate space.
      StorageDirView dirView = allocateSpace(sessionId, options);

      // TODO(carson): Add tempBlock to corresponding storageDir and remove the use of
      // StorageDirView.createTempBlockMeta.
      TempBlockMeta tempBlock =
          mTempBlockMetaFactory.createTempBlockMeta(sessionId, blockId, options.getSize(), dirView);
      // Add allocated temp block to metadata manager. This should never fail if allocator
      // correctly assigns a StorageDir.
      mMetaManager.addTempBlockMeta(tempBlock);
      return tempBlock;
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
   *
   * @param sessionId the session id
   * @param minContiguousBytes the minimum amount of contigious free space in bytes
   * @param minAvailableBytes the minimum amount of free space in bytes
   * @param location the location to free space
   */
  @VisibleForTesting
  public synchronized void freeSpace(long sessionId, long minContiguousBytes,
      long minAvailableBytes, BlockStoreLocation location) {
    LOG.debug("freeSpace: sessionId={}, minContiguousBytes={}, minAvailableBytes={}, location={}",
        sessionId, minAvailableBytes, minAvailableBytes, location);
    // TODO(ggezer): Too much memory pressure when pinned-inodes list is large.
    BlockMetadataEvictorView evictorView = getUpdatedView();
    boolean contiguousSpaceFound = false;
    boolean availableBytesFound = false;

    int blocksIterated = 0;
    int blocksRemoved = 0;
    int spaceFreed = 0;

    // List of all dirs that belong to the given location.
    List<StorageDirView> dirViews = evictorView.getDirs(location);

    Iterator<Long> evictionCandidates = mMetaManager.getBlockIterator()
        .getIterator(location, BlockOrder.NATURAL);
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
        Optional<BlockMeta> optionalBlockMeta = mMetaManager.getBlockMeta(blockToDelete);
        if (!optionalBlockMeta.isPresent()) {
          LOG.warn("Failed to evict blockId {}, it could be already deleted", blockToDelete);
          continue;
        }
        BlockMeta blockMeta = optionalBlockMeta.get();
        removeBlockFileAndMeta(blockMeta);
        blocksRemoved++;
        for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
          synchronized (listener) {
            listener.onRemoveBlockByWorker(blockMeta.getBlockId());
            listener.onRemoveBlock(blockMeta.getBlockId(),
                blockMeta.getBlockLocation());
          }
        }
        spaceFreed += blockMeta.getBlockSize();
      }
    }

    if (!contiguousSpaceFound || !availableBytesFound) {
      throw new ResourceExhaustedRuntimeException(
          format("Failed to free %d bytes space at location %s. "
                  + "Min contiguous requested: %d, Min available requested: %d, "
                  + "Blocks iterated: %d, Blocks removed: %d, Space freed: %d",
              minAvailableBytes, location.tierAlias(), minContiguousBytes, minAvailableBytes,
              blocksIterated, blocksRemoved, spaceFreed), false);
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
   * @param moveOptions the allocate options for the move
   * @return the resulting information about the move operation
   */
  private MoveBlockResult moveBlockInternal(long sessionId, long blockId,
      AllocateOptions moveOptions) throws IOException {
    try (BlockLock lock = mLockManager.acquireBlockLock(sessionId, blockId, BlockLockType.WRITE)) {
      checkTempBlockDoesNotExist(blockId);
      BlockMeta srcBlockMeta;
      try (LockResource r = new LockResource(mMetadataReadLock)) {
        srcBlockMeta = mMetaManager.getBlockMeta(blockId).orElseThrow(() ->
          new IllegalStateException(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(blockId)));
      }

      BlockStoreLocation srcLocation = srcBlockMeta.getBlockLocation();
      String srcFilePath = srcBlockMeta.getPath();
      long blockSize = srcBlockMeta.getBlockSize();
      // Update moveOptions with the block size.
      moveOptions.setSize(blockSize);
      if (srcLocation.belongsTo(moveOptions.getLocation())) {
        return new MoveBlockResult(true, blockSize, srcLocation, srcLocation);
      }

      TempBlockMeta dstTempBlock;
      try {
        dstTempBlock = createBlockMetaInternal(sessionId, blockId, false, moveOptions);
      } catch (Exception e) {
        return new MoveBlockResult(false, blockSize, null, null);
      }

      // When `newLocation` is some specific location, the `newLocation` and the `dstLocation` are
      // just the same; while for `newLocation` with a wildcard significance, the `dstLocation`
      // is a specific one with specific tier and dir which belongs to newLocation.
      BlockStoreLocation dstLocation = dstTempBlock.getBlockLocation();

      // When the dstLocation belongs to srcLocation, simply abort the tempBlockMeta just created
      // internally from the newLocation and return success with specific block location.
      if (dstLocation.belongsTo(srcLocation)) {
        mMetaManager.abortTempBlockMeta(dstTempBlock);
        return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
      }
      String dstFilePath = dstTempBlock.getCommitPath();

      // Heavy IO is guarded by block lock but not metadata lock. This may throw IOException.
      FileUtils.move(srcFilePath, dstFilePath);

      try (LockResource r = new LockResource(mMetadataWriteLock)) {
        // If this metadata update fails, we panic for now.
        // TODO(bin): Implement rollback scheme to recover from IO failures.
        mMetaManager.moveBlockMeta(srcBlockMeta, dstTempBlock);
      }
      return new MoveBlockResult(true, blockSize, srcLocation, dstLocation);
    }
  }

  /**
   * Removes a block physically and from metadata.
   *
   * @param blockMeta block metadata
   */
  private void removeBlockFileAndMeta(BlockMeta blockMeta) {
    FileUtils.delete(blockMeta.getPath());
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
  private static void createBlockFile(String blockPath) {
    FileUtils.createBlockPath(blockPath,
        Configuration.getString(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS));
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
  public void removeInaccessibleStorage() {
    try (LockResource r = new LockResource(mMetadataWriteLock)) {
      for (StorageTier tier : mMetaManager.getTiers()) {
        for (StorageDir dir : tier.getStorageDirs()) {
          String path = dir.getDirPath();
          if (!FileUtils.isStorageDirAccessible(path)) {
            LOG.error("Storage check failed for path {}. The directory will be excluded.", path);
            removeDir(dir);
          }
        }
      }
    }
  }

  @Override
  public BlockMetadataManager getMetadataManager() {
    return mMetaManager;
  }

  @Override
  public BlockLockManager getLockManager() {
    return mLockManager;
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
      for (BlockStoreEventListener listener : mBlockStoreEventListeners) {
        synchronized (listener) {
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
