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

import static alluxio.worker.block.BlockMetadataManager.WORKER_STORAGE_TIER_ASSOC;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.exception.runtime.DeadlineExceededRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import alluxio.underfs.FileId;
import alluxio.underfs.UfsIOManager;
import alluxio.underfs.UfsManager;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.io.LocalFileBlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.grpc.GrpcExecutors;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A implementation of BlockStore.
 * Each block will be stored and processed as a complete unit.
 */
public class MonoBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(MonoBlockStore.class);
  private static final long LOAD_TIMEOUT =
      Configuration.getMs(PropertyKey.USER_NETWORK_RPC_KEEPALIVE_TIMEOUT);

  private final BlockMetadataManager mMetaManager;

  private final BlockLockManager mLockManager;
  private final LocalBlockStore mLocalBlockStore;
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final AtomicReference<Long> mWorkerId;
  private final ScheduledExecutorService mDelayer =
      new ScheduledThreadPoolExecutor(1, ThreadFactoryUtils.build("LoadTimeOut", true));

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mMetadataLock = new ReentrantReadWriteLock();

  /** ReadLock provided by {@link #mMetadataLock} to guard metadata read operations. */
  private final Lock mMetadataReadLock = mMetadataLock.readLock();

  /** WriteLock provided by {@link #mMetadataLock} to guard metadata write operations. */
  private final Lock mMetadataWriteLock = mMetadataLock.writeLock();

  /**
   * Constructor of MonoBlockStore.
   *
   * @param localBlockStore
   * @param blockMasterClientPool
   * @param ufsManager
   * @param workerId
   */
  public MonoBlockStore(LocalBlockStore localBlockStore,
      BlockMasterClientPool blockMasterClientPool,
      UfsManager ufsManager,
      AtomicReference<Long> workerId) {
    mLocalBlockStore = requireNonNull(localBlockStore);
    mBlockMasterClientPool = requireNonNull(blockMasterClientPool);
    mUnderFileSystemBlockStore =
        new UnderFileSystemBlockStore(localBlockStore, requireNonNull(ufsManager));
    mWorkerId = workerId;
    mMetaManager = localBlockStore.getMetadataManager();
    mLockManager = localBlockStore.getLockManager();
  }

  @Override
  public void abortBlock(long sessionId, long blockId) {
    mLocalBlockStore.abortBlock(sessionId, blockId);
  }

  @Override
  public void accessBlock(long sessionId, long blockId) {
    mLocalBlockStore.accessBlock(sessionId, blockId);
  }

  @Override
  public void cleanupSession(long sessionId) {
    mLocalBlockStore.cleanupSession(sessionId);
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try (BlockLock lock = mLocalBlockStore.commitBlockLocked(sessionId, blockId, pinOnCreate)) {
      BlockMeta meta = mLocalBlockStore.getVolatileBlockMeta(blockId).get();
      BlockStoreLocation loc = meta.getBlockLocation();
      blockMasterClient.commitBlock(mWorkerId.get(),
          mLocalBlockStore.getBlockStoreMeta().getUsedBytesOnTiers().get(loc.tierAlias()),
          loc.tierAlias(), loc.mediumType(), blockId, meta.getBlockSize());
    } catch (AlluxioStatusException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.dec();
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions) {
    BlockStoreLocation loc;
    String tierAlias = WORKER_STORAGE_TIER_ASSOC.getAlias(tier);
    if (Strings.isNullOrEmpty(createBlockOptions.getMedium())) {
      loc = BlockStoreLocation.anyDirInTier(tierAlias);
    } else {
      loc = BlockStoreLocation.anyDirInAnyTierWithMedium(createBlockOptions.getMedium());
    }
    TempBlockMeta createdBlock;
    createdBlock = mLocalBlockStore.createBlock(sessionId, blockId,
        AllocateOptions.forCreate(createBlockOptions.getInitialBytes(), loc));
    DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return createdBlock.getPath();
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, IOException {
    LOG.debug("getBlockReader: sessionId={}, blockId={}, lockId={}", sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      BlockMeta blockMeta = mMetaManager.getBlockMeta(blockId).get();
      return new LocalFileBlockReader(blockMeta.getPath());
    }
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    BlockReader reader;
    Optional<? extends BlockMeta> blockMeta = mLocalBlockStore.getVolatileBlockMeta(blockId);
    if (blockMeta.isPresent()) {
      reader = mLocalBlockStore.createBlockReader(sessionId, blockId, offset);
    } else {
      boolean checkUfs = options != null && (options.hasUfsPath() || options.getBlockInUfsTier());
      if (!checkUfs) {
        throw new BlockDoesNotExistRuntimeException(blockId);
      }
      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      reader = createUfsBlockReader(sessionId, blockId, offset, positionShort, options);
    }
    DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return reader;
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort,
      Protocol.OpenUfsBlockOptions options)
      throws IOException {
    try {
      BlockReader reader = mUnderFileSystemBlockStore.createBlockReader(sessionId, blockId, offset,
          positionShort, options);
      return new DelegatingBlockReader(reader, () -> closeUfsBlock(sessionId, blockId));
    } catch (Exception e) {
      try {
        closeUfsBlock(sessionId, blockId);
      } catch (Exception ee) {
        LOG.warn("Failed to close UFS block", ee);
      }
      String errorMessage = format("Failed to read from UFS, sessionId=%d, "
              + "blockId=%d, offset=%d, positionShort=%s, options=%s: %s",
          sessionId, blockId, offset, positionShort, options, e);
      if (e instanceof FileNotFoundException) {
        throw new NotFoundException(errorMessage, e);
      }
      throw new UnavailableException(errorMessage, e);
    }
  }

  private void closeUfsBlock(long sessionId, long blockId)
      throws IOException {
    try {
      mUnderFileSystemBlockStore.closeBlock(sessionId, blockId);
      Optional<TempBlockMeta> tempBlockMeta = mLocalBlockStore.getTempBlockMeta(blockId);
      if (tempBlockMeta.isPresent() && tempBlockMeta.get().getSessionId() == sessionId) {
        commitBlock(sessionId, blockId, false);
      } else {
        // When getTempBlockMeta() return null, such as a block readType NO_CACHE writeType THROUGH.
        // Counter will not be decrement in the commitblock().
        // So we should decrement counter here.
        if (mUnderFileSystemBlockStore.isNoCache(sessionId, blockId)) {
          DefaultBlockWorker.Metrics.WORKER_ACTIVE_CLIENTS.dec();
        }
      }
    } finally {
      mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
    }
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId) throws IOException {
    return mLocalBlockStore.createBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockStoreMeta getBlockStoreMeta() {
    return mLocalBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getBlockStoreMetaFull() {
    return mLocalBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public Optional<TempBlockMeta> getTempBlockMeta(long blockId) {
    return mLocalBlockStore.getTempBlockMeta(blockId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mLocalBlockStore.hasBlockMeta(blockId);
  }

  @Override
  public boolean hasTempBlockMeta(long blockId) {
    return mLocalBlockStore.hasTempBlockMeta(blockId);
  }

  @Override
  public Optional<BlockMeta> getVolatileBlockMeta(long blockId) {
    return mLocalBlockStore.getVolatileBlockMeta(blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws IOException {
    mLocalBlockStore.moveBlock(sessionId, blockId, moveOptions);
  }

  @Override
  public Optional<BlockLock> pinBlock(long sessionId, long blockId) {
    return mLocalBlockStore.pinBlock(sessionId, blockId);
  }

  @Override
  public void unpinBlock(BlockLock lock) {
    lock.close();
  }

  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    mLocalBlockStore.updatePinnedInodes(inodes);
  }

  @Override
  public void registerBlockStoreEventListener(BlockStoreEventListener listener) {
    mLocalBlockStore.registerBlockStoreEventListener(listener);
  }

  @Override
  public void removeBlock(long sessionId, long blockId) throws IOException {
    mLocalBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void removeInaccessibleStorage() {
    mLocalBlockStore.removeInaccessibleStorage();
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes) {
    mLocalBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public CompletableFuture<List<BlockStatus>> load(List<Block> blocks, UfsReadOptions options) {
    ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
    List<BlockStatus> errors = Collections.synchronizedList(new ArrayList<>());
    long sessionId = Sessions.LOAD_SESSION_ID;
    for (Block block : blocks) {
      long blockId = block.getBlockId();
      long blockSize = block.getLength();
      BlockWriter blockWriter;
      UfsIOManager manager;
      BlockStoreLocation loc =
          BlockStoreLocation.anyDirInTier(WORKER_STORAGE_TIER_ASSOC.getAlias(0));
      try {
        manager = mUnderFileSystemBlockStore.getOrAddUfsIOManager(block.getMountId());
        if (options.hasBandwidth()) {
          manager.setQuota(options.getTag(), options.getBandwidth());
        }
        mLocalBlockStore.createBlock(sessionId, blockId, AllocateOptions.forCreate(blockSize, loc));
        blockWriter = mLocalBlockStore.createBlockWriter(sessionId, blockId);
      } catch (Exception e) {
        handleException(e, block, errors, sessionId);
        continue;
      }
      ByteBuffer buf = NioDirectBufferPool.acquire((int) blockSize);
      CompletableFuture<Void> future = RetryUtils.retryCallable("read from ufs",
              () -> manager.read(buf, block.getOffsetInFile(), blockSize,
                  FileId.of(IdUtils.fileIdFromBlockId(blockId)), block.getUfsPath(), options),
              new ExponentialBackoffRetry(1000, 5000, 5))
          // use orTimeout in java 11
          .applyToEither(timeoutAfter(LOAD_TIMEOUT, TimeUnit.MILLISECONDS), d -> d)
          .thenRunAsync(() -> {
            buf.flip();
            blockWriter.append(buf);
          }, GrpcExecutors.BLOCK_WRITER_EXECUTOR)
          .thenRun(() -> {
            try {
              blockWriter.close();
            } catch (IOException e) {
              throw AlluxioRuntimeException.from(e);
            } finally {
              NioDirectBufferPool.release(buf);
            }
          })
          .thenRun(() -> commitBlock(sessionId, blockId, false))
          .exceptionally(t -> {
            handleException(t.getCause(), block, errors, sessionId);
            return null;
          });
      futures.add(future);
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(x -> errors);
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    LOG.debug("getBlockMeta: sessionId={}, blockId={}, lockId={}", sessionId, blockId, lockId);
    mLockManager.validateLock(sessionId, blockId, lockId);
    try (LockResource r = new LockResource(mMetadataReadLock)) {
      return mMetaManager.getBlockMeta(blockId).get();
    }
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
      TempBlockMeta tempBlockMeta = mMetaManager.getTempBlockMeta(blockId).get();
      return new LocalFileBlockWriter(tempBlockMeta.getPath());
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
    Optional<TempBlockMeta> tempBlockMetaOptional = mMetaManager.getTempBlockMeta(blockId);
    if (null == tempBlockMetaOptional || !tempBlockMetaOptional.isPresent()) {
      throw new BlockDoesNotExistException(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND, blockId);
    }
    TempBlockMeta tempBlockMeta = tempBlockMetaOptional.get();
    long ownerSessionId = tempBlockMeta.getSessionId();
    if (ownerSessionId != sessionId) {
      throw new InvalidWorkerStateException(ExceptionMessage.BLOCK_ID_FOR_DIFFERENT_SESSION,
          blockId, ownerSessionId, sessionId);
    }
  }

  private void handleException(Throwable e, Block block, List<BlockStatus> errors, long sessionId) {
    LOG.warn("Load block failure: {}", block, e);
    AlluxioRuntimeException exception = AlluxioRuntimeException.from(e);
    BlockStatus.Builder builder = BlockStatus.newBuilder().setBlock(block)
        .setCode(exception.getStatus().getCode().value()).setRetryable(exception.isRetryable());
    if (exception.getMessage() != null) {
      builder.setMessage(exception.getMessage());
    }
    errors.add(builder.build());
    if (hasTempBlockMeta(block.getBlockId())) {
      try {
        abortBlock(sessionId, block.getBlockId());
      } catch (Exception ee) {
        LOG.warn(format("fail to abort temp block %s after failing to load block",
            block.getBlockId()), ee);
      }
    }
  }

  private <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit) {
    CompletableFuture<T> result = new CompletableFuture<>();
    mDelayer.schedule(() -> result.completeExceptionally(new DeadlineExceededRuntimeException(
        format("time out after waiting for %s %s", timeout, unit))), timeout, unit);
    return result;
  }

  @Override
  public void close() throws IOException {
    mLocalBlockStore.close();
    mUnderFileSystemBlockStore.close();
  }

  /**
   * get the local block store.
   * @return LocalBlockStore
   */
  public LocalBlockStore getLocalBlockStore() {
    return mLocalBlockStore;
  }
}
