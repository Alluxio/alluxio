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
import static java.util.Objects.requireNonNull;

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryUtils;
import alluxio.underfs.UfsManager;
import alluxio.util.IdUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.grpc.GrpcExecutors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A implementation of BlockStore.
 * Each block will be stored and processed as a complete unit.
 */
public class MonoBlockStore implements BlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(MonoBlockStore.class);

  private final LocalBlockStore mLocalBlockStore;
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final AtomicReference<Long> mWorkerId;

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
    OptionalLong lockId = OptionalLong.of(
        mLocalBlockStore.commitBlockLocked(sessionId, blockId, pinOnCreate));

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      BlockMeta meta = mLocalBlockStore.getVolatileBlockMeta(blockId).get();
      BlockStoreLocation loc = meta.getBlockLocation();
      blockMasterClient.commitBlock(mWorkerId.get(),
          mLocalBlockStore.getBlockStoreMeta().getUsedBytesOnTiers().get(loc.tierAlias()),
          loc.tierAlias(), loc.mediumType(), blockId, meta.getBlockSize());
    } catch (AlluxioStatusException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      if (lockId.isPresent()) {
        mLocalBlockStore.unpinBlock(lockId.getAsLong());
      }
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
      String errorMessage = String.format("Failed to read from UFS, sessionId=%d, "
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
  public OptionalLong pinBlock(long sessionId, long blockId) {
    return mLocalBlockStore.pinBlock(sessionId, blockId);
  }

  @Override
  public void unpinBlock(long id) {
    mLocalBlockStore.unpinBlock(id);
  }

  @Override
  public void updatePinnedInodes(Set<Long> inodes) {
    mLocalBlockStore.updatePinnedInodes(inodes);
  }

  @Override
  public void updateReplicaInfo(Map<Long, Long> ReplicaInfo) {
    mLocalBlockStore.updateReplicaInfo(ReplicaInfo);
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
  public List<BlockStatus> load(List<Block> blocks, String tag, OptionalLong bandwidth) {
    ArrayList<CompletableFuture<BlockStatus>> futures = new ArrayList<>();
    ImmutableList.Builder<BlockStatus> errors = new ImmutableList.Builder<>();
    long sessionId = IdUtils.createSessionId();
    for (Block block : blocks) {
      long blockId = block.getBlockId();
      long blockSize = block.getBlockSize();
      BlockWriter blockWriter;
      UfsIOManager manager;
      BlockStoreLocation loc =
          BlockStoreLocation.anyDirInTier(WORKER_STORAGE_TIER_ASSOC.getAlias(0));
      try {
        manager = mUnderFileSystemBlockStore.getOrAddUfsIOManager(block.getMountId());
        if (bandwidth.isPresent()) {
          manager.setQuota(tag, bandwidth.getAsLong());
        }
        mLocalBlockStore.createBlock(sessionId, blockId, AllocateOptions.forCreate(blockSize, loc));
        blockWriter = mLocalBlockStore.createBlockWriter(sessionId, blockId);
      } catch (Exception e) {
        AlluxioRuntimeException exception = AlluxioRuntimeException.from(e);
        errors.add(BlockStatus.newBuilder().setBlock(block)
            .setCode(exception.getStatus().getCode().value()).setMessage(e.getMessage())
            .setRetryable(exception.isRetryable()).build());
        continue;
      }
      CompletableFuture<BlockStatus> future = RetryUtils.retryCallable("read from ufs",
              () -> manager.read(blockId, block.getOffsetInFile(), blockSize, block.getUfsPath(),
                  false, tag), new ExponentialBackoffRetry(1000, 5000, 5))
          .thenAcceptAsync(d -> blockWriter.append(ByteBuffer.wrap(d)),
              GrpcExecutors.BLOCK_WRITER_EXECUTOR)
          .thenRun(() -> {
            try {
              blockWriter.close();
            } catch (IOException e) {
              abortBlock(sessionId, block.getBlockId());
              throw AlluxioRuntimeException.from(e);
            }
          })
          .thenRun(() -> commitBlock(sessionId, blockId, false)).handle((res, throwable) -> {
            if (throwable == null) {
              return null;
            } else {
              try {
                abortBlock(sessionId, block.getBlockId());
              } catch (Exception e) {
                throwable.addSuppressed(e);
              }
              AlluxioRuntimeException exception = AlluxioRuntimeException.from(throwable);
              return BlockStatus.newBuilder().setBlock(block)
                  .setCode(exception.getStatus().getCode().value())
                  .setMessage(throwable.getMessage()).setRetryable(exception.isRetryable()).build();
            }
          });
      futures.add(future);
    }
    errors.addAll(futures.stream().map(CompletableFuture::join).filter(Objects::nonNull)
        .collect(Collectors.toList()));
    return errors.build();
  }

  @Override
  public void close() throws IOException {
    mLocalBlockStore.close();
    mUnderFileSystemBlockStore.close();
  }
}
