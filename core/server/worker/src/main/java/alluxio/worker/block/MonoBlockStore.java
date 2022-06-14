package alluxio.worker.block;

import static alluxio.worker.block.BlockMetadataManager.WORKER_STORAGE_TIER_ASSOC;
import static java.util.Objects.requireNonNull;

import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.UnavailableException;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
  public void abortBlock(long sessionId, long blockId) throws IOException {
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
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws IOException {
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
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
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
      CreateBlockOptions createBlockOptions)
      throws WorkerOutOfSpaceException, IOException {
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
      throw new UnavailableException(String.format("Failed to read from UFS, sessionId=%d, "
              + "blockId=%d, offset=%d, positionShort=%s, options=%s: %s",
          sessionId, blockId, offset, positionShort, options, e), e);
    }
  }

  private void closeUfsBlock(long sessionId, long blockId)
      throws IOException {
    try {
      mUnderFileSystemBlockStore.close(sessionId, blockId);
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
      throws WorkerOutOfSpaceException, IOException {
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
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws WorkerOutOfSpaceException, IOException {
    mLocalBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void close() throws IOException {
    mLocalBlockStore.close();
  }
}
