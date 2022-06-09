package alluxio.worker.block;

import alluxio.ClientContext;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.master.MasterClientContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Optional;

/**
 * Tiered Block Worker Implementation.
 */
public class TieredBlockWorker extends DefaultBlockWorker {

  private final TieredBlockStore mTieredBlockStore;

  /**
   * Constructs a tiered block worker.
   * @param ufsManager
   * @param tieredBlockStore
   */
  public TieredBlockWorker(UfsManager ufsManager, TieredBlockStore tieredBlockStore) {
    this(new BlockMasterClientPool(),
        new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(Configuration.global())).build()),
        new Sessions(), tieredBlockStore, ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param tieredBlockStore an Alluxio block store
   * @param ufsManager ufs manager
   */
  @VisibleForTesting
  public TieredBlockWorker(BlockMasterClientPool blockMasterClientPool,
                           FileSystemMasterClient fileSystemMasterClient, Sessions sessions,
                           TieredBlockStore tieredBlockStore,
                           UfsManager ufsManager) {
    super(blockMasterClientPool, fileSystemMasterClient, sessions, tieredBlockStore, ufsManager);
    mTieredBlockStore = tieredBlockStore;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
                                       boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    BlockReader reader;
    Optional<? extends BlockMeta> blockMeta = mTieredBlockStore.getVolatileBlockMeta(blockId);
    if (blockMeta.isPresent()) {
      reader = mTieredBlockStore.createBlockReader(sessionId, blockId, offset);
    } else {
      boolean checkUfs = options != null && (options.hasUfsPath() || options.getBlockInUfsTier());
      if (!checkUfs) {
        throw new BlockDoesNotExistRuntimeException(blockId);
      }
      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      reader = createUfsBlockReader(sessionId, blockId, offset, positionShort, options);
    }
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return reader;
  }
}
