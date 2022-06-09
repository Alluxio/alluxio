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

package alluxio.worker.page;

import alluxio.ClientContext;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.master.MasterClientContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.annotations.VisibleForTesting;

/**
 * Paged Block Worker Implementation.
 */
public class PagedBlockWorker extends DefaultBlockWorker {

  private final PagedBlockStore mPagedBlockStore;

  /**
   * Constructs a tiered block worker.
   *
   * @param ufsManager ufs manager
   * @param pagedBlockStore a paged block store
   */
  public PagedBlockWorker(UfsManager ufsManager, PagedBlockStore pagedBlockStore) {
    this(new BlockMasterClientPool(),
        new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(Configuration.global())).build()),
        new Sessions(), pagedBlockStore, ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param pagedBlockStore the paged block store
   * @param ufsManager ufs manager
   */
  @VisibleForTesting
  public PagedBlockWorker(BlockMasterClientPool blockMasterClientPool,
                          FileSystemMasterClient fileSystemMasterClient, Sessions sessions,
                          PagedBlockStore pagedBlockStore,
                          UfsManager ufsManager) {
    super(blockMasterClientPool, fileSystemMasterClient, sessions, pagedBlockStore, ufsManager);
    mPagedBlockStore = pagedBlockStore;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
                                       boolean positionShort,
                                       Protocol.OpenUfsBlockOptions options) {
    BlockReader reader = mPagedBlockStore.createBlockReader(blockId, options);
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return reader;
  }
}
