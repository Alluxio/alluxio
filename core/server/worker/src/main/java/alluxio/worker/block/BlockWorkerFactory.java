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

import alluxio.ClientContext;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterClientContext;
import alluxio.underfs.UfsManager;
import alluxio.worker.Worker;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerRegistry;
import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.page.PagedBlockStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link BlockWorker} instance.
 */
@ThreadSafe
public final class BlockWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerFactory.class);

  private static final boolean DORA_WORKER_ENABLED =
      Configuration.getBoolean(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED);

  /**
   * Constructs a new {@link BlockWorkerFactory}.
   */
  public BlockWorkerFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Worker create(WorkerRegistry registry, UfsManager ufsManager) {
    BlockMasterClientPool blockMasterClientPool = new BlockMasterClientPool();
    AtomicReference<Long> workerId = new AtomicReference<>(-1L);
    if (DORA_WORKER_ENABLED) {
      Worker worker = new PagedDoraWorker(workerId, Configuration.global());
      registry.add(Worker.class, worker);
      return worker;
    } else {
      BlockStore blockStore;
      switch (Configuration.global()
          .getEnum(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.class)) {
        case PAGE:
          LOG.info("Creating PagedBlockWorker");
          blockStore = PagedBlockStore.create(ufsManager, blockMasterClientPool, workerId);
          break;
        case FILE:
          LOG.info("Creating DefaultBlockWorker");
          blockStore =
              new MonoBlockStore(new TieredBlockStore(), blockMasterClientPool, ufsManager,
                  workerId);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported block store type.");
      }
      BlockWorker blockWorker = new DefaultBlockWorker(blockMasterClientPool,
          new FileSystemMasterClient(
              MasterClientContext.newBuilder(ClientContext.create(Configuration.global())).build()),
          new Sessions(), blockStore, workerId);
      registry.add(BlockWorker.class, blockWorker);
      return blockWorker;
    }
  }
}
