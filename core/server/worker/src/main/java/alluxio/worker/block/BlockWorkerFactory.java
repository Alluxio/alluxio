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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.worker.WorkerFactory;
import alluxio.worker.WorkerRegistry;
import alluxio.worker.page.PagedBlockStore;
import alluxio.worker.page.PagedBlockWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link BlockWorker} instance.
 */
@ThreadSafe
public final class BlockWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerFactory.class);

  /**
   * Constructs a new {@link BlockWorkerFactory}.
   */
  public BlockWorkerFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public BlockWorker create(WorkerRegistry registry, UfsManager ufsManager) {
    BlockWorker blockWorker;
    switch (Configuration.global()
        .getEnum(PropertyKey.USER_BLOCK_STORE_TYPE, BlockStoreType.class)) {
      case PAGE:
        LOG.info("Creating PagedBlockWorker");
        blockWorker =
            new PagedBlockWorker(ufsManager, PagedBlockStore.createPagedBlockStore(ufsManager));
        break;
      case FILE:
        LOG.info("Creating DefaultBlockWorker");
        blockWorker = new TieredBlockWorker(ufsManager, new TieredBlockStore());
        break;
      default:
        throw new UnsupportedOperationException("Unsupported block store type.");
    }
    registry.add(BlockWorker.class, blockWorker);
    return blockWorker;
  }
}
