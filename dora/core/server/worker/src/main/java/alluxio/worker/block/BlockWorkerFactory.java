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

import static java.util.Objects.requireNonNull;

import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.util.FeatureUtils;
import alluxio.worker.WorkerFactory;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.inject.Inject;
import com.google.inject.name.Named;
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
  private final boolean mWorkerRegisterToAllMasters = Configuration.getBoolean(
      PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS);

  private final UfsManager mUfsManager;

  private final BlockStore mBlockStore;

  private final BlockMasterClientPool mBlockMasterClientPool;

  private final FileSystemMasterClient mFileSystemMasterClient;

  private final AtomicReference<Long> mWorkerId;

  @Inject
  BlockWorkerFactory(UfsManager ufsManager,
      BlockStore blockStore,
      BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient,
      @Named("workerId") AtomicReference<Long> workerId) {
    mUfsManager = requireNonNull(ufsManager, "ufsManager is null");
    mBlockStore = requireNonNull(blockStore);
    mBlockMasterClientPool = requireNonNull(blockMasterClientPool);
    mFileSystemMasterClient = requireNonNull(fileSystemMasterClient);
    mWorkerId = requireNonNull(workerId);
  }

  @Override
  public boolean isEnabled() {
    return !FeatureUtils.isDora();
  }

  @Override
  public BlockWorker create() {
    mBlockStore.initialize();
    BlockWorker blockWorker = mWorkerRegisterToAllMasters
            ? new AllMasterRegistrationBlockWorker(mBlockMasterClientPool,
            mFileSystemMasterClient, new Sessions(), mBlockStore, mWorkerId)
            : new DefaultBlockWorker(mBlockMasterClientPool,
            mFileSystemMasterClient, new Sessions(), mBlockStore, mWorkerId);
    return blockWorker;
  }
}
