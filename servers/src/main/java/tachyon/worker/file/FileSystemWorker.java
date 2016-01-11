/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.file;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.WorkerBase;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.block.BlockDataManager;

/**
 * This class is responsible for managing all top level components of the file system worker.
 */
public final class FileSystemWorker extends WorkerBase {
  /** Logic for managing file persistence */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Configuration object */
  private final TachyonConf mTachyonConf;

  /** The service that persists files */
  private Future<?> mFilePersistenceService;

  /**
   * Creates a new instance of {@link FileSystemWorker}.
   *
   * @param blockDataManager a block data manager handle
   * @throws IOException if an I/O error occurs
   */
  public FileSystemWorker(BlockDataManager blockDataManager) throws IOException {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));
    Preconditions.checkState(WorkerIdRegistry.getWorkerId() != 0, "Failed to register worker");

    mTachyonConf = WorkerContext.getConf();
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockDataManager));

    // Setup MasterClientBase
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf), mTachyonConf);
  }

  /**
   * Starts the lineage worker service.
   */
  public void start() {
    mFilePersistenceService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient),
            mTachyonConf.getInt(Constants.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));
  }

  /**
   * Stops the lineage worker service.
   */
  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    mFileSystemMasterWorkerClient.close();
    getExecutorService().shutdown();
  }
}
