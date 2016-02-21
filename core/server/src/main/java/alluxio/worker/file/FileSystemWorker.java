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

package alluxio.worker.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AbstractWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for managing all top level components of the file system worker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class FileSystemWorker extends AbstractWorker {
  /** Logic for managing file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Configuration object. */
  private final Configuration mConf;

  /** The service that persists files. */
  private Future<?> mFilePersistenceService;

  /**
   * Creates a new instance of {@link FileSystemWorker}.
   *
   * @param blockWorker the block worker handle
   * @throws IOException if an I/O error occurs
   */
  public FileSystemWorker(BlockWorker blockWorker) throws IOException {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));

    mConf = WorkerContext.getConf();
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockWorker));

    // Setup AbstractMasterClient
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);
  }

  /**
   * {@inheritDoc}
   * <p>
   * {@link FileSystemWorker} exposes no RPC service.
   */
  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<String, TProcessor>();
  }

  /**
   * Starts the filesystem worker service.
   */
  @Override
  public void start() {
    mFilePersistenceService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient),
            mConf.getInt(Constants.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));
  }

  /**
   * Stops the filesystem worker service.
   */
  @Override
  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    mFileSystemMasterWorkerClient.close();
    getExecutorService().shutdown();
  }
}
