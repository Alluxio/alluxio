/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.FileSystemWorkerClientService;
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
  /** Logic for handling RPC requests. */
  private final FileSystemWorkerClientServiceHandler mServiceHandler;

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

    mServiceHandler = new FileSystemWorkerClientServiceHandler(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME,
        new FileSystemWorkerClientService.Processor<FileSystemWorkerClientServiceHandler>(
            getWorkerServiceHandler()));
    return services;
  }

  /**
   * @return the worker service handler
   */
  public FileSystemWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
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
