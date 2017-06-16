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

package alluxio.worker.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.TProcessor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for persisting files when requested by the master and a defunct
 * {@link FileSystemWorkerClientServiceHandler} which always returns UnsupportedOperation Exception.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultFileSystemWorker extends AbstractWorker implements FileSystemWorker {
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockWorker.class);

  /** Logic for managing file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Logic for handling RPC requests. */
  private final FileSystemWorkerClientServiceHandler mServiceHandler;
  /** This worker's worker ID. May be updated by another thread if worker re-registration occurs. */
  private final AtomicReference<Long> mWorkerId;

  /** The service that persists files. */
  private Future<?> mFilePersistenceService;
  /** Handler to the ufs manager. */
  private final UfsManager mUfsManager;

  /**
   * Creates a new DefaultFileSystemWorker.
   *
   * @param blockWorker the block worker handle
   * @param ufsManager the ufs manager
   */
  DefaultFileSystemWorker(BlockWorker blockWorker, UfsManager ufsManager) {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));
    mWorkerId = blockWorker.getWorkerId();
    mUfsManager = ufsManager;
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockWorker, "blockWorker"),
        RateLimiter.create(Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT)),
        mUfsManager);

    // Setup AbstractMasterClient
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));

    mServiceHandler = new FileSystemWorkerClientServiceHandler();
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_WORKER_NAME;
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME,
        new FileSystemWorkerClientService.Processor<>(mServiceHandler));
    return services;
  }

  @Override
  public void start(WorkerNetAddress address) {
    mFilePersistenceService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient,
                mWorkerId),
            (int) Configuration.getMs(PropertyKey.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));
  }

  @Override
  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    // The executor shutdown needs to be done in a loop with retry because the interrupt
    // signal can sometimes be ignored.
    CommonUtils.waitFor("file system worker executor shutdown", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        getExecutorService().shutdownNow();
        try {
          return getExecutorService().awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });
    mFileSystemMasterWorkerClient.close();
  }
}
