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

package tachyon.worker.block;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.thrift.BlockWorkerClientService;
import tachyon.util.CommonUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.DataServer;
import tachyon.worker.NetAddress;
import tachyon.worker.WorkerBase;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.file.FileSystemMasterClient;

/**
 * The class is responsible for managing all top level components of the Block Worker, including:
 *
 * Servers: {@link BlockWorkerClientServiceHandler} (RPC Server), BlockDataServer (Data Server)
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link BlockDataManager} (Logic for all block related storage operations)
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. TACHYON-1624)
public final class BlockWorker extends WorkerBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleanerThread;

  /** Logic for handling RPC requests. */
  private final BlockWorkerClientServiceHandler mServiceHandler;

  /** Logic for managing block store and under file system store. */
  private final BlockDataManager mBlockDataManager;

  /** Server for data requests and responses. */
  private final DataServer mDataServer;

  /** Client for all block master communication. */
  private final BlockMasterClient mBlockMasterClient;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Configuration object. */
  private final TachyonConf mTachyonConf;

  /** Space reserver for the block data manager. */
  private SpaceReserver mSpaceReserver = null;

  /**
   * @return the block data manager for other worker services
   */
  public BlockDataManager getBlockDataManager() {
    return mBlockDataManager;
  }

  /**
   * @return the worker service handler
   */
  public BlockWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }

  /**
   * @return the worker data service bind host
   */
  public String getDataBindHost() {
    return mDataServer.getBindHost();
  }

  /**
   * @return the worker data service port
   */
  public int getDataLocalPort() {
    return mDataServer.getPort();
  }

  /**
   * Creates a new instance of {@link BlockWorker}.
   *
   * @throws IOException for other exceptions
   */
  public BlockWorker() throws IOException {
    super(Executors.newFixedThreadPool(4,
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mTachyonConf = WorkerContext.getConf();

    // Setup BlockMasterClient
    mBlockMasterClient = new BlockMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf), mTachyonConf);

    mFileSystemMasterClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf), mTachyonConf);

    // Set up BlockDataManager
    mBlockDataManager = new BlockDataManager(WorkerContext.getWorkerSource(), mBlockMasterClient,
        mFileSystemMasterClient, new TieredBlockStore());

    // Setup DataServer
    mDataServer = DataServer.Factory.create(
        NetworkAddressUtils.getBindAddress(ServiceType.WORKER_DATA, mTachyonConf),
        mBlockDataManager, mTachyonConf);
    // Reset data server port
    mTachyonConf.set(Constants.WORKER_DATA_PORT, Integer.toString(mDataServer.getPort()));

    // Setup RPC ServerHandler
    mServiceHandler = new BlockWorkerClientServiceHandler(mBlockDataManager);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME,
        new BlockWorkerClientService.Processor<BlockWorkerClientServiceHandler>(
            getWorkerServiceHandler()));
    return services;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   *
   * @throws IOException if a non-Tachyon related exception occurs
   */
  @Override
  public void start() throws IOException {
    NetAddress netAddress;
    try {
      netAddress = WorkerContext.getNetAddress();
      WorkerIdRegistry.registerWithBlockMaster(mBlockMasterClient, netAddress);
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to get a worker id from block master", e);
      throw Throwables.propagate(e);
    }

    // Setup BlockMasterSync
    mBlockMasterSync =
        new BlockMasterSync(mBlockDataManager, netAddress, mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(mBlockDataManager, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleanerThread = new SessionCleaner(mBlockDataManager);

    // Setup space reserver
    if (mTachyonConf.getBoolean(Constants.WORKER_TIERED_STORE_RESERVER_ENABLED)) {
      mSpaceReserver = new SpaceReserver(mBlockDataManager);
    }

    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleanerThread);

    // Start the space reserver
    if (mSpaceReserver != null) {
      getExecutorService().submit(mSpaceReserver);
    }
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * @throws IOException if the data server fails to close
   */
  @Override
  public void stop() throws IOException {
    mDataServer.close();

    mSessionCleanerThread.stop();
    mBlockMasterClient.close();
    if (mSpaceReserver != null) {
      mSpaceReserver.stop();
    }
    mFileSystemMasterClient.close();
    // Use shutdownNow because HeartbeatThreads never finish until they are interrupted
    getExecutorService().shutdownNow();

    mBlockDataManager.stop();
    // TODO(binfan): investigate why we need to close dataserver again. There used to be a comment
    // saying the reason to stop and close again is due to some issues in Thrift.
    while (!mDataServer.isClosed()) {
      mDataServer.close();
      CommonUtils.sleepMs(100);
    }
  }

}
