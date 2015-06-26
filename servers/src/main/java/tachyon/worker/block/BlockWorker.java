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
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Throwables;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Users;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.web.UIWebServer;
import tachyon.web.WorkerUIWebServer;
import tachyon.worker.DataServer;

/**
 * The class responsible for managing all top level components of the Block Worker. These include:
 *
 * Servers: BlockServiceHandler (RPC Server), BlockDataServer (Data Server)
 *
 * Periodic Threads: BlockMasterSync (Worker to Master continuous communication)
 *
 * Logic: BlockDataManager (Logic for all block related storage operations)
 *
 */
public class BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;
  /** Logic for handling RPC requests. */
  private BlockServiceHandler mServiceHandler;
  /** Logic for managing block store and under file system store. */
  private BlockDataManager mBlockDataManager;
  /** Server for data requests and responses. */
  private DataServer mDataServer;
  /** Threadpool for the master sync */
  private ExecutorService mSyncExecutorService;
  /** Net address of this worker */
  private NetAddress mWorkerNetAddress;
  /** Configuration object */
  private TachyonConf mTachyonConf;
  /** Server socket for thrift */
  private TServerSocket mThriftServerSocket;
  /** Thread pool for trift */
  private TThreadPoolServer mThriftServer;
  /** Users object for tracking metadata */
  private Users mUsers;
  /** Port to run RPC server on */
  private int mThriftPort;
  /** Id of this worker */
  private long mWorkerId;
  /** Under file system folder for temporary files. */
  private String mUfsWorkerFolder;
  /** Worker start time in milliseconds */
  private final long mStartTimeMs;
  /** Worker Web UI server */
  private final UIWebServer mWebServer;

  /**
   * Creates a Tachyon Block Worker.
   * @param tachyonConf the configuration values to be used
   */
  public BlockWorker(TachyonConf tachyonConf) throws IOException {
    mTachyonConf = tachyonConf;
    mStartTimeMs = System.currentTimeMillis();

    // Set up BlockDataManager
    mBlockDataManager = new BlockDataManager(tachyonConf);

    // Set up DataServer
    int dataServerPort =
        tachyonConf.getInt(Constants.WORKER_DATA_PORT, Constants.DEFAULT_WORKER_DATA_SERVER_PORT);
    InetSocketAddress dataServerAddress =
        new InetSocketAddress(NetworkUtils.getLocalHostName(tachyonConf), dataServerPort);
    mDataServer =
        DataServer.Factory.createDataServer(dataServerAddress, mBlockDataManager, mTachyonConf);

    // Setup RPC Server
    mServiceHandler = new BlockServiceHandler(mBlockDataManager);
    mThriftServerSocket = createThriftServerSocket();
    mThriftPort = NetworkUtils.getPort(mThriftServerSocket);
    mThriftServer = createThriftServer();
    mWorkerNetAddress =
        new NetAddress(getWorkerAddress().getAddress().getCanonicalHostName(), mThriftPort,
            mDataServer.getPort());

    // Setup Worker to Master Syncer
    mSyncExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-heartbeat-%d"));
    mBlockMasterSync = new BlockMasterSync(mBlockDataManager, mTachyonConf, mWorkerNetAddress);
    mBlockMasterSync.registerWithMaster();

    // Setup user metadata mapping
    // TODO: Have a top level register that gets the worker id.
    mWorkerId = mBlockMasterSync.getWorkerId();
    String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
    String ufsAddress =
        mTachyonConf.get(Constants.UNDERFS_ADDRESS, tachyonHome + "/underFSStorage");
    String ufsWorkerFolder =
        mTachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER, ufsAddress + "/tachyon/workers");
    mUfsWorkerFolder = CommonUtils.concatPath(ufsWorkerFolder, mWorkerId);
    mUsers = new Users(mUfsWorkerFolder, mTachyonConf);

    // Give BlockDataManager a pointer to the user metadata mapping
    // TODO: Fix this hack when we have a top level register
    mBlockDataManager.setUsers(mUsers);
    mBlockDataManager.setWorkerId(mWorkerId);

    int webPort =
        mTachyonConf.getInt(Constants.WORKER_WEB_PORT, Constants.DEFAULT_WORKER_WEB_PORT);
    mWebServer =
        new WorkerUIWebServer("Tachyon Worker", new InetSocketAddress(
            mWorkerNetAddress.getMHost(), webPort), this, mTachyonConf);
  }

  /**
   * Runs the block worker. The thread calling this will be blocked until the thrift server shuts
   * down.
   */
  public void process() {
    mSyncExecutorService.submit(mBlockMasterSync);
    mWebServer.startWebServer();
    mThriftServer.serve();
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   * @throws IOException if the data server fails to close.
   */
  public void stop() throws IOException {
    mDataServer.close();
    mThriftServer.stop();
    mThriftServerSocket.close();
    mBlockMasterSync.stop();
    mSyncExecutorService.shutdown();
    while (!mDataServer.isClosed() || mThriftServer.isServing()) {
      // TODO: The reason to stop and close again is due to some issues in Thrift.
      mThriftServer.stop();
      mThriftServerSocket.close();
      CommonUtils.sleepMs(null, 100);
    }
  }

  /**
   * Helper method to create a thrift server socket.
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(getWorkerAddress());
    } catch (TTransportException tte) {
      LOG.error(tte.getMessage(), tte);
      throw Throwables.propagate(tte);
    }
  }

  /**
   * Helper method to create a thrift server.
   * @return a thrift server
   */
  private TThreadPoolServer createThriftServer() {
    int minWorkerThreads =
        mTachyonConf.getInt(Constants.WORKER_MIN_WORKER_THREADS, Runtime.getRuntime()
            .availableProcessors());
    int maxWorkerThreads =
        mTachyonConf.getInt(Constants.WORKER_MAX_WORKER_THREADS,
            Constants.DEFAULT_WORKER_MAX_WORKER_THREADS);
    WorkerService.Processor<BlockServiceHandler> processor =
        new WorkerService.Processor<BlockServiceHandler>(mServiceHandler);
    return new TThreadPoolServer(new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(new TFramedTransport.Factory())
        .protocolFactory(new TBinaryProtocol.Factory(true, true)));
  }

  /**
   * Get the worker start time (in UTC) in milliseconds.
   * @return the worker start time in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  /**
   * Gets the meta data of the entire store.
   * @return store meta data
   */
  public BlockStoreMeta getStoreMeta() {
    return mBlockDataManager.getStoreMeta();
  }

  /**
   * Helper method to get the {@link java.net.InetSocketAddress} of the worker.
   * @return the worker's address
   */
  private InetSocketAddress getWorkerAddress() {
    String workerHostname = NetworkUtils.getLocalHostName(mTachyonConf);
    int workerPort = mTachyonConf.getInt(Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT);
    return new InetSocketAddress(workerHostname, workerPort);
  }

  // Methods for unit test purposes only

  public NetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  public BlockServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }
}
