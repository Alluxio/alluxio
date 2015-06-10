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

  private BlockMasterSync mBlockMasterSync;
  private BlockServiceHandler mServiceHandler;
  private tachyon.worker.block.BlockDataManager mBlockDataManager;
  private DataServer mDataServer;
  private ExecutorService mHeartbeatExecutorService;
  private NetAddress mWorkerNetAddress;
  private TachyonConf mTachyonConf;
  private TServerSocket mThriftServerSocket;
  private TThreadPoolServer mThriftServer;
  private Users mUsers;
  private int mThriftPort;
  private int mWorkerId;
  private String mUfsWorkerFolder;

  public BlockWorker(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mBlockDataManager = new BlockDataManager(tachyonConf);

    int dataServerPort =
        tachyonConf.getInt(Constants.WORKER_DATA_PORT, Constants.DEFAULT_WORKER_DATA_SERVER_PORT);
    InetSocketAddress dataServerAddress =
        new InetSocketAddress(NetworkUtils.getLocalHostName(tachyonConf), dataServerPort);
    mDataServer =
        DataServer.Factory.createDataServer(dataServerAddress, mBlockDataManager, mTachyonConf);

    mServiceHandler = new BlockServiceHandler(mBlockDataManager);
    mThriftServerSocket = createThriftServerSocket();
    mThriftPort = NetworkUtils.getPort(mThriftServerSocket);
    mThriftServer = createThriftServer();
    mWorkerNetAddress =
        new NetAddress(getWorkerAddress().getAddress().getCanonicalHostName(), mThriftPort,
            mDataServer.getPort());
    mHeartbeatExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-heartbeat-%d"));
    mBlockMasterSync = new BlockMasterSync(mBlockDataManager, mTachyonConf, mWorkerNetAddress);
    mBlockMasterSync.registerWithMaster();
    // TODO: Have a top level register that gets the worker id.
    mWorkerId = mBlockMasterSync.getWorkerId();

    String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
    String ufsAddress =
        mTachyonConf.get(Constants.UNDERFS_ADDRESS, tachyonHome + "/underFSStorage");
    String ufsWorkerFolder =
        mTachyonConf.get(Constants.UNDERFS_WORKERS_FOLDER, ufsAddress + "/tachyon/workers");
    mUfsWorkerFolder = CommonUtils.concatPath(ufsWorkerFolder, mWorkerId);

    mUsers = new Users(mUfsWorkerFolder, mTachyonConf);
    // TODO: Fix this hack when we have a top level register
    mBlockDataManager.setUsers(mUsers);
  }

  public void process() {
    mHeartbeatExecutorService.submit(mBlockMasterSync);
    mThriftServer.serve();
  }

  public void stop() throws IOException {
    mDataServer.close();
    mThriftServer.stop();
    mThriftServerSocket.close();
    mHeartbeatExecutorService.shutdown();
    while (!mDataServer.isClosed() || mThriftServer.isServing()) {
      // TODO: The reason to stop and close again is due to some issues in Thrift.
      mThriftServer.stop();
      mThriftServerSocket.close();
      CommonUtils.sleepMs(null, 100);
    }
  }

  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(getWorkerAddress());
    } catch (TTransportException tte) {
      LOG.error(tte.getMessage(), tte);
      throw Throwables.propagate(tte);
    }
  }

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

  private InetSocketAddress getWorkerAddress() {
    String workerHostname = NetworkUtils.getLocalHostName(mTachyonConf);
    int workerPort = mTachyonConf.getInt(Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT);
    return new InetSocketAddress(workerHostname, workerPort);
  }
}
