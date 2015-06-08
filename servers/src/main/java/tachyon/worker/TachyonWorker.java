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

package tachyon.worker;

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
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerService;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.block.BlockWorkerServiceHandler;

/**
 * The main program that runs the Tachyon Worker. The Tachyon Worker is responsible for managing its
 * own local Tachyon space as well as its under storage system space.
 */
public class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private BlockWorkerServiceHandler mServiceHandler;
  private CoreWorker mCoreWorker;
  private DataServer mDataServer;
  private ExecutorService mHeartbeatExecutorService;
  private NetAddress mWorkerNetAddress;
  private TachyonConf mTachyonConf;
  private TServerSocket mThriftServerSocket;
  private TThreadPoolServer mThriftServer;
  private boolean shouldRun;
  private int mThriftPort;

  public TachyonWorker(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mCoreWorker = new CoreWorker();
    mDataServer = DataServer.Factory.createDataServer(mTachyonConf, mCoreWorker);
    mServiceHandler = new BlockWorkerServiceHandler(mCoreWorker);
    mThriftServerSocket = createThriftServerSocket();
    mThriftPort = NetworkUtils.getPort(mThriftServerSocket);
    mThriftServer = createThriftServer();
    mWorkerNetAddress =
        new NetAddress(getWorkerAddress().getAddress().getCanonicalHostName(), mThriftPort,
            mDataServer.getPort());
    mHeartbeatExecutorService =
        Executors.newFixedThreadPool(1, ThreadFactoryUtils.daemon("worker-heartbeat-%d"));
  }

  public static void main(String[] args) {
    checkArgs(args);
    TachyonConf tachyonConf = new TachyonConf();
    TachyonWorker worker = new TachyonWorker(tachyonConf);
    try {
      worker.join();
    } catch (Exception e) {
      LOG.error("Uncaught exception, shutting down Tachyon Worker", e);
      System.exit(-1);
    }
  }

  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java TachyonWorker");
      System.exit(-1);
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
    WorkerService.Processor<BlockWorkerServiceHandler> processor =
        new WorkerService.Processor<BlockWorkerServiceHandler>(mServiceHandler);
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

  public void join() {}
}
