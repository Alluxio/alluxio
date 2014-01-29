/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.Version;
import tachyon.conf.WorkerConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.Command;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;

/**
 * Entry point for a worker daemon.
 */
public class Worker implements Runnable {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final InetSocketAddress MasterAddress;
  private final InetSocketAddress WorkerAddress;

  private TServer mServer;
  private TNonblockingServerSocket mServerTNonblockingServerSocket;

  private WorkerStorage mWorkerStorage;
  private WorkerServiceHandler mWorkerServiceHandler;
  private DataServer mDataServer;

  private Thread mDataServerThread;
  private Thread mHeartbeatThread;

  private volatile boolean mStop = false;

  private Worker(InetSocketAddress masterAddress, InetSocketAddress workerAddress, int dataPort,
      int selectorThreads, int acceptQueueSizePerThreads, int workerThreads,
      String dataFolder, long memoryCapacityBytes) {
    MasterAddress = masterAddress;
    WorkerAddress = workerAddress;

    mWorkerStorage =
        new WorkerStorage(MasterAddress, WorkerAddress, dataFolder, memoryCapacityBytes);

    mWorkerServiceHandler = new WorkerServiceHandler(mWorkerStorage);

    mDataServer = new DataServer(new InetSocketAddress(workerAddress.getHostName(), dataPort),
        mWorkerStorage);
    mDataServerThread = new Thread(mDataServer);

    mHeartbeatThread = new Thread(this);
    try {
      LOG.info("The worker server tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      // TODO This is for Thrift 0.8 or newer.
      //      mServer = new TThreadedSelectorServer(new TThreadedSelectorServer
      //          .Args(new TNonblockingServerSocket(workerAddress)).processor(processor)
      //          .selectorThreads(selectorThreads).acceptQueueSizePerThread(acceptQueueSizePerThreads)
      //          .workerThreads(workerThreads));

      // This is for Thrift 0.7.0, for Hive compatibility.
      mServerTNonblockingServerSocket = new TNonblockingServerSocket(workerAddress);
      mServer = new THsHaServer(new THsHaServer.Args(mServerTNonblockingServerSocket).
          processor(processor).workerThreads(workerThreads));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }
  }

  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    Command cmd = null;
    while (!mStop) {
      long diff = System.currentTimeMillis() - lastHeartbeatMs;
      if (diff < WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS) {
        LOG.debug("Heartbeat process takes " + diff + " ms.");
        CommonUtils.sleepMs(LOG, WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS - diff);
      } else {
        LOG.error("Heartbeat process takes " + diff + " ms.");
      }

      try {
        cmd = mWorkerStorage.heartbeat();

        lastHeartbeatMs = System.currentTimeMillis();
      } catch (BlockInfoException e) {
        LOG.error(e.getMessage(), e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerStorage.resetMasterClient();
        CommonUtils.sleepMs(LOG, 1000);
        cmd = null;
        if (System.currentTimeMillis() - lastHeartbeatMs >= WorkerConf.get().HEARTBEAT_TIMEOUT_MS) {
          System.exit(-1);
        }
      }

      if (cmd != null) {
        switch (cmd.mCommandType) {
        case Unknown :
          LOG.error("Unknown command: " + cmd);
          break;
        case Nothing :
          LOG.debug("Nothing command: " + cmd);
          break;
        case Register :
          LOG.info("Register command: " + cmd);
          mWorkerStorage.register();
          break;
        case Free :
          mWorkerStorage.freeBlocks(cmd.mData);
          LOG.info("Free command: " + cmd);
          break;
        case Delete :
          LOG.info("Delete command: " + cmd);
          break;
        default :
          CommonUtils.runtimeException("Un-recognized command from master " + cmd.toString());
        }
      }

      mWorkerStorage.checkStatus();
    }
  }

  public static synchronized Worker createWorker(InetSocketAddress masterAddress,
      InetSocketAddress workerAddress, int dataPort, int selectorThreads,
      int acceptQueueSizePerThreads, int workerThreads, String localFolder, long spaceLimitBytes) {
    return new Worker(masterAddress, workerAddress, dataPort, selectorThreads,
        acceptQueueSizePerThreads, workerThreads, localFolder, spaceLimitBytes);
  }

  public static synchronized Worker createWorker(String masterAddress,
      String workerAddress, int dataPort, int selectorThreads, int acceptQueueSizePerThreads,
      int workerThreads, String localFolder, long spaceLimitBytes) {
    String[] address = masterAddress.split(":");
    InetSocketAddress master = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    address = workerAddress.split(":");
    InetSocketAddress worker = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    return new Worker(master, worker, dataPort, selectorThreads,
        acceptQueueSizePerThreads, workerThreads, localFolder, spaceLimitBytes);
  }

  public void start() {
    mDataServerThread.start();
    mHeartbeatThread.start();

    LOG.info("The worker server started @ " + WorkerAddress);
    mServer.serve();
    LOG.info("The worker server ends @ " + WorkerAddress);
  }

  public void stop() throws IOException, InterruptedException {
    mStop = true;
    mWorkerStorage.stop();
    mDataServer.close();
    mServer.stop();
    mServerTNonblockingServerSocket.close();
    while (!mDataServer.isClosed() || mServer.isServing() || mHeartbeatThread.isAlive()) {
      // TODO The reason to stop and close again is due to some issues in Thrift.
      mServer.stop();
      mServerTNonblockingServerSocket.close();
      CommonUtils.sleepMs(null, 100);
    }
    mHeartbeatThread.join();
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length != 1) {
      LOG.info("Usage: java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar " +
          "tachyon.Worker <WorkerHost>");
      System.exit(-1);
    }
    WorkerConf wConf = WorkerConf.get();
    Worker worker = Worker.createWorker(wConf.MASTER_HOSTNAME + ":" + wConf.MASTER_PORT,
        args[0] + ":" + wConf.PORT, wConf.DATA_PORT,
        wConf.SELECTOR_THREADS, wConf.QUEUE_SIZE_PER_SELECTOR,
        wConf.SERVER_THREADS, wConf.DATA_FOLDER, wConf.MEMORY_SIZE);
    worker.start();
  }

  /**
   * Get the worker server handler class. This is for unit test only.
   * @return the WorkerServiceHandler
   */
  WorkerServiceHandler getWorkerServiceHandler() {
    return mWorkerServiceHandler;
  }
}