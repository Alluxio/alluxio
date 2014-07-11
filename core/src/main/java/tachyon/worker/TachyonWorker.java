/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import tachyon.Constants;
import tachyon.Version;
import tachyon.conf.WorkerConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.Command;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;

/**
 * Entry point for a worker daemon.
 */
public class TachyonWorker implements Runnable {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new TachyonWorker
   * 
   * @param masterAddress
   *          The TachyonMaster's address
   * @param workerAddress
   *          This TachyonWorker's address
   * @param dataPort
   *          This TachyonWorker's data server's port
   * @param selectorThreads
   *          The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads
   *          The accept queue size per thread of the worker's thrift server
   * @param workerThreads
   *          The number of threads of the worker's thrift server
   * @param localFolder
   *          This TachyonWorker's local folder's path
   * @param spaceLimitBytes
   *          The maximum memory space this TachyonWorker can use, in bytes
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(InetSocketAddress masterAddress,
      InetSocketAddress workerAddress, int dataPort, int selectorThreads,
      int acceptQueueSizePerThreads, int workerThreads, String localFolder, long spaceLimitBytes) {
    return new TachyonWorker(masterAddress, workerAddress, dataPort, selectorThreads,
        acceptQueueSizePerThreads, workerThreads, localFolder, spaceLimitBytes);
  }

  /**
   * Create a new TachyonWorker
   * 
   * @param masterAddress
   *          The TachyonMaster's address. e.g., localhost:19998
   * @param workerAddress
   *          This TachyonWorker's address. e.g., localhost:29998
   * @param dataPort
   *          This TachyonWorker's data server's port
   * @param selectorThreads
   *          The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads
   *          The accept queue size per thread of the worker's thrift server
   * @param workerThreads
   *          The number of threads of the worker's thrift server
   * @param localFolder
   *          This TachyonWorker's local folder's path
   * @param spaceLimitBytes
   *          The maximum memory space this TachyonWorker can use, in bytes
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(String masterAddress,
      String workerAddress, int dataPort, int selectorThreads, int acceptQueueSizePerThreads,
      int workerThreads, String localFolder, long spaceLimitBytes) {
    String[] address = masterAddress.split(":");
    InetSocketAddress master = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    address = workerAddress.split(":");
    InetSocketAddress worker = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    return new TachyonWorker(master, worker, dataPort, selectorThreads, acceptQueueSizePerThreads,
        workerThreads, localFolder, spaceLimitBytes);
  }

  private static String getMasterLocation(String[] args) {
    WorkerConf wConf = WorkerConf.get();
    String confFileMasterLoc = wConf.MASTER_HOSTNAME + ":" + wConf.MASTER_PORT;
    String masterLocation;
    if (args.length < 2) {
      masterLocation = confFileMasterLoc;
    } else {
      masterLocation = args[1];
      if (masterLocation.indexOf(":") == -1) {
        masterLocation += ":" + wConf.MASTER_PORT;
      }
      if (!masterLocation.equals(confFileMasterLoc)) {
        LOG.warn("Master Address in configuration file(" + confFileMasterLoc + ") is different "
            + "from the command line one(" + masterLocation + ").");
      }
    }
    return masterLocation;
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length < 1 || args.length > 2) {
      LOG.info("Usage: java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.Worker <WorkerHost> [<MasterHost:Port>]");
      System.exit(-1);
    }

    WorkerConf wConf = WorkerConf.get();

    String resolvedWorkerHost;
    try {
      resolvedWorkerHost = NetworkUtils.resolveHostName(args[0]);
    } catch (UnknownHostException e) {
      resolvedWorkerHost = args[0];
    }

    TachyonWorker worker =
        TachyonWorker.createWorker(getMasterLocation(args), resolvedWorkerHost + ":" + wConf.PORT,
            wConf.DATA_PORT, wConf.SELECTOR_THREADS, wConf.QUEUE_SIZE_PER_SELECTOR,
            wConf.SERVER_THREADS, wConf.DATA_FOLDER, wConf.MEMORY_SIZE);
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception terminating worker", e);
      throw new RuntimeException(e);
    }
  }

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

  /**
   * @param masterAddress
   *          The TachyonMaster's address.
   * @param workerAddress
   *          This TachyonWorker's address.
   * @param dataPort
   *          This TachyonWorker's data server's port
   * @param selectorThreads
   *          The number of selector threads of the worker's thrift server
   * @param acceptQueueSizePerThreads
   *          The accept queue size per thread of the worker's thrift server
   * @param workerThreads
   *          The number of threads of the worker's thrift server
   * @param dataFolder
   *          This TachyonWorker's local folder's path
   * @param memoryCapacityBytes
   *          The maximum memory space this TachyonWorker can use, in bytes
   */
  private TachyonWorker(InetSocketAddress masterAddress, InetSocketAddress workerAddress,
      int dataPort, int selectorThreads, int acceptQueueSizePerThreads, int workerThreads,
      String dataFolder, long memoryCapacityBytes) {
    MasterAddress = masterAddress;
    WorkerAddress = workerAddress;

    mWorkerStorage =
        new WorkerStorage(MasterAddress, WorkerAddress, dataFolder, memoryCapacityBytes);

    mWorkerServiceHandler = new WorkerServiceHandler(mWorkerStorage);

    mDataServer =
        new DataServer(new InetSocketAddress(workerAddress.getHostName(), dataPort),
            mWorkerStorage);
    mDataServerThread = new Thread(mDataServer);

    mHeartbeatThread = new Thread(this);
    try {
      LOG.info("The worker server tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      mServerTNonblockingServerSocket = new TNonblockingServerSocket(workerAddress);
      mServer =
          new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
              mServerTNonblockingServerSocket).processor(processor)
              .selectorThreads(selectorThreads)
              .acceptQueueSizePerThread(acceptQueueSizePerThreads).workerThreads(workerThreads));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
    }
  }

  /**
   * Get the worker server handler class. This is for unit test only.
   * 
   * @return the WorkerServiceHandler
   */
  WorkerServiceHandler getWorkerServiceHandler() {
    return mWorkerServiceHandler;
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
        try {
          mWorkerStorage.resetMasterClient();
        } catch (TException e2) {
          LOG.error("Received exception while attempting to reset client", e2);
        }
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        cmd = null;
        if (System.currentTimeMillis() - lastHeartbeatMs >= WorkerConf.get().HEARTBEAT_TIMEOUT_MS) {
          CommonUtils.runtimeException("Timebeat timeout "
              + (System.currentTimeMillis() - lastHeartbeatMs) + "ms");
        }
      }

      if (cmd != null) {
        switch (cmd.mCommandType) {
        case Unknown:
          LOG.error("Unknown command: " + cmd);
          break;
        case Nothing:
          LOG.debug("Nothing command: " + cmd);
          break;
        case Register:
          LOG.info("Register command: " + cmd);
          mWorkerStorage.register();
          break;
        case Free:
          mWorkerStorage.freeBlocks(cmd.mData);
          LOG.info("Free command: " + cmd);
          break;
        case Delete:
          LOG.info("Delete command: " + cmd);
          break;
        default:
          CommonUtils.runtimeException("Un-recognized command from master " + cmd.toString());
        }
      }

      mWorkerStorage.checkStatus();
    }
  }

  /**
   * Start the data server thread and heartbeat thread of this TachyonWorker.
   */
  public void start() {
    mDataServerThread.start();
    mHeartbeatThread.start();

    LOG.info("The worker server started @ " + WorkerAddress);
    mServer.serve();
    LOG.info("The worker server ends @ " + WorkerAddress);
  }

  /**
   * Stop this TachyonWorker. Stop all the threads belong to this TachyonWorker.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
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
}
