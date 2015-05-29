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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemHdfs;
import tachyon.Users;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerService;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;

/**
 * Entry point for a worker daemon.
 */
public class TachyonWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Create a new TachyonWorker
   *
   * @param masterAddress The TachyonMaster's address
   * @param workerAddress This TachyonWorker's address
   * @param dataPort This TachyonWorker's data server's port
   * @param minWorkerThreads The min number of worker threads used in TThreadPoolServer
   * @param maxWorkerThreads The max number of worker threads used in TThreadPoolServer
   * @param tachyonConf The instance of {@link tachyon.conf.TachyonConf} to used by Worker.
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(InetSocketAddress masterAddress,
      InetSocketAddress workerAddress, int dataPort, int minWorkerThreads, int maxWorkerThreads,
      TachyonConf tachyonConf) {
    return new TachyonWorker(masterAddress, workerAddress, dataPort, minWorkerThreads,
        maxWorkerThreads, tachyonConf);
  }

  /**
   * Create a new TachyonWorker
   * 
   * @param masterAddress The TachyonMaster's address. e.g., localhost:19998
   * @param workerAddress This TachyonWorker's address. e.g., localhost:29998
   * @param dataPort This TachyonWorker's data server's port
   * @param minWorkerThreads The min number of worker threads used in TThreadPoolServer
   * @param maxWorkerThreads The max number of worker threads used in TThreadPoolServer
   * @param tachyonConf The instance of {@link tachyon.conf.TachyonConf} to used by Worker.
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(String masterAddress, String workerAddress,
      int dataPort, int minWorkerThreads, int maxWorkerThreads, TachyonConf tachyonConf) {
    String[] address = masterAddress.split(":");
    InetSocketAddress master = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    address = workerAddress.split(":");
    InetSocketAddress worker = new InetSocketAddress(address[0], Integer.parseInt(address[1]));
    return new TachyonWorker(master, worker, dataPort, minWorkerThreads, maxWorkerThreads,
        tachyonConf);
  }

  /**
   * Create a new TachyonWorker
   * 
   * @param tachyonConf The instance of {@link tachyon.conf.TachyonConf} to used by Worker.
   * @return The new TachyonWorker
   */
  public static synchronized TachyonWorker createWorker(TachyonConf tachyonConf) {
    String masterHostname =
        tachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName());
    int masterPort = tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    String workerHostName = NetworkUtils.getLocalHostName();
    int workerPort = tachyonConf.getInt(Constants.WORKER_PORT, Constants.DEFAULT_WORKER_PORT);
    int dataPort =
        tachyonConf.getInt(Constants.WORKER_DATA_PORT, Constants.DEFAULT_WORKER_DATA_SERVER_PORT);
    int minWorkerThreads =
        tachyonConf.getInt(Constants.WORKER_MIN_WORKER_THREADS, Runtime.getRuntime()
            .availableProcessors());

    int maxWorkerThreads =
        tachyonConf.getInt(Constants.WORKER_MAX_WORKER_THREADS,
            Constants.DEFAULT_WORKER_MAX_WORKER_THREADS);

    return new TachyonWorker(new InetSocketAddress(masterHostname, masterPort),
        new InetSocketAddress(workerHostName, workerPort), dataPort, minWorkerThreads,
        maxWorkerThreads, tachyonConf);

  }

  private static String getMasterLocation(String[] args, TachyonConf conf) {
    String masterHostname = conf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName());
    int masterPort = conf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
    String confFileMasterLoc = masterHostname + ":" + masterPort;
    String masterLocation;
    if (args.length < 1) {
      masterLocation = confFileMasterLoc;
    } else {
      masterLocation = args[0];
      if (masterLocation.indexOf(":") == -1) {
        masterLocation += ":" + masterPort;
      }
      if (!masterLocation.equals(confFileMasterLoc)) {
        LOG.warn("Master Address in configuration file(" + confFileMasterLoc + ") is different "
            + "from the command line one(" + masterLocation + ").");
      }
    }
    return masterLocation;
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length > 1) {
      LOG.info("Usage: java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.Worker [<MasterHost:Port>]");
      System.exit(-1);
    }

    String resolvedWorkerHost = NetworkUtils.getLocalHostName();
    LOG.info("Resolved local TachyonWorker host to " + resolvedWorkerHost);

    TachyonConf tachyonConf = new TachyonConf();
    TachyonWorker worker = TachyonWorker.createWorker(tachyonConf);
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception terminating worker", e);
      System.exit(-1);
    }
  }

  private final InetSocketAddress mMasterAddress;
  private final NetAddress mWorkerAddress;
  private TServer mServer;

  private TServerSocket mServerTServerSocket;
  private final WorkerStorage mWorkerStorage;

  private final WorkerServiceHandler mWorkerServiceHandler;

  private final DataServer mDataServer;

  private final Thread mHeartbeatThread;

  private volatile boolean mStop = false;

  private final int mPort;
  private final int mDataPort;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(1,
      ThreadFactoryUtils.daemon("heartbeat-worker-%d"));
  private final TachyonConf mTachyonConf;

  /**
   * @param masterAddress The TachyonMaster's address.
   * @param workerAddress This TachyonWorker's address.
   * @param dataPort This TachyonWorker's data server's port
   * @param minWorkerThreads The min number of worker threads used in TThreadPoolServer
   * @param maxWorkerThreads The max number of worker threads used in TThreadPoolServer
   * @param tachyonConf The {@link TachyonConf} instance for configuration properties
   */
  private TachyonWorker(InetSocketAddress masterAddress, InetSocketAddress workerAddress,
      int dataPort, int minWorkerThreads, int maxWorkerThreads, TachyonConf tachyonConf) {
    TachyonConf.assertValidPort(masterAddress, tachyonConf);
    TachyonConf.assertValidPort(workerAddress, tachyonConf);
    TachyonConf.assertValidPort(dataPort, tachyonConf);

    mTachyonConf = tachyonConf;

    mMasterAddress = masterAddress;

    mWorkerStorage = new WorkerStorage(mMasterAddress, mExecutorService, mTachyonConf);

    mWorkerServiceHandler = new WorkerServiceHandler(mWorkerStorage);

    // Extract the port from the generated socket.
    // When running tests, its great to use port '0' so the system will figure out what port to use
    // (any random free port).
    // In a production or any real deployment setup, port '0' should not be used as it will make
    // deployment more complicated.
    InetSocketAddress dataAddress = new InetSocketAddress(workerAddress.getHostName(), dataPort);
    BlocksLocker blockLocker = new BlocksLocker(mWorkerStorage, Users.DATASERVER_USER_ID);
    mDataServer = DataServer.Factory.createDataServer(dataAddress, blockLocker, mTachyonConf);
    mDataPort = mDataServer.getPort();

    mHeartbeatThread = new Thread(this);
    try {
      LOG.info("Tachyon Worker version " + Version.VERSION + " tries to start @ " + workerAddress);
      WorkerService.Processor<WorkerServiceHandler> processor =
          new WorkerService.Processor<WorkerServiceHandler>(mWorkerServiceHandler);

      mServerTServerSocket = new TServerSocket(workerAddress);
      mPort = NetworkUtils.getPort(mServerTServerSocket);

      // TODO: auth worker server???
      mServer =
          new TThreadPoolServer(new TThreadPoolServer.Args(mServerTServerSocket)
              .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads)
              .processor(processor).transportFactory(new TFramedTransport.Factory())
              .protocolFactory(new TBinaryProtocol.Factory(true, true)));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
    mWorkerAddress =
        new NetAddress(workerAddress.getAddress().getCanonicalHostName(), mPort, mDataPort);
    mWorkerStorage.initialize(mWorkerAddress);
  }

  /**
   * Gets the data port of the worker. For unit tests only.
   */
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * Gets the metadata port of the worker. For unit tests only.
   */
  public int getMetaPort() {
    return mPort;
  }

  /**
   * Gets the underlying {@link tachyon.conf.TachyonConf} instance for the Worker.
   */
  public TachyonConf getTachyonConf() {
    return mTachyonConf;
  }

  /**
   * Get the worker server handler class. This is for unit test only.
   * 
   * @return the WorkerServiceHandler
   */
  WorkerServiceHandler getWorkerServiceHandler() {
    return mWorkerServiceHandler;
  }

  private void login() throws IOException {
    String workerKeytabFile = mTachyonConf.get(Constants.WORKER_KEYTAB_KEY, null);
    String workerPrincipal = mTachyonConf.get(Constants.WORKER_PRINCIPAL_KEY, null);
    if (workerKeytabFile == null || workerPrincipal == null) {
      return;
    }
    String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS, "localhost/underfs");
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, mTachyonConf);
    if (ufs instanceof UnderFileSystemHdfs) {
      ((UnderFileSystemHdfs) ufs)
          .login(Constants.WORKER_KEYTAB_KEY, workerKeytabFile, Constants.WORKER_PRINCIPAL_KEY,
              workerPrincipal, NetworkUtils.getFqdnHost(mWorkerAddress));
    }
  }

  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    Command cmd = null;
    while (!mStop) {
      long diff = System.currentTimeMillis() - lastHeartbeatMs;
      int hbIntervalMs =
          mTachyonConf
              .getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
      if (diff < hbIntervalMs) {
        LOG.debug("Heartbeat process takes {} ms.", diff);
        CommonUtils.sleepMs(LOG, hbIntervalMs - diff);
      } else {
        LOG.error("Heartbeat process takes " + diff + " ms.");
      }

      try {
        cmd = mWorkerStorage.heartbeat();

        lastHeartbeatMs = System.currentTimeMillis();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        mWorkerStorage.resetMasterClient();
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        cmd = null;
        int heartbeatTimeout =
            mTachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS, 10 * Constants.SECOND_MS);
        if (System.currentTimeMillis() - lastHeartbeatMs >= heartbeatTimeout) {
          throw new RuntimeException("Heartbeat timeout "
              + (System.currentTimeMillis() - lastHeartbeatMs) + "ms");
        }
      }

      if (cmd != null) {
        switch (cmd.mCommandType) {
          case Unknown:
            LOG.error("Unknown command: " + cmd);
            break;
          case Nothing:
            LOG.debug("Nothing command: {}", cmd);
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
            throw new RuntimeException("Un-recognized command from master " + cmd.toString());
        }
      }

      mWorkerStorage.checkStatus();
    }
  }

  /**
   * Start the data server thread and heartbeat thread of this TachyonWorker.
   */
  public void start() throws IOException {
    login();

    mHeartbeatThread.start();

    LOG.info("The worker server started @ " + mWorkerAddress);
    mServer.serve();
    LOG.info("The worker server ends @ " + mWorkerAddress);
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
    mServerTServerSocket.close();
    mExecutorService.shutdown();
    while (!mDataServer.isClosed() || mServer.isServing() || mHeartbeatThread.isAlive()) {
      // TODO The reason to stop and close again is due to some issues in Thrift.
      mServer.stop();
      mServerTServerSocket.close();
      CommonUtils.sleepMs(null, 100);
    }
    mHeartbeatThread.join();
  }
}
