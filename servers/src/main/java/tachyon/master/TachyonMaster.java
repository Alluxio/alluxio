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

package tachyon.master;

import java.io.IOException;
import java.net.InetSocketAddress;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.LeaderSelectorClient;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.metrics.MetricsSystem;
import tachyon.thrift.MasterService;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.util.ThreadFactoryUtils;
import tachyon.web.MasterUIWebServer;
import tachyon.web.UIWebServer;

/**
 * Entry point for the Master program.
 */
public class TachyonMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.Master");
      System.exit(-1);
    }

    try {
      TachyonMaster master = new TachyonMaster(new TachyonConf());
      master.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception terminating Master", e);
      System.exit(-1);
    }
  }

  private boolean mIsStarted;
  private MasterInfo mMasterInfo;
  private InetSocketAddress mMasterAddress;
  private UIWebServer mWebServer;
  private TServerSocket mServerTServerSocket;
  private TServer mMasterServiceServer;
  private MasterServiceHandler mMasterServiceHandler;
  private MetricsSystem mMasterMetricsSystem;
  private Journal mJournal;
  private EditLogProcessor mEditLogProcessor;

  private int mMaxWorkerThreads;
  private int mMinWorkerThreads;
  private boolean mZookeeperMode = false;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2,
      ThreadFactoryUtils.build("heartbeat-master-%d", true));

  private LeaderSelectorClient mLeaderSelectorClient = null;

  /** metadata port (RPC local port) */
  private final int mPort;

  private final TachyonConf mTachyonConf;

  public TachyonMaster(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mZookeeperMode = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER);

    mIsStarted = false;
    mMinWorkerThreads = mTachyonConf.getInt(Constants.MASTER_MIN_WORKER_THREADS);
    mMaxWorkerThreads = mTachyonConf.getInt(Constants.MASTER_MAX_WORKER_THREADS);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        Constants.MASTER_MAX_WORKER_THREADS + " can not be less than "
            + Constants.MASTER_MIN_WORKER_THREADS);

    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      mServerTServerSocket =
          new TServerSocket(
              NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC, mTachyonConf));
      mPort = NetworkAddressUtils.getThriftPort(mServerTServerSocket);
      // reset master port
      mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mPort));

      String journalFolder =
          mTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
      String formatFilePrefix =
          mTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX);
      UnderFileSystem ufs = UnderFileSystem.get(journalFolder, mTachyonConf);
      if (ufs.providesStorage()) {
        Preconditions.checkState(isFormatted(ufs, journalFolder, formatFilePrefix),
            "Tachyon was not formatted! The journal folder is " + journalFolder);
      }
      mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf);
      mJournal = new Journal(journalFolder, "image.data", "log.data", mTachyonConf);
      mMasterInfo = new MasterInfo(mMasterAddress, mJournal, mExecutorService, mTachyonConf);

      mMasterMetricsSystem = new MetricsSystem("master", mTachyonConf);

      if (mZookeeperMode) {
        // InetSocketAddress.toString causes test issues, so build the string by hand
        String zkName =
            NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mTachyonConf) + ":"
                + mMasterAddress.getPort();
        String zkAddress = mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS);
        String zkElectionPath = mTachyonConf.get(Constants.ZOOKEEPER_ELECTION_PATH);
        String zkLeaderPath = mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH);
        mLeaderSelectorClient =
            new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
        mEditLogProcessor =
            new EditLogProcessor(mJournal, journalFolder, mMasterInfo, mTachyonConf);
        // TODO move this to executor service when the shared thread patch goes in
        Thread logProcessor = new Thread(mEditLogProcessor);
        logProcessor.start();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get MasterInfo instance for Unit Test
   *
   * @return MasterInfo of the Master
   */
  MasterInfo getMasterInfo() {
    return mMasterInfo;
  }

  /**
   * Gets the underlying {@link tachyon.conf.TachyonConf} instance for the Worker.
   *
   * @return TachyonConf of the Master
   */
  public TachyonConf getTachyonConf() {
    return mTachyonConf;
  }

  /**
   * Get the actual bind hostname on RPC service (used by unit test only).
   *
   * @return RPC bind hostname
   */
  public String getRPCBindHost() {
    return NetworkAddressUtils.getThriftSocket(mServerTServerSocket).getLocalSocketAddress()
        .toString();
  }

  /**
   * Get the actual port that the RPC service is listening on (used by unit test only)
   *
   * @return RPC local port
   */
  public int getRPCLocalPort() {
    return mPort;
  }

  /**
   * Get the actual bind hostname on web service (used by unit test only).
   *
   * @return Web bind hostname
   */
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  /**
   * Get the actual port that the web service is listening on (used by unit test only)
   *
   * @return Web local port
   */
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  private boolean isFormatted(UnderFileSystem ufs, String folder, String path) throws IOException {
    if (!folder.endsWith(TachyonURI.SEPARATOR)) {
      folder += TachyonURI.SEPARATOR;
    }
    String[] files = ufs.list(folder);
    if (files == null) {
      return false;
    }
    for (String file : files) {
      if (file.startsWith(path)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get whether the system is the leader in zookeeper mode, for unit test only.
   *
   * @return true if the system is the leader under zookeeper mode, false otherwise.
   */
  boolean isStarted() {
    return mIsStarted;
  }

  /**
   * Get whether the system is in zookeeper mode, for unit test only.
   *
   * @return true if the master is under zookeeper mode, false otherwise.
   */
  boolean isZookeeperMode() {
    return mZookeeperMode;
  }

  private void connectToUFS() throws IOException {
    String ufsAddress =
        mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, mTachyonConf);
    ufs.connectFromMaster(mTachyonConf,
        NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mTachyonConf));
  }

  private void setup() throws IOException, TTransportException {
    connectToUFS();
    if (mZookeeperMode) {
      mEditLogProcessor.stop();
    }
    mMasterInfo.init();

    mWebServer =
        new MasterUIWebServer(ServiceType.MASTER_WEB, NetworkAddressUtils.getBindAddress(
            ServiceType.MASTER_WEB, mTachyonConf), mMasterInfo, mTachyonConf);

    mMasterServiceHandler = new MasterServiceHandler(mMasterInfo);
    MasterService.Processor<MasterServiceHandler> masterServiceProcessor =
        new MasterService.Processor<MasterServiceHandler>(mMasterServiceHandler);

    mMasterServiceServer =
        new TThreadPoolServer(new TThreadPoolServer.Args(mServerTServerSocket)
            .maxWorkerThreads(mMaxWorkerThreads).minWorkerThreads(mMinWorkerThreads)
            .processor(masterServiceProcessor).transportFactory(new TFramedTransport.Factory())
            .protocolFactory(new TBinaryProtocol.Factory(true, true)));

    mIsStarted = true;
  }

  /**
   * Start a Tachyon master server.
   */
  public void start() {
    if (mZookeeperMode) {
      try {
        mLeaderSelectorClient.start();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw Throwables.propagate(e);
      }

      Thread currentThread = Thread.currentThread();
      mLeaderSelectorClient.setCurrentMasterThread(currentThread);
      boolean running = false;
      while (true) {
        if (mLeaderSelectorClient.isLeader()) {
          if (!running) {
            running = true;
            try {
              setup();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
              throw Throwables.propagate(e);
            } catch (TTransportException e) {
              LOG.error(e.getMessage(), e);
              throw Throwables.propagate(e);
            }
            mMasterMetricsSystem.registerSource(mMasterInfo.getMasterSource());
            mMasterMetricsSystem.start();
            mWebServer.addHandler(mMasterMetricsSystem.getServletHandler());
            mWebServer.startWebServer();
            LOG.info("The master (leader) server started @ " + mMasterAddress);
            mMasterServiceServer.serve();
            LOG.info("The master (previous leader) server ended @ " + mMasterAddress);
            mJournal.close();
          }
        } else {
          if (running) {
            mMasterServiceServer.stop();
            running = false;
          }
        }

        CommonUtils.sleepMs(LOG, 100);
      }
    } else {
      try {
        setup();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw Throwables.propagate(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        throw Throwables.propagate(e);
      }

      mMasterMetricsSystem.registerSource(mMasterInfo.getMasterSource());
      mMasterMetricsSystem.start();
      mWebServer.addHandler(mMasterMetricsSystem.getServletHandler());
      mWebServer.startWebServer();
      LOG.info("Tachyon Master version " + Version.VERSION + " started @ " + mMasterAddress);
      mMasterServiceServer.serve();
      LOG.info("Tachyon Master version " + Version.VERSION + " ended @ " + mMasterAddress);
    }
  }

  /*
   * Stop a Tachyon master server.
   */
  public void stop() throws Exception {
    if (mIsStarted) {
      mWebServer.shutdownWebServer();
      mMasterInfo.stop();
      mJournal.close();
      mMasterMetricsSystem.stop();
      mMasterServiceServer.stop();
      mServerTServerSocket.close();
      mExecutorService.shutdown();
      mIsStarted = false;
    }
    if (mZookeeperMode) {
      if (mLeaderSelectorClient != null) {
        mLeaderSelectorClient.close();
      }
      if (mEditLogProcessor != null) {
        mEditLogProcessor.stop();
      }
    }
  }
}
