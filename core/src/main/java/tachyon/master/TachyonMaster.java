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

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.LeaderSelectorClient;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemHdfs;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.thrift.MasterService;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
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

    TachyonConf tachyonConf = new TachyonConf();
    TachyonMaster master = new TachyonMaster(tachyonConf);
    master.start();
  }

  private boolean mIsStarted;
  private MasterInfo mMasterInfo;
  private InetSocketAddress mMasterAddress;
  private UIWebServer mWebServer;
  private TNonblockingServerSocket mServerTNonblockingServerSocket;
  private TServer mMasterServiceServer;
  private MasterServiceHandler mMasterServiceHandler;
  private Journal mJournal;
  private EditLogProcessor mEditLogProcessor;
  private int mWebPort;

  private int mSelectorThreads;
  private int mAcceptQueueSizePerThread;
  private int mWorkerThreads;
  private boolean mZookeeperMode = false;
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2,
      ThreadFactoryUtils.daemon("heartbeat-master-%d"));

  private LeaderSelectorClient mLeaderSelectorClient = null;

  /** metadata port */
  private final int mPort;

  private final TachyonConf mTachyonConf;

  public TachyonMaster(TachyonConf tachyonConf) {

    String hostName = tachyonConf.get(Constants.MASTER_HOSTNAME, "localhost");
    int port = tachyonConf.getInt(Constants.MASTER_PORT, 0);
    InetSocketAddress address = new InetSocketAddress(hostName, port);
    int webPort = tachyonConf.getInt(Constants.MASTER_WEB_PORT, 0);
    int selectorThreads = tachyonConf.getInt(Constants.MASTER_SELECTOR_THREADS, 3);
    int acceptQueueSizePerThreads = tachyonConf.getInt(Constants.MASTER_QUEUE_SIZE_PER_SELECTOR,
        3000);
    int workerThreads = tachyonConf.getInt(Constants.MASTER_SERVER_THREADS, 2 * Runtime.getRuntime()
        .availableProcessors());

    TachyonConf.assertValidPort(address, tachyonConf);
    TachyonConf.assertValidPort(webPort, tachyonConf);

    mTachyonConf = tachyonConf;
    mZookeeperMode = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false);

    mIsStarted = false;
    mWebPort = webPort;
    mSelectorThreads = selectorThreads;
    mAcceptQueueSizePerThread = acceptQueueSizePerThreads;
    mWorkerThreads = workerThreads;

    try {
      // Extract the port from the generated socket.
      // When running tests, its great to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      mServerTNonblockingServerSocket = new TNonblockingServerSocket(address);
      mPort = NetworkUtils.getPort(mServerTNonblockingServerSocket);

      mMasterAddress = new InetSocketAddress(NetworkUtils.getFqdnHost(address), mPort);
      String journalFolder = mTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER, "/journal/");
      String formatFilePrefix = mTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX,
          Constants.FORMAT_FILE_PREFIX);
      Preconditions.checkState(isFormatted(journalFolder, formatFilePrefix),
          "Tachyon was not formatted! The journal folder is " + journalFolder);
      mJournal = new Journal(journalFolder, "image.data", "log.data");
      mMasterInfo = new MasterInfo(mMasterAddress, mJournal, mExecutorService, mTachyonConf);

      if (mZookeeperMode) {
        // InetSocketAddress.toString causes test issues, so build the string by hand
        String zkName = NetworkUtils.getFqdnHost(mMasterAddress) + ":" + mMasterAddress.getPort();
        String zkAddress = mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS, null);
        String zkElectionPath = mTachyonConf.get(Constants.ZOOKEEPER_ELECTION_PATH, "/election");
        String zkLeaderPath = mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH, "/leader");
        mLeaderSelectorClient =
            new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
        mEditLogProcessor = new EditLogProcessor(mJournal, journalFolder, mMasterInfo);
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
   * Get the port used by unit test only
   */
  int getMetaPort() {
    return mPort;
  }

  private boolean isFormatted(String folder, String path) throws IOException {
    if (!folder.endsWith(TachyonURI.SEPARATOR)) {
      folder += TachyonURI.SEPARATOR;
    }
    UnderFileSystem ufs = UnderFileSystem.get(folder);
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
   * Get wehether the system is the leader under zookeeper mode, for unit test only.
   * 
   * @return true if the system is the leader under zookeeper mode, false otherwise.
   */
  boolean isStarted() {
    return mIsStarted;
  }

  /**
   * Get whether the system is for zookeeper mode, for unit test only.
   * 
   * @return true if the master is under zookeeper mode, false otherwise.
   */
  boolean isZookeeperMode() {
    return mZookeeperMode;
  }

  private void login() throws IOException {
    String masterKeytab = mTachyonConf.get(Constants.MASTER_KEYTAB_KEY, null);
    String masterPrincipal = mTachyonConf.get(Constants.MASTER_PRINCIPAL_KEY, null);
    if (masterKeytab == null || masterPrincipal == null) {
      return;
    }
    UnderFileSystem ufs = UnderFileSystem.get(mTachyonConf.get(Constants.UNDERFS_ADDRESS, null));
    if (ufs instanceof UnderFileSystemHdfs) {
      ((UnderFileSystemHdfs) ufs).login(masterKeytab, masterKeytab, masterPrincipal,
          masterPrincipal, NetworkUtils.getFqdnHost(mMasterAddress));
    }
  }

  private void setup() throws IOException, TTransportException {
    login();
    if (mZookeeperMode) {
      mEditLogProcessor.stop();
    }
    mMasterInfo.init();

    mWebServer =
        new UIWebServer("Tachyon Master Server", new InetSocketAddress(
            NetworkUtils.getFqdnHost(mMasterAddress), mWebPort), mMasterInfo);

    mMasterServiceHandler = new MasterServiceHandler(mMasterInfo);
    MasterService.Processor<MasterServiceHandler> masterServiceProcessor =
        new MasterService.Processor<MasterServiceHandler>(mMasterServiceHandler);

    mMasterServiceServer =
        new TThreadedSelectorServer(new TThreadedSelectorServer.Args(
            mServerTNonblockingServerSocket).processor(masterServiceProcessor)
            .selectorThreads(mSelectorThreads).acceptQueueSizePerThread(mAcceptQueueSizePerThread)
            .workerThreads(mWorkerThreads));

    mIsStarted = true;
  }

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

      mWebServer.startWebServer();
      LOG.info("Tachyon Master version " + Version.VERSION + " started @ " + mMasterAddress);
      mMasterServiceServer.serve();
      LOG.info("Tachyon Master version " + Version.VERSION + " ended @ " + mMasterAddress);
    }
  }

  public void stop() throws Exception {
    if (mIsStarted) {
      mWebServer.shutdownWebServer();
      mMasterInfo.stop();
      mMasterServiceServer.stop();
      mServerTNonblockingServerSocket.close();
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
