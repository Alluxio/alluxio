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
package tachyon.master;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import tachyon.Constants;
import tachyon.LeaderSelectorClient;
import tachyon.UnderFileSystem;
import tachyon.Version;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.thrift.MasterService;
import tachyon.util.CommonUtils;
import tachyon.web.UIWebServer;

/**
 * Entry point for the Master program.
 */
public class TachyonMaster {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.Master");
      System.exit(-1);
    }
    MasterConf mConf = MasterConf.get();
    TachyonMaster master =
        new TachyonMaster(new InetSocketAddress(mConf.HOSTNAME, mConf.PORT), mConf.WEB_PORT,
            mConf.SELECTOR_THREADS, mConf.QUEUE_SIZE_PER_SELECTOR, mConf.SERVER_THREADS);
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

  private LeaderSelectorClient mLeaderSelectorClient = null;

  public TachyonMaster(InetSocketAddress address, int webPort, int selectorThreads,
      int acceptQueueSizePerThreads, int workerThreads) {
    if (CommonConf.get().USE_ZOOKEEPER) {
      mZookeeperMode = true;
    }

    mIsStarted = false;
    mWebPort = webPort;
    mSelectorThreads = selectorThreads;
    mAcceptQueueSizePerThread = acceptQueueSizePerThreads;
    mWorkerThreads = workerThreads;

    try {
      mMasterAddress = address;
      String journalFolder = MasterConf.get().JOURNAL_FOLDER;
      if (!isFormatted(journalFolder, MasterConf.get().FORMAT_FILE_PREFIX)) {
        LOG.error("Tachyon was not formatted!");
        CommonUtils.runtimeException("Tachyon was not formatted!");
      }
      mJournal = new Journal(journalFolder, "image.data", "log.data");
      mMasterInfo = new MasterInfo(mMasterAddress, mJournal);

      if (mZookeeperMode) {
        CommonConf conf = CommonConf.get();
        mLeaderSelectorClient =
            new LeaderSelectorClient(conf.ZOOKEEPER_ADDRESS, conf.ZOOKEEPER_ELECTION_PATH,
                conf.ZOOKEEPER_LEADER_PATH, address.getHostName() + ":" + address.getPort());
        mEditLogProcessor = new EditLogProcessor(mJournal, journalFolder, mMasterInfo);
        Thread logProcessor = new Thread(mEditLogProcessor);
        logProcessor.start();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      CommonUtils.runtimeException(e);
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

  private boolean isFormatted(String folder, String path) throws IOException {
    if (!folder.endsWith(Constants.PATH_SEPARATOR)) {
      folder += Constants.PATH_SEPARATOR;
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

  private void setup() throws IOException, TTransportException {
    if (mZookeeperMode) {
      mEditLogProcessor.stop();
    }
    mMasterInfo.init();

    mWebServer =
        new UIWebServer("Tachyon Master Server", new InetSocketAddress(
            mMasterAddress.getHostName(), mWebPort), mMasterInfo);

    mMasterServiceHandler = new MasterServiceHandler(mMasterInfo);
    MasterService.Processor<MasterServiceHandler> masterServiceProcessor =
        new MasterService.Processor<MasterServiceHandler>(mMasterServiceHandler);

    mServerTNonblockingServerSocket = new TNonblockingServerSocket(mMasterAddress);
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
        CommonUtils.runtimeException(e);
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
              CommonUtils.runtimeException(e);
            } catch (TTransportException e) {
              LOG.error(e.getMessage(), e);
              CommonUtils.runtimeException(e);
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
        CommonUtils.runtimeException(e);
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        CommonUtils.runtimeException(e);
      }

      mWebServer.startWebServer();
      LOG.info("The master server started @ " + mMasterAddress);
      mMasterServiceServer.serve();
      LOG.info("The master server ended @ " + mMasterAddress);
    }
  }

  public void stop() throws Exception {
    if (mIsStarted) {
      mWebServer.shutdownWebServer();
      mMasterInfo.stop();
      mMasterServiceServer.stop();
      mServerTNonblockingServerSocket.close();
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