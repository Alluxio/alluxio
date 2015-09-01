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

package tachyon.master.next;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.LeaderSelectorClient;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.master.next.block.BlockMaster;
import tachyon.master.next.filesystem.FileSystemMaster;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.rawtable.RawTableMaster;
import tachyon.master.next.user.UserMaster;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
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


  private final TachyonConf mTachyonConf;
  private final boolean mUseZookeeper;
  private final int mMaxWorkerThreads;
  private final int mMinWorkerThreads;
  /** metadata port (RPC local port) */
  private final int mPort;
  private final TServerSocket mTServerSocket;
  private final InetSocketAddress mMasterAddress;

  // The masters
  private UserMaster mUserMaster;
  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private RawTableMaster mRawTableMaster;

  // The journals for the masters
  private final Journal mUserMasterJournal;
  private final Journal mBlockMasterJournal;
  private final Journal mFileSystemMasterJournal;
  private final Journal mRawTableMasterJournal;

  private UIWebServer mWebServer;
  private TServer mMasterServiceServer;
  private LeaderSelectorClient mLeaderSelectorClient = null;
  private boolean mIsServing = false;

  public TachyonMaster(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;

    mUseZookeeper = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER);
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
      mTServerSocket = new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC, mTachyonConf));
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master port
      mTachyonConf.set(Constants.MASTER_PORT, Integer.toString(mPort));
      mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf);

      // Check the journal directory
      String journalDirectory = mTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
      if (!journalDirectory.endsWith(TachyonURI.SEPARATOR)) {
        journalDirectory += TachyonURI.SEPARATOR;
      }
      Preconditions.checkState(isJournalFormatted(journalDirectory),
          "Tachyon was not formatted! The journal folder is " + journalDirectory);

      // Create the journals.
      mUserMasterJournal = new Journal(
          PathUtils.concatPath(journalDirectory, Constants.USER_MASTER_SERVICE_NAME), mTachyonConf);
      mBlockMasterJournal =
          new Journal(PathUtils.concatPath(journalDirectory, Constants.BLOCK_MASTER_SERVICE_NAME),
              mTachyonConf);
      mFileSystemMasterJournal = new Journal(
          PathUtils.concatPath(journalDirectory, Constants.FILE_SYSTEM_MASTER_SERVICE_NAME),
          mTachyonConf);
      mRawTableMasterJournal = new Journal(
          PathUtils.concatPath(journalDirectory, Constants.RAW_TABLE_MASTER_SERVICE_NAME),
          mTachyonConf);

      mUserMaster = new UserMaster(mUserMasterJournal);
      mBlockMaster = new BlockMaster(mBlockMasterJournal, mTachyonConf);
      mFileSystemMaster =
          new FileSystemMaster(mTachyonConf, mBlockMaster, mFileSystemMasterJournal);
      mRawTableMaster = new RawTableMaster(mTachyonConf, mFileSystemMaster, mRawTableMasterJournal);

      // TODO: implement metrics.

      if (mUseZookeeper) {
        // InetSocketAddress.toString causes test issues, so build the string by hand
        String zkName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mTachyonConf)
            + ":" + mMasterAddress.getPort();
        String zkAddress = mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS);
        String zkElectionPath = mTachyonConf.get(Constants.ZOOKEEPER_ELECTION_PATH);
        String zkLeaderPath = mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH);
        mLeaderSelectorClient =
            new LeaderSelectorClient(zkAddress, zkElectionPath, zkLeaderPath, zkName);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns the underlying {@link TachyonConf} instance for the Worker.
   *
   * @return TachyonConf of the Master
   */
  public TachyonConf getTachyonConf() {
    return mTachyonConf;
  }

  /**
   * Get the actual bind hostname on RPC service (used by unit test only).
   */
  public String getRPCBindHost() {
    return NetworkAddressUtils.getThriftSocket(mTServerSocket).getLocalSocketAddress().toString();
  }

  /**
   * Get the actual port that the RPC service is listening on (used by unit test only)
   */
  public int getRPCLocalPort() {
    return mPort;
  }

  /**
   * Get the actual bind hostname on web service (used by unit test only).
   */
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  /**
   * Get the actual port that the web service is listening on (used by unit test only)
   */
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  /**
   * Get whether the system is the leader in zookeeper mode, for unit test only.
   *
   * @return true if the system is the leader under zookeeper mode, false otherwise.
   */
  boolean isServing() {
    return mIsServing;
  }

  /**
   * Start a Tachyon master server.
   */
  public void start() throws Exception {
    if (mUseZookeeper) {
      try {
        mLeaderSelectorClient.start();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw Throwables.propagate(e);
      }

      Thread currentThread = Thread.currentThread();
      mLeaderSelectorClient.setCurrentMasterThread(currentThread);
      boolean started = false;
      while (true) {
        if (mLeaderSelectorClient.isLeader()) {
          if (started) {
            stopMaster();
          }
          startMaster(true);
          started = true;
          startServing();
        } else {
          // this is not the leader
          if (mIsServing || !started) {
            stopServing();
            stopMaster();

            // When transitioning from master to standby, recreate the masters with a readonly
            // journal.
            mUserMaster = new UserMaster(mUserMasterJournal.getReadOnlyJournal());
            mBlockMaster = new BlockMaster(mBlockMasterJournal.getReadOnlyJournal(), mTachyonConf);
            mFileSystemMaster = new FileSystemMaster(mTachyonConf, mBlockMaster,
                mFileSystemMasterJournal.getReadOnlyJournal());
            mRawTableMaster = new RawTableMaster(mTachyonConf, mFileSystemMaster,
                mRawTableMasterJournal.getReadOnlyJournal());
            startMaster(false);
            started = true;
          }
        }

        CommonUtils.sleepMs(LOG, 100);
      }
    } else {
      // not using zookeeper.
      startMaster(true);
      startServing();
    }
  }

  /*
   * Stop a Tachyon master server. Should only be called by tests.
   */
  public void stop() throws Exception {
    if (mIsServing) {
      LOG.info("Stopping Tachyon Master @ " + mMasterAddress);
      stopServing();
      stopMaster();
      mTServerSocket.close();
      mIsServing = false;
    }
    if (mUseZookeeper) {
      if (mLeaderSelectorClient != null) {
        mLeaderSelectorClient.close();
      }
    }
  }

  private void startMaster(boolean asMaster) {
    try {
      connectToUFS();

      mUserMaster.start(asMaster);
      mBlockMaster.start(asMaster);
      mFileSystemMaster.start(asMaster);
      mRawTableMaster.start(asMaster);

    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  private void stopMaster() {
    try {
      mUserMaster.stop();
      mBlockMaster.stop();
      mFileSystemMaster.stop();
      mRawTableMaster.stop();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  private void startServing() {
    // TODO: init MasterUIWebServer

    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    processor.registerProcessor(mUserMaster.getProcessorName(), mUserMaster.getProcessor());
    processor.registerProcessor(mBlockMaster.getProcessorName(), mBlockMaster.getProcessor());
    processor.registerProcessor(mFileSystemMaster.getProcessorName(),
        mFileSystemMaster.getProcessor());
    processor.registerProcessor(mRawTableMaster.getProcessorName(), mRawTableMaster.getProcessor());

    // create master thrift service with the multiplexed processor.
    mMasterServiceServer = new TThreadPoolServer(new TThreadPoolServer.Args(mTServerSocket)
        .maxWorkerThreads(mMaxWorkerThreads).minWorkerThreads(mMinWorkerThreads)
        .processor(processor).transportFactory(new TFramedTransport.Factory())
        .protocolFactory(new TBinaryProtocol.Factory(true, true)));

    mIsServing = true;

    // start web ui
    // mWebServer.startWebServer();

    String leaderStart = (mUseZookeeper) ? "(gained leadership)" : "";
    LOG.info("Tachyon Master version " + Version.VERSION + " started " + leaderStart + " @ "
        + mMasterAddress);

    // start thrift rpc server
    mMasterServiceServer.serve();

    String leaderStop = (mUseZookeeper) ? "(lost leadership)" : "";
    LOG.info("Tachyon Master version " + Version.VERSION + " ended " + leaderStop + " @ "
        + mMasterAddress);
  }

  private void stopServing() throws Exception {
    if (mMasterServiceServer != null) {
      mMasterServiceServer.stop();
    }
    if (mWebServer != null) {
      mWebServer.shutdownWebServer();
    }
    mIsServing = false;
  }


  /**
   * Checks to see if the journal directory is formatted.
   *
   * @param journalDirectory The journal directory to check
   * @return true if the journal directory was formatted previously, false otherwise
   * @throws IOException
   */
  private boolean isJournalFormatted(String journalDirectory) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(journalDirectory, mTachyonConf);
    if (!ufs.providesStorage()) {
      return true;
    }
    String[] files = ufs.list(journalDirectory);
    if (files == null) {
      return false;
    }
    // Search for the format file.
    String formatFilePrefix = mTachyonConf.get(Constants.MASTER_FORMAT_FILE_PREFIX);
    for (String file : files) {
      if (file.startsWith(formatFilePrefix)) {
        return true;
      }
    }
    return false;
  }

  private void connectToUFS() throws IOException {
    String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, mTachyonConf);
    ufs.connectFromMaster(mTachyonConf,
        NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, mTachyonConf));
  }
}
