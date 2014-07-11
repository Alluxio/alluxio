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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemCluster;
import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;
import tachyon.worker.TachyonWorker;

/**
 * A local Tachyon cluster with Multiple masters
 */
public class LocalTachyonClusterMultiMaster {
  public class MasterThread extends Thread {
    private TachyonMaster mMaster = null;

    public MasterThread(TachyonMaster master) {
      mMaster = master;
    }

    @Override
    public void run() {
      try {
        mMaster.start();
      } catch (Exception e) {
        CommonUtils.runtimeException(e + " \n Start Master Error \n" + e.getMessage());
      }
    }

    public void shutdown() {
      try {
        mMaster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(100);
    cluster.start();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);

    cluster = new LocalTachyonCluster(100);
    cluster.start();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
  }

  private TestingServer mCuratorServer = null;
  private int mNumOfMasters = 0;

  private List<TachyonMaster> mMasters = null;
  private TachyonWorker mWorker = null;
  private List<Integer> mMastersPorts;

  private int mWorkerPort;
  private long mWorkerCapacityBytes;

  private String mTachyonHome;
  private String mWorkerDataFolder;

  private List<MasterThread> mMasterThreads = null;
  private Thread mWorkerThread = null;

  private String mLocalhostName = null;

  private UnderFileSystemCluster mUnderFSCluster = null;

  private List<TachyonFS> mClients = new ArrayList<TachyonFS>();

  public LocalTachyonClusterMultiMaster(int masterPort, int workerPort, long workerCapacityBytes,
      int masters) {
    mNumOfMasters = masters;
    mMastersPorts = new ArrayList<Integer>(masters);
    for (int k = 0; k < mNumOfMasters; k ++) {
      mMastersPorts.add(masterPort + k * 10);
    }
    mWorkerPort = workerPort;
    mWorkerCapacityBytes = workerCapacityBytes;

    try {
      mCuratorServer = new TestingServer();
    } catch (Exception e) {
      CommonUtils.runtimeException(e);
    }
  }

  public LocalTachyonClusterMultiMaster(long workerCapacityBytes, int masters) {
    this(Constants.DEFAULT_MASTER_PORT - 1000, Constants.DEFAULT_WORKER_PORT - 1000,
        workerCapacityBytes, masters);
  }

  public synchronized TachyonFS getClient() throws IOException {
    mClients.add(TachyonFS.get(Constants.HEADER_FT + mCuratorServer.getConnectString()));
    return mClients.get(mClients.size() - 1);
  }

  public synchronized List<TachyonFS> getClients() {
    return mClients;
  }

  String getEditLogPath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/log.data";
  }

  String getImagePath() {
    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/image.data";
  }

  MasterInfo getMasterInfo(int masterIndex) {
    return mMasters.get(masterIndex).getMasterInfo();
  }

  public List<Integer> getMastersPorts() {
    return mMastersPorts;
  }

  public String getTachyonHome() {
    return mTachyonHome;
  }

  public String getTempFolderInUnderFs() {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  public int getWorkerPort() {
    return mWorkerPort;
  }

  public boolean killLeader() {
    for (int k = 0; k < mNumOfMasters; k ++) {
      if (mMasters.get(k).isStarted()) {
        try {
          mMasters.get(k).stop();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  private void deleteDir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  private void mkdir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (!ufs.mkdirs(path, true)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public void start() throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    mWorkerDataFolder = mTachyonHome + "/ramdisk";
    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";

    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);
    mkdir(masterDataFolder);
    mkdir(masterLogFolder);

    mLocalhostName = InetAddress.getLocalHost().getCanonicalHostName();

    mUnderFSCluster = UnderFileSystemCluster.get(mTachyonHome + "/dfs");
    String underfsFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/tachyon_underfs_folder";
    String masterJournalFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/journal";

    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.underfs.address", underfsFolder);
    System.setProperty("tachyon.master.journal.folder", masterJournalFolder
        + Constants.PATH_SEPARATOR);
    System.setProperty("tachyon.usezookeeper", "true");
    System.setProperty("tachyon.zookeeper.address", mCuratorServer.getConnectString());
    System.setProperty("tachyon.zookeeper.election.path", "/election");
    System.setProperty("tachyon.zookeeper.leader.path", "/leader");
    System.setProperty("tachyon.master.hostname", mLocalhostName);
    System.setProperty("tachyon.master.port", mMastersPorts.get(0) + "");
    System.setProperty("tachyon.master.web.port", (mMastersPorts.get(0) + 1) + "");
    System.setProperty("tachyon.worker.port", mWorkerPort + "");
    System.setProperty("tachyon.worker.data.port", (mWorkerPort + 1) + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    mkdir(masterJournalFolder);
    CommonUtils.touch(masterJournalFolder + "/_format_" + System.currentTimeMillis());

    mkdir(CommonConf.get().UNDERFS_DATA_FOLDER);
    mkdir(CommonConf.get().UNDERFS_WORKERS_FOLDER);

    mMasters = new ArrayList<TachyonMaster>();
    mMasterThreads = new ArrayList<MasterThread>();
    for (int k = 0; k < mNumOfMasters; k ++) {
      mMasters.add(new TachyonMaster(new InetSocketAddress(mLocalhostName, mMastersPorts.get(k)),
          mMastersPorts.get(k) + 1, 1, 1, 1));
      MasterThread thread = new MasterThread(mMasters.get(k));
      thread.start();
      mMasterThreads.add(thread);
    }

    CommonUtils.sleepMs(null, 10);

    mWorker =
        TachyonWorker.createWorker(
            CommonUtils.parseInetSocketAddress(mCuratorServer.getConnectString()),
            new InetSocketAddress(mLocalhostName, mWorkerPort), mWorkerPort + 1, 1, 1, 1,
            mWorkerDataFolder, mWorkerCapacityBytes);
    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.start();
        } catch (Exception e) {
          CommonUtils.runtimeException(e + " \n Start Master Error \n" + e.getMessage());
        }
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();
  }

  public void stop() throws Exception {
    stopTFS();
    stopUFS();
  }

  public void stopTFS() throws Exception {
    for (TachyonFS fs : mClients) {
      fs.close();
    }

    mWorker.stop();
    for (int k = 0; k < mNumOfMasters; k ++) {
      mMasterThreads.get(k).shutdown();
    }
    mCuratorServer.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.usezookeeper");
    System.clearProperty("tachyon.zookeeper.address");
    System.clearProperty("tachyon.zookeeper.election.path");
    System.clearProperty("tachyon.zookeeper.leader.path");
    System.clearProperty("tachyon.master.hostname");
    System.clearProperty("tachyon.master.port");
    System.clearProperty("tachyon.master.web.port");
    System.clearProperty("tachyon.worker.port");
    System.clearProperty("tachyon.worker.data.port");
    System.clearProperty("tachyon.worker.data.folder");
    System.clearProperty("tachyon.worker.memory.size");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
  }

  public void stopUFS() throws Exception {
    if (null != mUnderFSCluster) {
      mUnderFSCluster.cleanup();
    }
    System.clearProperty("tachyon.master.journal.folder");
    System.clearProperty("tachyon.underfs.address");
  }
}
