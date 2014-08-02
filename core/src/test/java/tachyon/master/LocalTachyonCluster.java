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

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystemCluster;
import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.worker.TachyonWorker;

/**
 * Local Tachyon cluster for unit tests.
 */
public class LocalTachyonCluster {
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

//  private TachyonMaster mMaster = null;

  private TachyonWorker mWorker = null;

  private long mWorkerCapacityBytes;
  private String mTachyonHome;

  private String mWorkerDataFolder;
//  private Thread mMasterThread = null;

  private Thread mWorkerThread = null;
  private String mLocalhostName = null;

//  private UnderFileSystemCluster mUnderFSCluster = null;

//  private List<TachyonFS> mClients = new ArrayList<TachyonFS>();

  private LocalTachyonMaster mMaster;

  public LocalTachyonCluster(long workerCapacityBytes) {
    mWorkerCapacityBytes = workerCapacityBytes;
  }

  public TachyonFS getClient() throws IOException {
//    mClients.add(TachyonFS.get(Constants.HEADER + mLocalhostName + ":" + getMasterPort()));
//    return mClients.get(mClients.size() - 1);
    return mMaster.getClient();
  }

  public String getEditLogPath() {
//    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/log.data";
    return mMaster.getEditLogPath();
  }

  public String getImagePath() {
//    return mUnderFSCluster.getUnderFilesystemAddress() + "/journal/image.data";
    return mMaster.getImagePath();
  }

  public InetSocketAddress getMasterAddress() {
    return new InetSocketAddress(mLocalhostName, getMasterPort());
  }

  public String getMasterHostname() {
    return mLocalhostName;
  }

  public MasterInfo getMasterInfo() {
    return mMaster.getMasterInfo();
  }

  public int getMasterPort() {
//    return mMaster.getLocalPort();
    return mMaster.getPort();
  }

  public String getTachyonHome() {
    return mTachyonHome;
  }

  public String getTempFolderInUnderFs() {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  public TachyonWorker getWorker() {
    return mWorker;
  }

  public NetAddress getWorkerAddress() {
    return new NetAddress(mLocalhostName, getWorkerPort(), getWorkerDataPort());
  }

  public String getWorkerDataFolder() {
    return mWorkerDataFolder;
  }

  public int getWorkerPort() {
    return mWorker.getMetaPort();
  }

  public int getWorkerDataPort() {
    return mWorker.getDataPort();
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
//    String masterDataFolder = mTachyonHome + "/data";
//    String masterLogFolder = mTachyonHome + "/logs";

    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);
//    mkdir(masterDataFolder);
//    mkdir(masterLogFolder);

    mLocalhostName = InetAddress.getLocalHost().getCanonicalHostName();

//    // To start the UFS either for integration or unit test. If it targets the unit test, UFS is
//    // setup over the local file system (see also {@link LocalFilesystemCluster} - under folder of
//    // "mTachyonHome/tachyon*". Otherwise, it starts some distributed file system cluster e.g.,
//    // miniDFSCluster (see also {@link tachyon.LocalMiniDFScluster} and setup the folder like
//    // "hdfs://xxx:xxx/tachyon*".
//    mUnderFSCluster = UnderFileSystemCluster.get(mTachyonHome + "/dfs");
//    String underfsFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/tachyon_underfs_folder";
//    // To setup the journalFolder under either local file system or distributed ufs like
//    // miniDFSCluster
//    String masterJournalFolder = mUnderFSCluster.getUnderFilesystemAddress() + "/journal";

    System.setProperty("tachyon.home", mTachyonHome);
//    System.setProperty("tachyon.master.hostname", mLocalhostName);
//    System.setProperty("tachyon.master.journal.folder", masterJournalFolder);
    System.setProperty("tachyon.master.port", 0 + "");
    System.setProperty("tachyon.master.web.port", 0 + "");
    System.setProperty("tachyon.worker.port", 0 + "");
    System.setProperty("tachyon.worker.data.port", 0 + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");
//    System.setProperty("tachyon.underfs.address", underfsFolder);
    System.setProperty("tachyon.user.remote.read.buffer.size.byte", 64 + "");

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

//    mkdir(masterJournalFolder);
//    CommonUtils.touch(masterJournalFolder + "/_format_" + System.currentTimeMillis());



//    mMaster =
//        new TachyonMaster(new InetSocketAddress(mLocalhostName, 0), 0, 1,
//            1, 1);
//
//    Runnable runMaster = new Runnable() {
//      @Override
//      public void run() {
//        try {
//          mMaster.start();
//        } catch (Exception e) {
//          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
//        }
//      }
//    };
//    mMasterThread = new Thread(runMaster);
//    mMasterThread.start();
    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    mkdir(CommonConf.get().UNDERFS_DATA_FOLDER);
    mkdir(CommonConf.get().UNDERFS_WORKERS_FOLDER);

    CommonUtils.sleepMs(null, 10);
    
    System.setProperty("tachyon.master.port", getMasterPort() + "");
    System.setProperty("tachyon.master.web.port", (getMasterPort() + 1) + "");

    mWorker =
        TachyonWorker.createWorker(new InetSocketAddress(mLocalhostName, getMasterPort()),
            new InetSocketAddress(mLocalhostName, 0), 0, 1, 1, 1,
            mWorkerDataFolder, mWorkerCapacityBytes);
    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.start();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
        }
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();

    System.setProperty("tachyon.worker.port", getWorkerPort() + "");
    System.setProperty("tachyon.worker.data.port", getWorkerDataPort() + "");
  }

  /**
   * Stop both of the tachyon and underfs service threads.
   * 
   * @throws Exception
   */
  public void stop() throws Exception {
    stopTFS();
    stopUFS();
  }

  /**
   * Stop the tachyon filesystem's service thread only
   * 
   * @throws Exception
   */
  public void stopTFS() throws Exception {
//    for (TachyonFS fs : mClients) {
//      fs.close();
//    }

    mMaster.stop();
    mWorker.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.master.hostname");
    System.clearProperty("tachyon.master.port");
    System.clearProperty("tachyon.master.web.port");
    System.clearProperty("tachyon.worker.port");
    System.clearProperty("tachyon.worker.data.port");
    System.clearProperty("tachyon.worker.data.folder");
    System.clearProperty("tachyon.worker.memory.size");
    System.clearProperty("tachyon.user.remote.read.buffer.size.byte");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
  }

  /**
   * Cleanup the underfs cluster test folder only
   *
   * @throws Exception
   */
  public void stopUFS() throws Exception {
//    if (null != mUnderFSCluster) {
//      mUnderFSCluster.cleanup();
//    }
//    System.clearProperty("tachyon.master.journal.folder");
//    System.clearProperty("tachyon.underfs.address");

    mMaster.cleanupUnderfs();
  }

  public void stopWorker() throws Exception {
//    for (TachyonFS fs : mClients) {
//      fs.close();
//    }
//    mClients.clear();
    mMaster.clearClients();
    mWorker.stop();
  }
}
