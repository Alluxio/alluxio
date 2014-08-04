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

import tachyon.Constants;
import tachyon.UnderFileSystem;
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

  private TachyonWorker mWorker = null;

  private final long mWorkerMemCapacityBytes;
  private String mTachyonHome;

  private String mWorkerDataFolder;

  private Thread mWorkerThread = null;
  private String mLocalhostName = null;

  private LocalTachyonMaster mMaster;

  public LocalTachyonCluster(long workerMemCapacityBytes) {
    mWorkerMemCapacityBytes = workerMemCapacityBytes;
  }

  public TachyonFS getClient() throws IOException {
    return mMaster.getClient();
  }

  public String getEditLogPath() {
    return mMaster.getEditLogPath();
  }

  public String getImagePath() {
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
    return mMaster.getMetaPort();
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

    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);

    mLocalhostName = InetAddress.getLocalHost().getCanonicalHostName();

    System.setProperty("tachyon.test.mode", "true");
    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.master.port", 0 + "");
    System.setProperty("tachyon.master.web.port", 0 + "");
    System.setProperty("tachyon.worker.port", 0 + "");
    System.setProperty("tachyon.worker.data.port", 0 + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    if (System.getProperty("tachyon.worker.hierarchystore.level.max") == null) {
      System.setProperty("tachyon.worker.hierarchystore.level.max", 1 + "");
    }
    System.setProperty("tachyon.worker.hierarchystore.level0.alias", "MEM");
    String sysTempDir = System.getProperty("java.io.tmpdir", "/tmp");
    System.setProperty("tachyon.worker.hierarchystore.level0.dirs", sysTempDir + "/mem");
    System.setProperty("tachyon.worker.hierarchystore.level0.dir.quota", mWorkerMemCapacityBytes
        + "");

    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");
    System.setProperty("tachyon.user.remote.read.buffer.size.byte", 64 + "");

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    mkdir(CommonConf.get().UNDERFS_DATA_FOLDER);
    mkdir(CommonConf.get().UNDERFS_WORKERS_FOLDER);

    CommonUtils.sleepMs(null, 10);

    System.setProperty("tachyon.master.port", getMasterPort() + "");
    System.setProperty("tachyon.master.web.port", (getMasterPort() + 1) + "");

    mWorker =
        TachyonWorker.createWorker(new InetSocketAddress(mLocalhostName, getMasterPort()),
            new InetSocketAddress(mLocalhostName, 0), 0, 1, 1, 1, mWorkerDataFolder);
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
    System.clearProperty("tachyon.worker.hierarchystore.level.max");
  }

  /**
   * Cleanup the underfs cluster test folder only
   * 
   * @throws Exception
   */
  public void stopUFS() throws Exception {
    mMaster.cleanupUnderfs();
  }

  public void stopWorker() throws Exception {
    mMaster.clearClients();
    mWorker.stop();
  }
}
