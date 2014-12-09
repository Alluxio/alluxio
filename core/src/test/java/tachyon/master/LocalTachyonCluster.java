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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.conf.UserConf;
import tachyon.conf.WorkerConf;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.TachyonWorker;

/**
 * Local Tachyon cluster for unit tests.
 */
public final class LocalTachyonCluster {
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

  private long mWorkerCapacityBytes;
  private String mTachyonHome;

  private String mWorkerDataFolder;

  private Thread mWorkerThread = null;
  private String mLocalhostName = null;

  private LocalTachyonMaster mMaster;

  public LocalTachyonCluster(long workerCapacityBytes) {
    mWorkerCapacityBytes = workerCapacityBytes;
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

  public TachyonConf getMasterTachyonConf() {
    return mMaster.getTachyonConf();
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

  public String getMasterUri() {
    return mMaster.getUri();
  }

  public int getMasterPort() {
    return mMaster.getMetaPort();
  }

  public String getTachyonHome() {
    return mTachyonHome;
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

    if (ufs.exists(path)) {
      ufs.delete(path, true);
    }
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

    mLocalhostName = NetworkUtils.getLocalHostName();

    System.setProperty("tachyon.test.mode", "true");
    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.worker.port", 0 + "");
    System.setProperty("tachyon.worker.data.port", 0 + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");
    System.setProperty("tachyon.user.remote.read.buffer.size.byte", 64 + "");
    System.setProperty("tachyon.worker.selector.threads", Integer.toString(1));
    System.setProperty("tachyon.worker.server.threads", Integer.toString(2));
    System.setProperty("tachyon.worker.network.netty.worker.threads", Integer.toString(2));

    WorkerConf.clear();
    UserConf.clear();

    TachyonConf masterConf = new TachyonConf();
    mMaster = LocalTachyonMaster.create(mTachyonHome, masterConf);
    mMaster.start();

    mkdir(masterConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data"));
    mkdir(masterConf.get(Constants.UNDERFS_WORKERS_FOLDER, "/tachyon/workers"));

    CommonUtils.sleepMs(null, 10);

    TachyonConf workerConf = new TachyonConf();
    workerConf.set(Constants.MASTER_PORT, getMasterPort() + "");
    workerConf.set(Constants.MASTER_WEB_PORT, (getMasterPort() + 1) + "");
    mWorker =
        TachyonWorker.createWorker(new InetSocketAddress(mLocalhostName, getMasterPort()),
            new InetSocketAddress(mLocalhostName, 0), 0, 1, 1, 1, mWorkerDataFolder,
            mWorkerCapacityBytes, workerConf);
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
    System.clearProperty("tachyon.worker.port");
    System.clearProperty("tachyon.worker.data.port");
    System.clearProperty("tachyon.worker.data.folder");
    System.clearProperty("tachyon.worker.memory.size");
    System.clearProperty("tachyon.user.remote.read.buffer.size.byte");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
    System.clearProperty("tachyon.worker.selector.threads");
    System.clearProperty("tachyon.worker.server.threads");
    System.clearProperty("tachyon.worker.network.netty.worker.threads");
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
