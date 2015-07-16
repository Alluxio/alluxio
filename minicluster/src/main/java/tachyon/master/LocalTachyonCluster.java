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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.block.BlockWorker;

/**
 * Local Tachyon cluster for unit tests.
 */
public final class LocalTachyonCluster {
  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);

    cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(null, Constants.SECOND_MS);
  }

  private BlockWorker mWorker = null;

  private long mWorkerCapacityBytes;
  private int mUserBlockSize;
  private int mQuotaUnitBytes;

  private String mTachyonHome;

  private String mWorkerDataFolder;

  private Thread mWorkerThread = null;
  private String mLocalhostName = null;

  private LocalTachyonMaster mMaster;

  private TachyonConf mMasterConf;

  private TachyonConf mWorkerConf;

  public LocalTachyonCluster(long workerCapacityBytes, int quotaUnitBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mQuotaUnitBytes = quotaUnitBytes;
    mUserBlockSize = userBlockSize;
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
    return mMasterConf;
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

  public String getTempFolderInUnderFs() {
    return mMasterConf.get(Constants.UNDERFS_ADDRESS, "/underFSStorage");
  }

  public BlockWorker getWorker() {
    return mWorker;
  }

  public TachyonConf getWorkerTachyonConf() {
    return mWorkerConf;
  }

  public NetAddress getWorkerAddress() {
    return mWorker.getWorkerNetAddress();
  }

  public String getWorkerDataFolder() {
    return mWorkerDataFolder;
  }

  private void deleteDir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, getMasterTachyonConf());

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  private void mkdir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, getMasterTachyonConf());

    if (ufs.exists(path)) {
      ufs.delete(path, true);
    }
    if (!ufs.mkdirs(path, true)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public void start() throws IOException {
    start(new TachyonConf());
  }

  public void start(TachyonConf tachyonConf) throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mWorkerDataFolder = "/datastore";

    mLocalhostName = NetworkUtils.getLocalHostName(100);

    mMasterConf = tachyonConf;
    mMasterConf.set(Constants.IN_TEST_MODE, "true");
    mMasterConf.set(Constants.TACHYON_HOME, mTachyonHome);
    mMasterConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    mMasterConf.set(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE, Integer.toString(mUserBlockSize));
    mMasterConf.set(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE, "64");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mMasterConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    // Lower the number of threads that the cluster will spin off.
    // default thread overhead is too much.
    mMasterConf.set(Constants.MASTER_MIN_WORKER_THREADS, "1");
    mMasterConf.set(Constants.MASTER_MAX_WORKER_THREADS, "100");
    mMasterConf.set(Constants.WEB_THREAD_COUNT, "1");

    // re-build the dir to set permission to 777
    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);

    mMaster = LocalTachyonMaster.create(mTachyonHome, mMasterConf);
    mMaster.start();

    mkdir(mMasterConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data"));
    mkdir(mMasterConf.get(Constants.UNDERFS_WORKERS_FOLDER, "/tachyon/workers"));

    CommonUtils.sleepMs(null, 10);

    mWorkerConf = new TachyonConf(mMasterConf);
    mWorkerConf.set(Constants.MASTER_HOSTNAME, mLocalhostName);
    mWorkerConf.set(Constants.MASTER_PORT, getMasterPort() + "");
    mWorkerConf.set(Constants.MASTER_WEB_PORT, (getMasterPort() + 1) + "");
    mWorkerConf.set(Constants.WORKER_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_PORT, "0");
    mWorkerConf.set(Constants.WORKER_WEB_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_FOLDER, mWorkerDataFolder);
    mWorkerConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    mWorkerConf.set(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, "15");
    mWorkerConf.set(Constants.WORKER_MIN_WORKER_THREADS, Integer.toString(1));
    mWorkerConf.set(Constants.WORKER_MAX_WORKER_THREADS, Integer.toString(100));
    mWorkerConf.set(Constants.WORKER_NETTY_WORKER_THREADS, Integer.toString(2));

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mWorkerConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    mWorkerConf.set("tachyon.worker.tieredstore.level0.alias", "MEM");
    mWorkerConf.set("tachyon.worker.tieredstore.level0.dirs.path", mTachyonHome + "/ramdisk");
    mWorkerConf.set("tachyon.worker.tieredstore.level0.dirs.quota", mWorkerCapacityBytes + "");
    mkdir(mTachyonHome + "/ramdisk");

    int maxLevel = mWorkerConf.getInt(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, 1);
    for (int level = 1; level < maxLevel; level ++) {
      String tierLevelDirPath = "tachyon.worker.tieredstore.level" + level + ".dirs.path";
      String[] dirPaths = mWorkerConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      List<String> newPaths = new ArrayList<String>();
      for (int i = 0; i < dirPaths.length; i ++) {
        String dirPath = mTachyonHome + dirPaths[i];
        newPaths.add(dirPath);
        mkdir(dirPath);
      }
      mWorkerConf.set("tachyon.worker.tieredstore.level" + level + ".dirs.path", Joiner.on(',')
          .join(newPaths));
    }

    mWorker = new BlockWorker(mWorkerConf);
    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.process();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
        }
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();
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
    System.clearProperty("tachyon.worker.tieredstore.level.max");
    System.clearProperty("tachyon.worker.network.netty.worker.threads");
    System.clearProperty("tachyon.worker.min.worker.threads");
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
