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
import tachyon.security.UserGroup;
import tachyon.security.authentication.TSetUserProcessor;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.TachyonWorker;

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

  private TachyonWorker mWorker = null;

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

  private UserGroup mFsOwner;

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
    return mMasterConf.get(Constants.UNDERFS_ADDRESS, "/underfs");
  }

  public TachyonWorker getWorker() {
    return mWorker;
  }

  public TachyonConf getWorkerTachyonConf() {
    return mWorkerConf;
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

  public UserGroup getFsOwner() {
    return mFsOwner;
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
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mWorkerDataFolder = "/datastore";

    /** Set the loginUser to TSetUserProcessor for directly function call from MasterInfo,
     * because the directly call doesn't from thrift SASL framework, the TSetUserProcessor
     * .getRetomeUser will cause NullPointException
     * */
    mFsOwner = UserGroup.getTachyonLoginUser();
    TSetUserProcessor.setRemoteUser(mFsOwner);

    mLocalhostName = NetworkUtils.getLocalHostName();

    mMasterConf = new TachyonConf();
    mMasterConf.set(Constants.IN_TEST_MODE, "true");
    mMasterConf.set(Constants.TACHYON_HOME, mTachyonHome);
    mMasterConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    mMasterConf.set(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE, Integer.toString(mUserBlockSize));
    mMasterConf.set(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE, "64");
    //turn off
    mMasterConf.set(Constants.FS_PERMISSIONS_ENABLED_KEY, "false");

    // Lower the number of threads that the cluster will spin off.
    // default thread overhead is too much.
    mMasterConf.set(Constants.MASTER_MIN_WORKER_THREADS, "1");
    mMasterConf.set(Constants.MASTER_MAX_WORKER_THREADS, "100");
    mMasterConf.set(Constants.MASTER_WEB_THREAD_COUNT, "1");

    // re-build the dir to set permission to 777
    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);

    mMaster = LocalTachyonMaster.create(mTachyonHome, mMasterConf);
    mMaster.start();

    mkdir(mMasterConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data"));
    mkdir(mMasterConf.get(Constants.UNDERFS_WORKERS_FOLDER, "/tachyon/workers"));

    CommonUtils.sleepMs(null, 10);

    mWorkerConf = new TachyonConf(mMasterConf);
    mWorkerConf.set(Constants.MASTER_PORT, getMasterPort() + "");
    mWorkerConf.set(Constants.MASTER_WEB_PORT, (getMasterPort() + 1) + "");
    mWorkerConf.set(Constants.WORKER_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_FOLDER, mWorkerDataFolder);
    mWorkerConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    mWorkerConf.set(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, "15");
    mWorkerConf.set(Constants.WORKER_MIN_WORKER_THREADS, Integer.toString(1));
    mWorkerConf.set(Constants.WORKER_MAX_WORKER_THREADS, Integer.toString(100));
    mWorkerConf.set(Constants.WORKER_NETTY_WORKER_THREADS, Integer.toString(2));

    mWorkerConf.set("tachyon.worker.hierarchystore.level0.alias", "MEM");
    mWorkerConf.set("tachyon.worker.hierarchystore.level0.dirs.path", mTachyonHome + "/ramdisk");
    mWorkerConf.set("tachyon.worker.hierarchystore.level0.dirs.quota", mWorkerCapacityBytes + "");

    int maxLevel = mWorkerConf.getInt(Constants.WORKER_MAX_HIERARCHY_STORAGE_LEVEL, 1);
    for (int level = 1; level < maxLevel; level ++) {
      String tierLevelDirPath = "tachyon.worker.hierarchystore.level" + level + ".dirs.path";
      String[] dirPaths = mWorkerConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      String newPath = "";
      for (int i = 0; i < dirPaths.length; i ++) {
        newPath += mTachyonHome + dirPaths[i] + ",";
      }
      mWorkerConf.set("tachyon.worker.hierarchystore.level" + level + ".dirs.path",
          newPath.substring(0, newPath.length() - 1));
    }

    mWorker =
        TachyonWorker.createWorker(new InetSocketAddress(mLocalhostName, getMasterPort()),
            new InetSocketAddress(mLocalhostName, 0), 0, 1, 100, mWorkerConf);
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
    System.clearProperty("tachyon.worker.hierarchystore.level.max");
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

  /**
   * Set the user for testing
   * @param user
   */
  public void setAuthenticationUser(UserGroup user) {
    TSetUserProcessor.setRemoteUser(user);
  }

  public UserGroup getAuthenticatedUser() {
    return TSetUserProcessor.getRemoteUser();
  }
}
