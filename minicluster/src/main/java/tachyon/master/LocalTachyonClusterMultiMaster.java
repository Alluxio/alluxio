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
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.worker.block.BlockWorker;

/**
 * A local Tachyon cluster with Multiple masters
 */
public class LocalTachyonClusterMultiMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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

  private TestingServer mCuratorServer = null;
  private int mNumOfMasters = 0;
  private BlockWorker mWorker = null;
  private long mWorkerCapacityBytes;
  private int mUserBlockSize;

  private String mTachyonHome;
  private String mWorkerDataFolder;

  private Thread mWorkerThread = null;

  private final List<LocalTachyonMaster> mMasters = new ArrayList<LocalTachyonMaster>();

  private final Supplier<String> mClientSuppliers = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool mClientPool = new ClientPool(mClientSuppliers);

  private TachyonConf mMasterConf;

  private TachyonConf mWorkerConf;

  public LocalTachyonClusterMultiMaster(long workerCapacityBytes, int masters, int userBlockSize) {
    mNumOfMasters = masters;
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;

    try {
      mCuratorServer = new TestingServer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public synchronized TachyonFS getClient() throws IOException {
    return mClientPool.getClient(mMasterConf);
  }

  public String getUri() {
    return Constants.HEADER_FT + mCuratorServer.getConnectString();
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
    UnderFileSystem ufs = UnderFileSystem.get(path, mMasterConf);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  private void mkdir(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, mMasterConf);

    if (ufs.exists(path)) {
      ufs.delete(path, true);
    }
    if (!ufs.mkdirs(path, true)) {
      throw new IOException("Failed to make folder: " + path);
    }
  }

  public void start() throws IOException {
    int maxLevel = 1;
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mWorkerDataFolder = "/datastore";

    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";

    mMasterConf = new TachyonConf();
    mMasterConf.set(Constants.IN_TEST_MODE, "true");
    mMasterConf.set(Constants.TACHYON_HOME, mTachyonHome);
    mMasterConf.set(Constants.USE_ZOOKEEPER, "true");
    mMasterConf.set(Constants.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
    mMasterConf.set(Constants.ZOOKEEPER_ELECTION_PATH, "/election");
    mMasterConf.set(Constants.ZOOKEEPER_LEADER_PATH, "/leader");
    mMasterConf.set(Constants.USER_QUOTA_UNIT_BYTES, "10000");
    mMasterConf.set(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE, Integer.toString(mUserBlockSize));

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mMasterConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    // re-build the dir to set permission to 777
    deleteDir(mTachyonHome);
    mkdir(mTachyonHome);
    mkdir(masterDataFolder);
    mkdir(masterLogFolder);

    mkdir(mMasterConf.get(Constants.UNDERFS_DATA_FOLDER, "/tachyon/data"));
    mkdir(mMasterConf.get(Constants.UNDERFS_WORKERS_FOLDER, "/tachyon/workers"));

    for (int k = 0; k < mNumOfMasters; k ++) {
      final LocalTachyonMaster master = LocalTachyonMaster.create(mTachyonHome, mMasterConf);
      master.start();
      mMasters.add(master);
    }

    CommonUtils.sleepMs(null, 10);

    mWorkerConf = new TachyonConf(mMasterConf);
    mWorkerConf.set(Constants.WORKER_DATA_FOLDER, mWorkerDataFolder);
    mWorkerConf.set(Constants.WORKER_MEMORY_SIZE, mWorkerCapacityBytes + "");
    mWorkerConf.set(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, 15 + "");

    // Setup conf for worker
    mWorkerConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, Integer.toString(maxLevel));
    mWorkerConf.set("tachyon.worker.tieredstore.level0.alias", "MEM");
    mWorkerConf.set("tachyon.worker.tieredstore.level0.dirs.path", mTachyonHome + "/ramdisk");
    mWorkerConf.set("tachyon.worker.tieredstore.level0.dirs.quota", mWorkerCapacityBytes + "");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mWorkerConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    for (int level = 1; level < maxLevel; level ++) {
      String tierLevelDirPath = "tachyon.worker.tieredstore.level" + level + ".dirs.path";
      String[] dirPaths = mWorkerConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      String newPath = "";
      for (int i = 0; i < dirPaths.length; i ++) {
        newPath += mTachyonHome + dirPaths[i] + ",";
      }
      mWorkerConf.set("tachyon.worker.tieredstore.level" + level + ".dirs.path",
          newPath.substring(0, newPath.length() - 1));
    }

    mWorkerConf.set(Constants.MASTER_ADDRESS, mCuratorServer.getConnectString().split(":")[0]);
    mWorkerConf.set(Constants.MASTER_PORT, mCuratorServer.getPort() + "");
    mWorkerConf.set(Constants.WORKER_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_PORT, "0");
    mWorkerConf.set(Constants.WORKER_MIN_WORKER_THREADS, "1");
    mWorkerConf.set(Constants.WORKER_MAX_WORKER_THREADS, "100");

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    mWorker = new BlockWorker(mWorkerConf);
    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.process();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
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
    mClientPool.close();

    mWorker.stop();
    for (int k = 0; k < mNumOfMasters; k ++) {
      mMasters.get(k).stop();
    }
    mCuratorServer.stop();

    System.clearProperty("tachyon.home");
    System.clearProperty("tachyon.usezookeeper");
    System.clearProperty("tachyon.zookeeper.address");
    System.clearProperty("tachyon.zookeeper.election.path");
    System.clearProperty("tachyon.zookeeper.leader.path");
    System.clearProperty("tachyon.worker.port");
    System.clearProperty("tachyon.worker.data.port");
    System.clearProperty("tachyon.worker.data.folder");
    System.clearProperty("tachyon.worker.memory.size");
    System.clearProperty("tachyon.worker.to.master.heartbeat.interval.ms");
  }

  public void stopUFS() throws Exception {
    // masters share underfs, so only need to call on the first master
    mMasters.get(0).cleanupUnderfs();
  }
}
