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
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockWorker;

/**
 * A local Tachyon cluster with Multiple masters
 */
public class LocalTachyonClusterMultiMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static void main(String[] args) throws Exception {
    LocalTachyonCluster cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(Constants.SECOND_MS);

    cluster = new LocalTachyonCluster(100, 8 * Constants.MB, Constants.GB);
    cluster.start();
    CommonUtils.sleepMs(Constants.SECOND_MS);
    cluster.stop();
    CommonUtils.sleepMs(Constants.SECOND_MS);
  }

  private TestingServer mCuratorServer = null;
  private int mNumOfMasters = 0;
  private BlockWorker mWorker = null;
  private long mWorkerCapacityBytes;
  private int mUserBlockSize;

  private String mTachyonHome;
  private String mWorkerDataFolder;
  private String mHostname;

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

  public synchronized TachyonFileSystem getClient() throws IOException {
    return mClientPool.getClient(mMasterConf);
  }

  public TachyonConf getMasterTachyonConf() {
    return mMasterConf;
  }

  public String getUri() {
    return Constants.HEADER_FT + mHostname + ":" + getMasterPort();
  }

  public int getMasterPort() {
    return mMasters.get(0).getRPCLocalPort();
  }

  public boolean killLeader() {
    for (int k = 0; k < mNumOfMasters; k ++) {
      if (mMasters.get(k).isServing()) {
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

    mHostname = NetworkAddressUtils.getLocalHostName(100);

    String masterDataFolder = mTachyonHome + "/data";
    String masterLogFolder = mTachyonHome + "/logs";

    // TODO: Would be good to have a masterContext as well
    mMasterConf = new TachyonConf();
    mMasterConf.set(Constants.IN_TEST_MODE, "true");
    mMasterConf.set(Constants.TACHYON_HOME, mTachyonHome);
    mMasterConf.set(Constants.USE_ZOOKEEPER, "true");
    mMasterConf.set(Constants.MASTER_HOSTNAME, mHostname);
    mMasterConf.set(Constants.MASTER_BIND_HOST, mHostname);
    mMasterConf.set(Constants.MASTER_PORT, "0");
    mMasterConf.set(Constants.MASTER_WEB_BIND_HOST, mHostname);
    mMasterConf.set(Constants.MASTER_WEB_PORT, "0");
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
      // Each master should generate a new port for binding
      mMasterConf.set(Constants.MASTER_PORT, "0");
    }
    // Use first master port
    mMasterConf.set(Constants.MASTER_PORT, getMasterPort() + "");

    CommonUtils.sleepMs(null, 10);

    mWorkerConf = WorkerContext.getConf();
    mWorkerConf.merge(mMasterConf);
    mWorkerConf.set(Constants.WORKER_DATA_FOLDER, mWorkerDataFolder);
    mWorkerConf.set(Constants.WORKER_MEMORY_SIZE, mWorkerCapacityBytes + "");
    mWorkerConf.set(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, 15 + "");

    // Setup conf for worker
    mWorkerConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, Integer.toString(maxLevel));
    mWorkerConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    mWorkerConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 0),
        mTachyonHome + "/ramdisk");
    mWorkerConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        mWorkerCapacityBytes + "");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mWorkerConf.set(Constants.HOST_RESOLUTION_TIMEOUT_MS, "250");

    for (int level = 1; level < maxLevel; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = mWorkerConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      String newPath = "";
      for (String dirPath : dirPaths) {
        newPath += mTachyonHome + dirPath + ",";
      }
      mWorkerConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, level),
          newPath.substring(0, newPath.length() - 1));
    }

    mWorkerConf.set(Constants.WORKER_BIND_HOST, mHostname);
    mWorkerConf.set(Constants.WORKER_PORT, "0");
    mWorkerConf.set(Constants.WORKER_DATA_BIND_HOST, mHostname);
    mWorkerConf.set(Constants.WORKER_DATA_PORT, "0");
    mWorkerConf.set(Constants.WORKER_WEB_BIND_HOST, mHostname);
    mWorkerConf.set(Constants.WORKER_WEB_PORT, "0");
    mWorkerConf.set(Constants.WORKER_MIN_WORKER_THREADS, "1");
    mWorkerConf.set(Constants.WORKER_MAX_WORKER_THREADS, "100");

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    mWorkerConf.set(Constants.WORKER_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    mWorker = new BlockWorker();
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

    System.clearProperty(Constants.TACHYON_HOME);
    System.clearProperty(Constants.USE_ZOOKEEPER);
    System.clearProperty(Constants.ZOOKEEPER_ADDRESS);
    System.clearProperty(Constants.ZOOKEEPER_ELECTION_PATH);
    System.clearProperty(Constants.ZOOKEEPER_LEADER_PATH);
    System.clearProperty(Constants.WORKER_PORT);
    System.clearProperty(Constants.WORKER_DATA_PORT);
    System.clearProperty(Constants.WORKER_DATA_FOLDER);
    System.clearProperty(Constants.WORKER_MEMORY_SIZE);
    System.clearProperty(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
  }

  public void stopUFS() throws Exception {
    // masters share underfs, so only need to call on the first master
    mMasters.get(0).cleanupUnderfs();
  }
}
