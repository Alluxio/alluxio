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

import com.google.common.base.Supplier;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;

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
  private TachyonWorker mWorker = null;
  private long mWorkerCapacityBytes;

  private String mTachyonHome;
  private String mWorkerDataFolder;

  private Thread mWorkerThread = null;

  private String mLocalhostName = null;

  private final List<LocalTachyonMaster> mMasters = new ArrayList<LocalTachyonMaster>();

  private final Supplier<String> clientSupplier = new Supplier<String>() {
    @Override
    public String get() {
      return getUri();
    }
  };
  private final ClientPool clientPool = new ClientPool(clientSupplier);

  public LocalTachyonClusterMultiMaster(long workerCapacityBytes, int masters) {
    mNumOfMasters = masters;
    mWorkerCapacityBytes = workerCapacityBytes;

    try {
      mCuratorServer = new TestingServer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public synchronized TachyonFS getClient() throws IOException {
    return clientPool.getClient();
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

    System.setProperty("tachyon.test.mode", "true");
    System.setProperty("tachyon.home", mTachyonHome);
    System.setProperty("tachyon.usezookeeper", "true");
    System.setProperty("tachyon.zookeeper.address", mCuratorServer.getConnectString());
    System.setProperty("tachyon.zookeeper.election.path", "/election");
    System.setProperty("tachyon.zookeeper.leader.path", "/leader");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");

    CommonConf.clear();
    MasterConf.clear();
    WorkerConf.clear();
    UserConf.clear();

    mkdir(CommonConf.get().UNDERFS_DATA_FOLDER);
    mkdir(CommonConf.get().UNDERFS_WORKERS_FOLDER);

    for (int k = 0; k < mNumOfMasters; k ++) {
      final LocalTachyonMaster master = LocalTachyonMaster.create(mTachyonHome);
      master.start();
      mMasters.add(master);
    }

    CommonUtils.sleepMs(null, 10);

    mWorker =
        TachyonWorker.createWorker(
            CommonUtils.parseInetSocketAddress(mCuratorServer.getConnectString()),
            new InetSocketAddress(mLocalhostName, 0), 0, 1, 1, 1,
            mWorkerDataFolder, mWorkerCapacityBytes);
    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.start();
        } catch (Exception e) {
          throw new RuntimeException(e + " \n Start Master Error \n" + e.getMessage(), e);
        }
      }
    };
    mWorkerThread = new Thread(runWorker);
    mWorkerThread.start();
    System.setProperty("tachyon.worker.port", mWorker.getMetaPort() + "");
    System.setProperty("tachyon.worker.data.port", mWorker.getDataPort() + "");
  }

  public void stop() throws Exception {
    stopTFS();
    stopUFS();
  }

  public void stopTFS() throws Exception {
    clientPool.close();

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
    // masters share underfs, so only need to call on the first master
    mMasters.get(0).cleanupUnderfs();
  }
}
