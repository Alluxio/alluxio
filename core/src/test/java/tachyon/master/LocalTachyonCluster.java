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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.client.TachyonFS;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
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
  private static final int DEFAULT_WORKER_COUNT = 1;

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

  private final List<TachyonWorker> mWorkers;

  private final long mWorkerCapacityBytes;
  private final int mNumWorkers;
  private final ExecutorService mWorkerExecutor;
  private final Random mRandom = new Random();
  private String mTachyonHome;

  private String mWorkerDataFolder;

  private String mLocalhostName = null;

  private LocalTachyonMaster mMaster;

  public LocalTachyonCluster(long workerCapacityBytes) {
    this(workerCapacityBytes, DEFAULT_WORKER_COUNT);
  }

  public LocalTachyonCluster(long workerCapacityBytes, int numWorkers) {
    Preconditions.checkArgument(numWorkers > 0, "Only positive worker sizes are allowed");

    mWorkerCapacityBytes = workerCapacityBytes;
    mNumWorkers = numWorkers;
    mWorkers = Lists.newArrayListWithCapacity(mNumWorkers);
    mWorkerExecutor =
        Executors.newFixedThreadPool(mNumWorkers,
            new ThreadFactoryBuilder().setNameFormat("tachyon-worker-%d").build());
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
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  public ImmutableList<TachyonWorker> getWorkers() {
    return ImmutableList.copyOf(mWorkers);
  }

  private TachyonWorker getRandomWorker() {
    int index = mRandom.nextInt(mWorkers.size());
    return mWorkers.get(index);
  }

  /**
   * With multiple users, this method doesn't make as much sense.  Tests that use this should
   * relook at their flow to see if it can be replaced.
   *
   * The logic of this method is to return a new random worker on each call.  Callers should
   * not expect that this method returns the same result each time.  This way it works closer
   * to how the master works.
   */
  @Deprecated
  public TachyonWorker getWorker() {
    return getRandomWorker();
  }

  /**
   * With multiple users, this method doesn't make as much sense.  Tests that use this should
   * relook at their flow to see if it can be replaced.
   *
   * The logic of this method is to return a new random worker on each call.  Callers should
   * not expect that this method returns the same result each time.  This way it works closer
   * to how the master works.
   */
  @Deprecated
  public NetAddress getWorkerAddress() {
    TachyonWorker worker = getRandomWorker();
    return new NetAddress(mLocalhostName, worker.getMetaPort(), worker.getDataPort());
  }

  public String getWorkerDataFolder() {
    return mWorkerDataFolder;
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
    System.setProperty("tachyon.master.port", 0 + "");
    System.setProperty("tachyon.master.web.port", 0 + "");
    System.setProperty("tachyon.worker.port", 0 + "");
    System.setProperty("tachyon.worker.data.port", 0 + "");
    System.setProperty("tachyon.worker.data.folder", mWorkerDataFolder);
    System.setProperty("tachyon.worker.memory.size", mWorkerCapacityBytes + "");
    System.setProperty("tachyon.worker.to.master.heartbeat.interval.ms", 15 + "");
    System.setProperty("tachyon.user.remote.read.buffer.size.byte", 64 + "");
    // Lower the number of threads that the cluster will spin off.
    // default thread overhead is too much.
    System.setProperty("tachyon.master.selector.threads", Integer.toString(1));
    System.setProperty("tachyon.master.server.threads", Integer.toString(2));
    System.setProperty("tachyon.worker.selector.threads", Integer.toString(1));
    System.setProperty("tachyon.worker.server.threads", Integer.toString(2));
    System.setProperty("tachyon.worker.network.netty.worker.threads", Integer.toString(2));
    System.setProperty("tachyon.master.web.threads", Integer.toString(9));

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

    for (int i = 0; i < mNumWorkers; i++) {
      final TachyonWorker worker =
          TachyonWorker.createWorker(new InetSocketAddress(mLocalhostName, getMasterPort()),
              new InetSocketAddress(mLocalhostName, 0), 0, 1, 1, 1, mWorkerDataFolder,
              mWorkerCapacityBytes);
      Runnable runWorker = new Runnable() {
        @Override
        public void run() {
          try {
            worker.start();
          } catch (Exception e) {
            throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
          }
        }
      };
      mWorkers.add(worker);
      mWorkerExecutor.submit(runWorker);
    }
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
    shutdownWorkers();
    mWorkerExecutor.shutdown();

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
    System.clearProperty("tachyon.master.selector.threads");
    System.clearProperty("tachyon.master.server.threads");
    System.clearProperty("tachyon.worker.selector.threads");
    System.clearProperty("tachyon.worker.server.threads");
    System.clearProperty("tachyon.worker.network.netty.worker.threads");
    System.clearProperty("tachyon.master.web.threads");
  }

  private void shutdownWorkers() throws IOException, InterruptedException {
    Throwable last = null;
    for (TachyonWorker worker: mWorkers) {
      try {
        worker.stop();
      } catch (Throwable t) {
        last = t;
      }
    }
    if (last != null) {
      throw Throwables.propagate(last);
    }
    mWorkerExecutor.shutdownNow();
    mWorkerExecutor.awaitTermination(5, TimeUnit.SECONDS);
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
    shutdownWorkers();
  }
}
