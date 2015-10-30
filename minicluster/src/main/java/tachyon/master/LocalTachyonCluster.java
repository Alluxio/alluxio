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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFS;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockWorker;

/**
 * Local Tachyon cluster for integration tests.
 *
 * Example to use
 * <pre>
 * // Create a cluster instance
 * localTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES,
 *     QUOTA_UNIT_BYTES, BLOCK_SIZE_BYTES);
 * // If you have special conf parameter to set for integration tests:
 * TachyonConf testConf = localTachyonCluster.getTestConf();
 * testConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
 * // After setting up the test conf, start this local cluster:
 * localTachyonCluster.start();
 * </pre>
 */
public final class LocalTachyonCluster {
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

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockWorker mWorker = null;
  private long mWorkerCapacityBytes;
  private int mUserBlockSize;
  private int mQuotaUnitBytes;
  private String mTachyonHome;
  private Thread mWorkerThread = null;
  private String mLocalhostName = null;
  private LocalTachyonMaster mMaster;
  private TachyonConf mMasterConf;
  private TachyonConf mWorkerConf;
  private TachyonConf mClientConf;
  private TachyonConf mTestConf;

  public LocalTachyonCluster(long workerCapacityBytes, int quotaUnitBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mQuotaUnitBytes = quotaUnitBytes;
    mUserBlockSize = userBlockSize;
    mTestConf = new TachyonConf();
  }

  public TachyonFS getOldClient() throws IOException {
    return mMaster.getOldClient();
  }

  public TachyonFileSystem getClient() throws IOException {
    return mMaster.getClient();
  }

  public LocalTachyonMaster getMaster() {
    return mMaster;
  }

  public TachyonConf getMasterTachyonConf() {
    return mMasterConf;
  }

  public String getMasterHostname() {
    return mLocalhostName;
  }

  public String getMasterUri() {
    return mMaster.getUri();
  }

  public int getMasterPort() {
    return mMaster.getRPCLocalPort();
  }

  public String getTachyonHome() {
    return mTachyonHome;
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

  private void setupTestConf() throws IOException {
    Preconditions.checkNotNull(mTestConf);
    mTestConf.set(Constants.IN_TEST_MODE, "true");
    mTestConf.set(Constants.TACHYON_HOME, mTachyonHome);
    mTestConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    mTestConf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    mTestConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    mTestConf.set(Constants.MASTER_HOSTNAME, mLocalhostName);
    mTestConf.set(Constants.MASTER_PORT, Integer.toString(0));
    mTestConf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));
    mTestConf.set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, Integer.toString(1000));
    mTestConf.set(Constants.MASTER_WORKER_THREADS_MIN, "1");
    mTestConf.set(Constants.MASTER_WORKER_THREADS_MAX, "100");

    // If tests fail to connect they should fail early rather than using the default ridiculously
    // high retries
    mTestConf.set(Constants.MASTER_RETRY_COUNT, "3");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    mTestConf.set(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250");

    mTestConf.set(Constants.WEB_THREAD_COUNT, "1");
    mTestConf.set(Constants.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "../servers/src/main/webapp"));

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default tachyon storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    mTestConf.set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");
    mTestConf.set(Constants.USER_FILE_TACHYON_STORAGE_TYPE_DEFAULT, "STORE");
    mTestConf.set(Constants.USER_FILE_UNDER_STORAGE_TYPE_DEFAULT, "SYNC_PERSIST");

    mTestConf.set(Constants.WORKER_PORT, Integer.toString(0));
    mTestConf.set(Constants.WORKER_DATA_PORT, Integer.toString(0));
    mTestConf.set(Constants.WORKER_WEB_PORT, Integer.toString(0));
    mTestConf.set(Constants.WORKER_DATA_FOLDER, "/datastore");
    mTestConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    mTestConf.set(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, Integer.toString(15));
    mTestConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MIN, Integer.toString(1));
    mTestConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MAX, Integer.toString(2048));
    mTestConf.set(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS, Integer.toString(2));

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    mTestConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    mTestConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Delete the temp dir by ufs, otherwise, permission problem may be encountered.
    UnderFileSystemUtils.deleteDir(mTachyonHome, mTestConf);

    // Setup tiered store
    String ramdiskPath = PathUtils.concatPath(mTachyonHome, "/ramdisk");
    mTestConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    mTestConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 0),
        ramdiskPath);
    mTestConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        Long.toString(mWorkerCapacityBytes));
    UnderFileSystemUtils.mkdirIfNotExists(ramdiskPath, mTestConf);

    int numLevels = mWorkerConf.getInt(Constants.WORKER_TIERED_STORAGE_LEVELS);
    for (int level = 1; level < numLevels; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = mTestConf.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<String>();
      for (String dirPath : dirPaths) {
        String newPath = mTachyonHome + dirPath;
        newPaths.add(newPath);
        UnderFileSystemUtils.mkdirIfNotExists(newPath, mTestConf);
      }
      mTestConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level),
          Joiner.on(',').join(newPaths));
    }
  }

  public TachyonConf getTestConf() {
    return mTestConf;
  }

  /**
   * Configures and starts master.
   *
   * @throws IOException when the operation fails
   */
  private void startMaster() throws IOException {
    mMasterConf = new TachyonConf(mTestConf.getInternalProperties());
    MasterContext.reset(mMasterConf);

    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    // Update the test conf with actual RPC port.
    mTestConf.set(Constants.MASTER_PORT, String.valueOf(getMasterPort()));

    // We need to update client context with the most recent configuration so they know the correct
    // port to connect to master.
    mClientConf = new TachyonConf(mTestConf.getInternalProperties());
    ClientContext.reset(mClientConf);
  }

  /**
   * Configure and start worker.
   *
   * @throws IOException when the operation fails
   */
  private void startWorker() throws IOException {
    // We need to update the worker context with the most recent configuration so they know the
    // correct port to connect to master.
    mWorkerConf = new TachyonConf(mTestConf.getInternalProperties());
    WorkerContext.reset(mWorkerConf);

    mWorker = new BlockWorker();
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
   * Starts both a master and a worker using the configurations in test conf respectively.
   *
   * @throws IOException when the operation fails
   */
  public void start() throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mLocalhostName = NetworkAddressUtils.getLocalHostName(100);
    setupTestConf();

    // Disable hdfs client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    startMaster();

    UnderFileSystemUtils.mkdirIfNotExists(mMasterConf.get(Constants.UNDERFS_ADDRESS),
        mMasterConf);
    CommonUtils.sleepMs(10);

    startWorker();
    // wait until worker registered with master
    // TODO(binfan): use callback to ensure LocalTachyonCluster setup rather than sleep
    CommonUtils.sleepMs(100);
  }

  /**
   * Stop both of the tachyon and underfs service threads.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    stopTFS();
    stopUFS();

    MasterContext.reset();
    WorkerContext.reset();
    ClientContext.reset();

    // clear HDFS client caching
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  /**
   * Stop the tachyon filesystem's service thread only.
   *
   * @throws Exception when the operation fails
   */
  public void stopTFS() throws Exception {
    LOG.info("stop Tachyon filesytstem");

    // Stopping Worker before stopping master speeds up tests
    mWorker.stop();
    mMaster.stop();
  }

  /**
   * Cleanup the underfs cluster test folder only.
   *
   * @throws Exception when the operation fails
   */
  public void stopUFS() throws Exception {
    LOG.info("stop under storage system");
    mMaster.cleanupUnderfs();
  }

  /**
   * Cleanup the worker state from the master and stop the worker.
   *
   * @throws Exception when the operation fails
   */
  public void stopWorker() throws Exception {
    mMaster.clearClients();
    mWorker.stop();
  }
}
