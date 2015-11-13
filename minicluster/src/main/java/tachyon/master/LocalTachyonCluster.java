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

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.TachyonFS;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystemCluster;
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
 * TachyonConf testConf = localTachyonCluster.newTestConf();
 * testConf.set(Constants.USER_FILE_BUFFER_BYTES, String.valueOf(BUFFER_BYTES));
 * // After setting up the test conf, start this local cluster:
 * localTachyonCluster.start(testConf);
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

  public LocalTachyonCluster(long workerCapacityBytes, int quotaUnitBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mQuotaUnitBytes = quotaUnitBytes;
    mUserBlockSize = userBlockSize;
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

  public TachyonConf newTestConf() throws IOException {
    TachyonConf testConf = new TachyonConf();
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
    mLocalhostName = NetworkAddressUtils.getLocalHostName(100);

    testConf.set(Constants.IN_TEST_MODE, "true");
    testConf.set(Constants.TACHYON_HOME, mTachyonHome);
    testConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    testConf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    testConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    testConf.set(Constants.MASTER_HOSTNAME, mLocalhostName);
    testConf.set(Constants.MASTER_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, Integer.toString(1000));
    testConf.set(Constants.MASTER_WORKER_THREADS_MIN, "1");
    testConf.set(Constants.MASTER_WORKER_THREADS_MAX, "100");
    testConf.set(Constants.THRIFT_STOP_TIMEOUT_SECONDS, "0");

    // If tests fail to connect they should fail early rather than using the default ridiculously
    // high retries
    testConf.set(Constants.MASTER_RETRY_COUNT, "3");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    testConf.set(Constants.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250");

    testConf.set(Constants.WEB_THREAD_COUNT, "1");
    testConf.set(Constants.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "../servers/src/main/webapp"));

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default tachyon storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    testConf.set(Constants.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");

    testConf.set(Constants.WORKER_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_FOLDER, "/datastore");
    testConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    testConf.set(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, Integer.toString(15));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MIN, Integer.toString(1));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MAX, Integer.toString(2048));
    testConf.set(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS, Integer.toString(2));

    // Perform immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Setup tiered store
    String ramdiskPath = PathUtils.concatPath(mTachyonHome, "ramdisk");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 0),
        ramdiskPath);
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        Long.toString(mWorkerCapacityBytes));

    int numLevel = testConf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = testConf.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<String>();
      for (String dirPath : dirPaths) {
        String newPath = mTachyonHome + dirPath;
        newPaths.add(newPath);
      }
      testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level),
          Joiner.on(',').join(newPaths));
    }
    return testConf;
  }

  /**
   * Sets up corresponding directories for tests.
   *
   * @param testConf configuration of this test
   * @throws IOException when creating or deleting dirs failed
   */
  private void setupTest(TachyonConf testConf) throws IOException {
    String tachyonHome = testConf.get(Constants.TACHYON_HOME);
    // Delete the tachyon home dir for this test from ufs to avoid permission problems
    UnderFileSystemUtils.deleteDir(tachyonHome, testConf);

    // Create ufs dir
    UnderFileSystemUtils.mkdirIfNotExists(testConf.get(Constants.UNDERFS_ADDRESS),
        testConf);

    // Create storage dirs for worker
    int numLevel = testConf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = testConf.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        UnderFileSystemUtils.mkdirIfNotExists(dirPath, testConf);
      }
    }
  }

  /**
   * Configures and starts master.
   *
   * @throws IOException when the operation fails
   */
  private void startMaster(TachyonConf testConf) throws IOException {
    mMasterConf = new TachyonConf(testConf.getInternalProperties());
    MasterContext.reset(mMasterConf);

    mMaster = LocalTachyonMaster.create(mTachyonHome);
    mMaster.start();

    // Update the test conf with actual RPC port.
    testConf.set(Constants.MASTER_PORT, String.valueOf(getMasterPort()));

    // If we are using the LocalMiniDFSCluster, we need to update the UNDERFS_ADDRESS to point to
    // the cluster's current address. This must happen here because the cluster isn't initialized
    // until mMaster is started.
    UnderFileSystemCluster ufs = UnderFileSystemCluster.get();
    // TODO(andrew): Move logic to the integration-tests project so that we can use instanceof here
    // instead of comparing classnames.
    if (ufs.getClass().getSimpleName().equals("LocalMiniDFSCluster")) {
      String ufsAddress = ufs.getUnderFilesystemAddress() + mTachyonHome;
      testConf.set(Constants.UNDERFS_ADDRESS, ufsAddress);
      MasterContext.getConf().set(Constants.UNDERFS_ADDRESS, ufsAddress);
      mMasterConf = MasterContext.getConf();
    }

    // We need to update client context with the most recent configuration so they know the correct
    // port to connect to master.
    mClientConf = new TachyonConf(testConf.getInternalProperties());
    ClientContext.reset(mClientConf);
  }

  /**
   * Configure and start worker.
   *
   * @throws IOException when the operation fails
   */
  private void startWorker(TachyonConf testConf) throws IOException {
    // We need to update the worker context with the most recent configuration so they know the
    // correct port to connect to master.
    mWorkerConf = new TachyonConf(testConf.getInternalProperties());
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
   * Starts both a master and a worker using the default test configurations.
   *
   * @throws IOException when the operation fails
   */
  public void start() throws IOException {
    start(newTestConf());
  }

  /**
   * Starts both a master and a worker using the configurations in test conf respectively.
   *
   * @throws IOException when the operation fails
   */
  public void start(TachyonConf conf) throws IOException {
    // Disable hdfs client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    setupTest(conf);

    startMaster(conf);

    CommonUtils.sleepMs(10);

    startWorker(conf);
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
