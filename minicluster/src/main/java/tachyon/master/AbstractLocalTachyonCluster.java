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
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import tachyon.Constants;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.underfs.UnderFileSystemCluster;
import tachyon.util.CommonUtils;
import tachyon.util.UnderFileSystemUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.file.FileSystemWorker;

/**
 * Local Tachyon cluster.
 */
public abstract class AbstractLocalTachyonCluster {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long CLUSTER_READY_POLL_INTERVAL_MS = 10;
  private static final long CLUSTER_READY_TIMEOUT_MS = 60000;
  private static final String ELLIPSIS = "…";
  private static final Random RANDOM_GENERATOR = new Random();

  protected long mWorkerCapacityBytes;
  protected int mUserBlockSize;
  protected int mQuotaUnitBytes = 1000;

  protected TachyonConf mMasterConf;
  protected TachyonConf mWorkerConf;

  protected BlockWorker mWorker;
  protected FileSystemWorker mFileSystemWorker;
  protected UnderFileSystemCluster mUfsCluster;

  protected String mTachyonHome;
  protected String mHostname;

  protected Thread mWorkerThread;

  /** The names of all the master services, for creating journal folders. */
  // TODO(gpang): Consolidate this array of services with the one in Format.java
  protected String[] mMasterServiceNames = new String[] {
      Constants.BLOCK_MASTER_NAME,
      Constants.FILE_SYSTEM_MASTER_NAME,
      Constants.LINEAGE_MASTER_NAME,
      Constants.RAW_TABLE_MASTER_NAME,
  };

  public AbstractLocalTachyonCluster(long workerCapacityBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;
  }

  /**
   * Starts both a master and a worker using the default test configurations.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start() throws IOException, ConnectionFailedException  {
    start(newTestConf());
  }

  /**
   * Starts both master and a worker using the configurations in test conf respectively.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start(TachyonConf conf) throws IOException, ConnectionFailedException {
    // Disable hdfs client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    setupTest(conf);

    startMaster(conf);

    waitForMasterReady();

    startWorker(conf);

    waitForWorkerReady();
  }

  /**
   * Waits for the master to be ready.
   *
   * Specifically, waits for it to be possible to connect to the master's rpc and web ports.
   */
  private void waitForMasterReady() {
    long startTime = System.currentTimeMillis();
    String actionMessage = "waiting for master to serve web";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(getMaster().getWebBindHost(),
        getMaster().getWebLocalPort()) || mMasterConf.getInt(Constants.MASTER_WEB_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for master to serve rpc";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(getMaster().getRPCBindHost(),
        getMaster().getRPCLocalPort()) || mMasterConf.getInt(Constants.MASTER_RPC_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
  }

  /**
   * Waits for the worker to be ready.
   *
   * Specifically, waits for the worker to register with the master and for it to be possible to
   * connect to the worker's data, rpc, and web ports.
   */
  private void waitForWorkerReady() {
    long startTime = System.currentTimeMillis();
    String actionMessage = "waiting for worker to register with master";
    LOG.info(actionMessage + ELLIPSIS);
    while (!workerRegistered()) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for worker to serve web";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mWorker.getWebBindHost(), mWorker.getWebLocalPort())
        || mWorkerConf.getInt(Constants.WORKER_WEB_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for worker to serve data";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mWorker.getDataBindHost(), mWorker.getDataLocalPort())
        || mWorkerConf.getInt(Constants.WORKER_DATA_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
    actionMessage = "waiting for worker to serve rpc";
    LOG.info(actionMessage + ELLIPSIS);
    // The port should be set properly after the server has started
    while (!NetworkAddressUtils.isServing(mWorker.getRPCBindHost(), mWorker.getRPCLocalPort())
        || mWorkerConf.getInt(Constants.WORKER_RPC_PORT) == 0) {
      waitAndCheckTimeout(startTime, actionMessage);
    }
  }

  /**
   * Checks whether the time since startTime has exceeded the maximum timeout, then sleeps for
   * {@link #CLUSTER_READY_POLL_INTERVAL_MS}ms
   *
   * @param startTime the time to compare against the current time to check for timeout
   * @param actionMessage a message describing the action being waited for; this message is included
   *        in the error message reported if timeout occurs
   */
  private void waitAndCheckTimeout(long startTime, String actionMessage) {
    if (System.currentTimeMillis() - startTime > CLUSTER_READY_TIMEOUT_MS) {
      throw new RuntimeException("Failed to start cluster. Timed out " + actionMessage);
    }
    CommonUtils.sleepMs(CLUSTER_READY_POLL_INTERVAL_MS);
  }

  /**
   * @return whether the worker has registered with the master
   */
  private boolean workerRegistered() {
    return WorkerIdRegistry.getWorkerId() != WorkerIdRegistry.INVALID_WORKER_ID;
  }

  /**
   * Configures and starts a master.
   *
   * @param conf configuration of this test
   * @throws IOException when the operation fails
   */
  protected abstract void startMaster(TachyonConf conf) throws IOException;

  /**
   * Configures and starts a worker.
   *
   * @param conf configuration of this test
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  protected abstract void startWorker(TachyonConf conf) throws IOException,
      ConnectionFailedException;

  /**
   * Sets up corresponding directories for tests.
   *
   * @param conf configuration of this test
   * @throws IOException when creating or deleting dirs failed
   */
  protected void setupTest(TachyonConf conf) throws IOException {
    String tachyonHome = conf.get(Constants.TACHYON_HOME);
    // Deletes the tachyon home dir for this test from ufs to avoid permission problems
    UnderFileSystemUtils.deleteDir(tachyonHome, conf);

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.get().
    UnderFileSystemUtils.mkdirIfNotExists(conf.get(Constants.UNDERFS_ADDRESS), conf);

    // Creates storage dirs for worker
    int numLevel = conf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level ++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = conf.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        UnderFileSystemUtils.mkdirIfNotExists(dirPath, conf);
      }
    }

    // Starts the UFS for integration tests. If this is for HDFS profiles, it starts miniDFSCluster
    // (see also {@link tachyon.LocalMiniDFSCluster} and sets up the folder like
    // "hdfs://xxx:xxx/tachyon*".
    mUfsCluster = UnderFileSystemCluster.get(mTachyonHome, conf);

    // Sets the journal folder
    String journalFolder =
        mUfsCluster.getUnderFilesystemAddress() + "/journal" + RANDOM_GENERATOR.nextLong();
    conf.set(Constants.MASTER_JOURNAL_FOLDER, journalFolder);

    // Formats the journal
    UnderFileSystemUtils.mkdirIfNotExists(journalFolder, conf);
    for (String masterServiceName : mMasterServiceNames) {
      UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(journalFolder, masterServiceName),
          conf);
    }
    UnderFileSystemUtils
        .touch(PathUtils.concatPath(journalFolder, "_format_" + System.currentTimeMillis()), conf);

    // If we are using the LocalMiniDFSCluster or S3UnderStorageCluster or OSSUnderStorageCluster,
    // we need to update the UNDERFS_ADDRESS to point to the cluster's current address.
    // This must happen after UFS is started with UnderFileSystemCluster.get().
    // TODO(andrew): Move logic to the integration-tests project so that we can use instanceof here
    // instead of comparing classnames.
    if (mUfsCluster.getClass().getSimpleName().equals("LocalMiniDFSCluster")
        || mUfsCluster.getClass().getSimpleName().equals("S3UnderStorageCluster")
        || mUfsCluster.getClass().getSimpleName().equals("OSSUnderStorageCluster")) {
      String ufsAddress = mUfsCluster.getUnderFilesystemAddress() + mTachyonHome;
      conf.set(Constants.UNDERFS_ADDRESS, ufsAddress);
    }
  }

  /**
   * Stops both the tachyon and underfs service threads.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    stopTFS();
    stopUFS();

    resetContext();
  }

  /**
   * Stops the tachyon filesystem's service thread only.
   *
   * @throws Exception when the operation fails
   */
  public abstract void stopTFS() throws Exception;

  /**
   * Cleans up the underfs cluster test folder only.
   *
   * @throws Exception when the operation fails
   */
  protected void stopUFS() throws Exception {
    LOG.info("stop under storage system");
    if (mUfsCluster != null) {
      mUfsCluster.cleanup();
    }
  }

  /**
   * Creates a default {@link tachyon.conf.TachyonConf} for testing.
   *
   * @return a testing TachyonConf
   * @throws IOException when the operation fails
   */
  public TachyonConf newTestConf() throws IOException {
    TachyonConf testConf = new TachyonConf();
    setTachyonHome();
    setHostname();

    testConf.set(Constants.IN_TEST_MODE, "true");
    testConf.set(Constants.TACHYON_HOME, mTachyonHome);
    testConf.set(Constants.USER_QUOTA_UNIT_BYTES, Integer.toString(mQuotaUnitBytes));
    testConf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    testConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    testConf.set(Constants.MASTER_HOSTNAME, mHostname);
    testConf.set(Constants.MASTER_RPC_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, Integer.toString(1000));
    testConf.set(Constants.MASTER_WORKER_THREADS_MIN, "1");
    testConf.set(Constants.MASTER_WORKER_THREADS_MAX, "100");

    testConf.set(Constants.MASTER_BIND_HOST, mHostname);
    testConf.set(Constants.MASTER_WEB_BIND_HOST, mHostname);

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

    testConf.set(Constants.WORKER_RPC_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.WORKER_DATA_FOLDER, "/datastore");
    testConf.set(Constants.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    testConf.set(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, Integer.toString(15));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MIN, Integer.toString(1));
    testConf.set(Constants.WORKER_WORKER_BLOCK_THREADS_MAX, Integer.toString(2048));
    testConf.set(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS, Integer.toString(2));

    testConf.set(Constants.WORKER_BIND_HOST, mHostname);
    testConf.set(Constants.WORKER_DATA_BIND_HOST, mHostname);
    testConf.set(Constants.WORKER_WEB_BIND_HOST, mHostname);

    // Performs an immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    testConf.set(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Sets up the tiered store
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
   * Runs a worker.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  protected void runWorker() throws IOException, ConnectionFailedException {
    mWorker = new BlockWorker();
    mFileSystemWorker = new FileSystemWorker(mWorker.getBlockDataManager());

    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mFileSystemWorker.start();
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
   * Returns a {@link tachyon.client.file.TachyonFileSystem} client.
   *
   * @return a TachyonFS client
   * @throws IOException when the operation fails
   */
  public abstract TachyonFileSystem getClient() throws IOException;

  /**
   * Gets the master which should be listening for RPC and Web requests.
   */
  protected abstract LocalTachyonMaster getMaster();

  /**
   * Gets master's {@link tachyon.conf.TachyonConf}.
   */
  public TachyonConf getMasterTachyonConf() {
    return mMasterConf;
  }

  /**
   * Resets contexts. This is called when the cluster is stopped.
   */
  protected void resetContext() {}

  /**
   * Sets hostname.
   */
  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  /**
   * Sets tachyon home.
   *
   * @throws IOException when the operation fails
   */
  protected void setTachyonHome() throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
  }

}
