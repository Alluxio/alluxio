/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.exception.ConnectionFailedException;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterPrivateAccess;
import alluxio.security.LoginUser;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.WorkerIdRegistry;

import com.google.common.base.Joiner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster.
 */
@NotThreadSafe
public abstract class AbstractLocalAlluxioCluster {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long CLUSTER_READY_POLL_INTERVAL_MS = 10;
  private static final long CLUSTER_READY_TIMEOUT_MS = 60000;
  private static final String ELLIPSIS = "…";
  private static final Random RANDOM_GENERATOR = new Random();

  protected long mWorkerCapacityBytes;
  protected int mUserBlockSize;

  protected Configuration mMasterConf;
  protected Configuration mWorkerConf;

  protected AlluxioWorker mWorker;
  protected UnderFileSystemCluster mUfsCluster;

  protected String mHome;
  protected String mHostname;

  protected Thread mWorkerThread;

  /**
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param userBlockSize the block size for a user
   */
  public AbstractLocalAlluxioCluster(long workerCapacityBytes, int userBlockSize) {
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
   * @param conf the configuration for Alluxio
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start(Configuration conf) throws IOException, ConnectionFailedException {
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
   * {@link #CLUSTER_READY_POLL_INTERVAL_MS}ms.
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
    long workerId = WorkerIdRegistry.getWorkerId();
    if (workerId == WorkerIdRegistry.INVALID_WORKER_ID) {
      return false;
    }
    BlockMaster blockMaster = PrivateAccess.getBlockMaster(getMaster().getInternalMaster());
    return BlockMasterPrivateAccess.isWorkerRegistered(blockMaster, workerId);
  }

  /**
   * Configures and starts a master.
   *
   * @param conf configuration of this test
   * @throws IOException when the operation fails
   */
  protected abstract void startMaster(Configuration conf) throws IOException;

  /**
   * Configures and starts a worker.
   *
   * @param conf configuration of this test
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  protected abstract void startWorker(Configuration conf) throws IOException,
      ConnectionFailedException;

  /**
   * Sets up corresponding directories for tests.
   *
   * @param conf configuration of this test
   * @throws IOException when creating or deleting dirs failed
   */
  protected void setupTest(Configuration conf) throws IOException {
    String alluxioHome = conf.get(Constants.HOME);

    // Deletes the alluxio home dir for this test from ufs to avoid permission problems
    UnderFileSystemUtils.deleteDir(alluxioHome, conf);

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.get().
    UnderFileSystemUtils.mkdirIfNotExists(conf.get(Constants.UNDERFS_ADDRESS), conf);

    // Creates storage dirs for worker
    int numLevel = conf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = conf.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        UnderFileSystemUtils.mkdirIfNotExists(dirPath, conf);
      }
    }

    // Starts the UFS for integration tests. If this is for HDFS profiles, it starts miniDFSCluster
    // (see also {@link alluxio.LocalMiniDFSCluster} and sets up the folder like
    // "hdfs://xxx:xxx/alluxio*".
    mUfsCluster = UnderFileSystemCluster.get(mHome, conf);

    // Sets the journal folder
    String journalFolder =
        mUfsCluster.getUnderFilesystemAddress() + "/journal" + RANDOM_GENERATOR.nextLong();
    conf.set(Constants.MASTER_JOURNAL_FOLDER, journalFolder);

    // Formats the journal
    UnderFileSystemUtils.mkdirIfNotExists(journalFolder, conf);
    for (String masterServiceName : AlluxioMaster.getServiceNames()) {
      UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(journalFolder, masterServiceName),
          conf);
    }
    UnderFileSystemUtils
        .touch(PathUtils.concatPath(journalFolder, "_format_" + System.currentTimeMillis()), conf);

    // If we are using the LocalMiniDFSCluster or S3UnderStorageCluster or OSSUnderStorageCluster,
    // we need to update the UNDERFS_ADDRESS to point to the cluster's current address.
    // This must happen after UFS is started with UnderFileSystemCluster.get().
    // TODO(andrew): Move logic to the alluxio-tests module so that we can use instanceof here
    // instead of comparing classnames.
    if (mUfsCluster.getClass().getSimpleName().equals("LocalMiniDFSCluster")
        || mUfsCluster.getClass().getSimpleName().equals("S3UnderStorageCluster")
        || mUfsCluster.getClass().getSimpleName().equals("OSSUnderStorageCluster")) {
      String ufsAddress = mUfsCluster.getUnderFilesystemAddress() + mHome;
      conf.set(Constants.UNDERFS_ADDRESS, ufsAddress);
    }
  }

  /**
   * Stops both the alluxio and underfs service threads.
   *
   * @throws Exception when the operation fails
   */
  public void stop() throws Exception {
    stopFS();
    stopUFS();

    resetContext();
    resetLoginUser();
  }

  /**
   * Stops the alluxio filesystem's service thread only.
   *
   * @throws Exception when the operation fails
   */
  public abstract void stopFS() throws Exception;

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
   * Resets the {@link LoginUser}. This is called when the cluster is stopped.
   *
   * @throws Exception when the operation fails
   */
  private void resetLoginUser() throws Exception {
    // Use reflection to reset the private static member sLoginUser in LoginUser.
    Field field = LoginUser.class.getDeclaredField("sLoginUser");
    field.setAccessible(true);
    field.set(null, null);
  }

  /**
   * Creates a default {@link Configuration} for testing.
   *
   * @return a test configuration
   * @throws IOException when the operation fails
   */
  public Configuration newTestConf() throws IOException {
    Configuration testConf = new Configuration();
    setAlluxioHome();
    setHostname();

    testConf.set(Constants.IN_TEST_MODE, "true");
    testConf.set(Constants.HOME, mHome);
    testConf.set(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    testConf.set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    testConf.set(Constants.MASTER_HOSTNAME, mHostname);
    testConf.set(Constants.MASTER_RPC_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_WEB_PORT, Integer.toString(0));
    testConf.set(Constants.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.toString(1000));
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
        PathUtils.concatPath(System.getProperty("user.dir"), "../core/server/src/main/webapp"));

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default alluxio storage is STORE, and under storage is SYNC_PERSIST for tests.
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
    String ramdiskPath = PathUtils.concatPath(mHome, "ramdisk");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, 0),
        ramdiskPath);
    testConf.set(String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        Long.toString(mWorkerCapacityBytes));

    int numLevel = testConf.getInt(Constants.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level++) {
      String tierLevelDirPath =
          String.format(Constants.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT, level);
      String[] dirPaths = testConf.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<String>();
      for (String dirPath : dirPaths) {
        String newPath = mHome + dirPath;
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
    mWorker = new AlluxioWorker();
    Whitebox.setInternalState(AlluxioWorker.class, "sAlluxioWorker", mWorker);

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
   * Returns a {@link FileSystem} client.
   *
   * @return a {@link FileSystem} client
   * @throws IOException when the operation fails
   */
  public abstract FileSystem getClient() throws IOException;

  /**
   * Gets the master which should be listening for RPC and Web requests.
   */
  protected abstract LocalAlluxioMaster getMaster();

  /**
   * @return the master's {@link Configuration}
   */
  public Configuration getMasterConf() {
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
   * Sets alluxio home.
   *
   * @throws IOException when the operation fails
   */
  protected void setAlluxioHome() throws IOException {
    mHome =
        File.createTempFile("Alluxio", "U" + System.currentTimeMillis()).getAbsolutePath();
  }

}
