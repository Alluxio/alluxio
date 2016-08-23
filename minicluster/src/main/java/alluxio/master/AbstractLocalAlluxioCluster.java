/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.AlluxioTestDirectory;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.PropertyKeyFormat;
import alluxio.client.file.FileSystem;
import alluxio.client.util.ClientTestUtils;
import alluxio.exception.ConnectionFailedException;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.underfs.LocalFileSystemCluster;
import alluxio.underfs.UnderFileSystemCluster;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.AlluxioWorkerService;
import alluxio.worker.DefaultAlluxioWorker;

import com.google.common.base.Joiner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  protected AlluxioWorkerService mWorker;
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
   * Starts both master and a worker using the configurations in test conf respectively.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start() throws IOException, ConnectionFailedException {
    // Disable HDFS client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    setupTest();
    startMaster();
    getMaster().getInternalMaster().waitForReady();
    startWorker();
    mWorker.waitForReady();

    // Reset contexts so that they pick up the master and worker configuration.
    reset();
  }

  /**
   * Configures and starts a master.
   *
   * @throws IOException when the operation fails
   */
  protected abstract void startMaster() throws IOException;

  /**
   * Configures and starts a worker.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  protected abstract void startWorker() throws IOException, ConnectionFailedException;

  /**
   * Sets up corresponding directories for tests.
   *
   * @throws IOException when creating or deleting dirs failed
   */
  protected void setupTest() throws IOException {
    String alluxioHome = Configuration.get(PropertyKey.HOME);

    // Deletes the alluxio home dir for this test from ufs to avoid permission problems
    UnderFileSystemUtils.deleteDir(alluxioHome);

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.get().
    UnderFileSystemUtils.mkdirIfNotExists(Configuration.get(PropertyKey.UNDERFS_ADDRESS));

    // Creates storage dirs for worker
    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(level);
      String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        UnderFileSystemUtils.mkdirIfNotExists(dirPath);
      }
    }

    // Starts the UFS for integration tests. If this is for HDFS profiles, it starts miniDFSCluster
    // (see also {@link alluxio.LocalMiniDFSCluster} and sets up the folder like
    // "hdfs://xxx:xxx/alluxio*".
    mUfsCluster = UnderFileSystemCluster.get(mHome);

    // Sets the journal folder
    String journalFolder =
        mUfsCluster.getUnderFilesystemAddress() + "/journal" + RANDOM_GENERATOR.nextLong();
    Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, journalFolder);

    // Formats the journal
    UnderFileSystemUtils.mkdirIfNotExists(journalFolder);
    for (String masterServiceName : AlluxioMaster.getServiceNames()) {
      UnderFileSystemUtils.mkdirIfNotExists(PathUtils.concatPath(journalFolder, masterServiceName));
    }
    UnderFileSystemUtils
        .touch(PathUtils.concatPath(journalFolder, "_format_" + System.currentTimeMillis()));

    // If we are using anything except LocalFileSystemCluster as UnderFS,
    // we need to update the UNDERFS_ADDRESS to point to the cluster's current address.
    // This must happen after UFS is started with UnderFileSystemCluster.get().
    if (!mUfsCluster.getClass().getName().equals(LocalFileSystemCluster.class.getName())) {
      String ufsAddress = mUfsCluster.getUnderFilesystemAddress() + mHome;
      Configuration.set(PropertyKey.UNDERFS_ADDRESS, ufsAddress);
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
    ConfigurationTestUtils.resetConfiguration();
    reset();
    LoginUserTestUtils.resetLoginUser();
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
   * Creates a default {@link Configuration} for testing.
   *
   * @throws IOException when the operation fails
   */
  public void initConfiguration() throws IOException {
    setAlluxioHome();
    setHostname();

    Configuration.set(PropertyKey.TEST_MODE, "true");
    Configuration.set(PropertyKey.HOME, mHome);
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, Integer.toString(mUserBlockSize));
    Configuration.set(PropertyKey.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, Integer.toString(64));
    Configuration.set(PropertyKey.MASTER_HOSTNAME, mHostname);
    Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.MASTER_WEB_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Integer.toString(1000));
    Configuration.set(PropertyKey.MASTER_WORKER_THREADS_MIN, "1");
    Configuration.set(PropertyKey.MASTER_WORKER_THREADS_MAX, "100");

    Configuration.set(PropertyKey.MASTER_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.MASTER_WEB_BIND_HOST, mHostname);

    // If tests fail to connect they should fail early rather than using the default ridiculously
    // high retries
    Configuration.set(PropertyKey.MASTER_RETRY, "3");

    // Since tests are always running on a single host keep the resolution timeout low as otherwise
    // people running with strange network configurations will see very slow tests
    Configuration.set(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS, "250");

    Configuration.set(PropertyKey.WEB_THREADS, "1");
    Configuration.set(PropertyKey.WEB_RESOURCES,
        PathUtils.concatPath(System.getProperty("user.dir"), "../core/server/src/main/webapp"));

    // default write type becomes MUST_CACHE, set this value to CACHE_THROUGH for tests.
    // default alluxio storage is STORE, and under storage is SYNC_PERSIST for tests.
    // TODO(binfan): eliminate this setting after updating integration tests
    Configuration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH");

    Configuration.set(PropertyKey.WORKER_RPC_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.WORKER_DATA_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.WORKER_WEB_PORT, Integer.toString(0));
    Configuration.set(PropertyKey.WORKER_DATA_FOLDER, "/datastore");
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, Long.toString(mWorkerCapacityBytes));
    Configuration.set(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS, Integer.toString(15));
    Configuration.set(PropertyKey.WORKER_BLOCK_THREADS_MIN, Integer.toString(1));
    Configuration.set(PropertyKey.WORKER_BLOCK_THREADS_MAX, Integer.toString(2048));
    Configuration.set(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS, Integer.toString(2));

    Configuration.set(PropertyKey.WORKER_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.WORKER_DATA_BIND_HOST, mHostname);
    Configuration.set(PropertyKey.WORKER_WEB_BIND_HOST, mHostname);

    // Performs an immediate shutdown of data server. Graceful shutdown is unnecessary and slow
    Configuration.set(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD, Integer.toString(0));
    Configuration.set(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT, Integer.toString(0));

    // Sets up the tiered store
    String ramdiskPath = PathUtils.concatPath(mHome, "ramdisk");
    Configuration.set(
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT.format(0), "MEM");
    Configuration.set(
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(0),
        ramdiskPath);
    Configuration.set(
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_QUOTA_FORMAT.format(0),
        Long.toString(mWorkerCapacityBytes));

    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 1; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(level);
      String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
      List<String> newPaths = new ArrayList<>();
      for (String dirPath : dirPaths) {
        String newPath = mHome + dirPath;
        newPaths.add(newPath);
      }
      Configuration.set(
          PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_DIRS_PATH_FORMAT.format(level),
          Joiner.on(',').join(newPaths));
    }

    // For some test profiles, default properties get overwritten by system properties (e.g., s3
    // credentials for s3Test).
    // TODO(binfan): have one dedicated property (e.g., alluxio.test.properties) to carry on all the
    // properties we want to overwrite in tests, rather than simply merging all system properties.
    Configuration.merge(System.getProperties());
  }

  /**
   * Runs a worker.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  protected void runWorker() throws IOException, ConnectionFailedException {
    mWorker = new DefaultAlluxioWorker();
    Whitebox.setInternalState(AlluxioWorkerService.Factory.class, "sAlluxioWorker", mWorker);

    Runnable runWorker = new Runnable() {
      @Override
      public void run() {
        try {
          mWorker.start();

        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start worker error", e);
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
   * Resets the cluster to original state.
   */
  protected void reset() {
    ClientTestUtils.resetClient();
    GroupMappingServiceTestUtils.resetCache();
  }

  /**
   * Sets hostname.
   */
  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  /**
   * Sets alluxio home.
   */
  protected void setAlluxioHome() {
    mHome = AlluxioTestDirectory.createTemporaryDirectory("test-cluster").getAbsolutePath();
  }
}
