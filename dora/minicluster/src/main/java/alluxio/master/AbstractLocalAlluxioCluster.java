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
import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.cli.Format;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.util.ClientTestUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.proxy.ProxyProcess;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster.
 */
@NotThreadSafe
public abstract class AbstractLocalAlluxioCluster {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractLocalAlluxioCluster.class);

  private static final Random RANDOM_GENERATOR = new Random();
  private static final int WAIT_MASTER_START_TIMEOUT_MS = 200_000;

  protected ProxyProcess mProxyProcess;
  protected Thread mProxyThread;

  protected List<WorkerProcess> mWorkers;
  protected List<Thread> mWorkerThreads;

  protected String mWorkDirectory;
  protected String mHostname;

  private int mNumWorkers;

  /**
   * @param numWorkers the number of workers to run
   */
  AbstractLocalAlluxioCluster(int numWorkers) {
    mProxyProcess = ProxyProcess.Factory.create();
    mNumWorkers = numWorkers;
    mWorkerThreads = new ArrayList<>();
  }

  /**
   * Starts both master and a worker using the configurations in test conf respectively.
   */
  public void start() throws Exception {
    // Disable HDFS client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    resetClientPools();

    setupTest();
    startMasters();
    startWorkers();
    startProxy();

    // Reset contexts so that they pick up the updated configuration.
    reset();
  }

  /**
   * Configures and starts the master(s).
   */
  protected abstract void startMasters() throws Exception;

  protected void waitForMasterServing() throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("master starts serving RPCs", () -> {
      try {
        getClient().getStatus(new AlluxioURI("/"));
        return true;
      } catch (AlluxioException | AlluxioStatusException e) {
        LOG.error("Failed to get status of /:", e);
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(WAIT_MASTER_START_TIMEOUT_MS));
  }

  /**
   * Restarts the master(s).
   */
  public void restartMasters() throws Exception {
    stopMasters();
    startMasters();
  }

  /**
   * Configures and starts the proxy.
   */
  protected void startProxy() throws Exception {
    mProxyProcess = ProxyProcess.Factory.create();
    Runnable runProxy = () -> {
      try {
        mProxyProcess.start();
      } catch (InterruptedException e) {
        // this is expected
      } catch (Exception e) {
        // Log the exception as the RuntimeException will be caught and handled silently by JUnit
        LOG.error("Start proxy error", e);
        throw new RuntimeException(e + " \n Start Proxy Error \n" + e.getMessage(), e);
      }
    };

    mProxyThread = new Thread(runProxy);
    mProxyThread.setName("ProxyThread-" + System.identityHashCode(mProxyThread));
    mProxyThread.start();
    TestUtils.waitForReady(mProxyProcess);
  }

  /**
   * Configures and starts the worker(s).
   */
  public void startWorkers() throws Exception {
    mWorkers = new ArrayList<>();
    for (int i = 0; i < mNumWorkers; ++i) {
      // If dora is enabled, automatically setting the worker page store and rocksdb dirs.
      if (Configuration.getBoolean(PropertyKey.DORA_ENABLED)) {
        String pageStoreDir = PathUtils.concatPath(mWorkDirectory, "worker" + i);
        Configuration.set(PropertyKey.WORKER_PAGE_STORE_DIRS, pageStoreDir);
        Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR, pageStoreDir);
      }
      WorkerProcess worker = WorkerProcess.Factory.create();
      mWorkers.add(worker);
      Runnable runWorker = () -> {
        try {
          worker.start();
        } catch (InterruptedException e) {
          // this is expected
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by
          // JUnit
          LOG.error("Start worker error", e);
          throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
        }
      };
      Thread thread = new Thread(runWorker);
      thread.setName("WorkerThread-" + System.identityHashCode(thread));
      mWorkerThreads.add(thread);
      thread.start();
    }

    for (WorkerProcess worker : mWorkers) {
      TestUtils.waitForReady(worker);
    }
  }

  /**
   * Restarts workers with the addresses provided, so that the workers can restart with
   * static addresses to simulate a worker restart in the cluster.
   *
   * @param addresses worker addresses to use
   */
  public void restartWorkers(Collection<WorkerNetAddress> addresses) throws Exception {
    // Start the worker one by one, so we avoid updating config while this worker is starting
    for (WorkerNetAddress addr : addresses) {
      Configuration.set(PropertyKey.WORKER_RPC_PORT, addr.getRpcPort());
      Configuration.set(PropertyKey.WORKER_WEB_PORT, addr.getWebPort());
      WorkerProcess worker = WorkerProcess.Factory.create();
      mWorkers.add(worker);

      Runnable runWorker = () -> {
        try {
          worker.start();
        } catch (InterruptedException e) {
          // this is expected
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by
          // JUnit
          LOG.error("Start worker error", e);
          throw new RuntimeException(e + " \n Start Worker Error \n" + e.getMessage(), e);
        }
      };
      Thread thread = new Thread(runWorker);
      thread.setName("WorkerThread-" + System.identityHashCode(thread));
      mWorkerThreads.add(thread);
      thread.start();

      TestUtils.waitForReady(worker);
    }
  }

  /**
   * Sets up corresponding directories for tests.
   */
  protected void setupTest() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(Configuration.global());
    String underfsAddress = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String doraUfsRoot = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    UnderFileSystem doraUfs = UnderFileSystem.Factory.create(doraUfsRoot, Configuration.global());

    // Deletes the ufs dir for this test from to avoid permission problems
    // Do not delete the ufs root if the ufs is not local UFS.
    // In some test cases, S3 and HDFS are used.
    if (ufs.getUnderFSType().equals("local")) {
      UnderFileSystemUtils.deleteDirIfExists(ufs, underfsAddress);
    }
    if (ufs.getUnderFSType().equals("local")) {
      UnderFileSystemUtils.deleteDirIfExists(doraUfs, doraUfsRoot);
    }

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.create()
    UnderFileSystemUtils.mkdirIfNotExists(ufs, underfsAddress);
    UnderFileSystemUtils.mkdirIfNotExists(doraUfs, doraUfsRoot);

    // Creates storage dirs for worker
    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = Configuration.getString(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        FileUtils.createDir(dirPath);
      }
    }

    formatJournal();
  }

  protected void formatJournal() throws IOException {
    // Formats the journal
    Format.format(Format.Mode.MASTER, Configuration.global());
  }

  /**
   * Stops both the alluxio and underfs service threads.
   */
  public void stop() throws Exception {
    stopFS();
    reset();
    Configuration.reloadProperties();
  }

  /**
   * Stops the alluxio filesystem's service thread only.
   */
  public void stopFS() throws Exception {
    LOG.info("stop Alluxio filesystem");
    stopProxy();
    stopWorkers();
    stopMasters();
  }

  /**
   * Stops the masters, formats them, and then restarts them. This is useful if a fresh state is
   * desired, for example when restoring from a backup.
   */
  public void formatAndRestartMasters() throws Exception {
    stopMasters();
    Format.format(Format.Mode.MASTER, Configuration.global());
    startMasters();
  }

  /**
   * Stops the masters.
   */
  protected abstract void stopMasters() throws Exception;

  /**
   * Stops the proxy.
   */
  protected void stopProxy() throws Exception {
    mProxyProcess.stop();
    if (mProxyThread != null) {
      while (mProxyThread.isAlive()) {
        LOG.info("Stopping thread {}.", mProxyThread.getName());
        mProxyThread.interrupt();
        mProxyThread.join(1000);
      }
      mProxyThread = null;
    }
  }

  /**
   * Stops the workers.
   */
  public void stopWorkers() throws Exception {
    killWorkerProcesses();

    // forget all the workers in the master
    LocalAlluxioMaster master = getLocalAlluxioMaster();
    if (master != null) {
      DefaultBlockMaster bm =
          (DefaultBlockMaster) master.getMasterProcess().getMaster(BlockMaster.class);
      bm.forgetAllWorkers();
    }
  }

  /**
   * Kills all worker processes without forgetting them in the master,
   * so we can validate the master mechanism handling dead workers.
   */
  public void killWorkerProcesses() throws Exception {
    if (mWorkers == null) {
      return;
    }
    for (WorkerProcess worker : mWorkers) {
      worker.stop();
    }
    for (Thread thread : mWorkerThreads) {
      while (thread.isAlive()) {
        LOG.info("Stopping thread {}.", thread.getName());
        thread.interrupt();
        thread.join(1000);
      }
    }
    mWorkerThreads.clear();
  }

  /**
   * @return true if the workers are started, and not stopped
   */
  public boolean isStartedWorkers() {
    return !mWorkerThreads.isEmpty();
  }

  /**
   * Creates a default {@link Configuration} for testing.
   *
   * @param name the name of the test/cluster
   */
  public abstract void initConfiguration(String name) throws IOException;

  /**
   * Returns a {@link FileSystem} client.
   *
   * @return a {@link FileSystem} client
   */
  public abstract FileSystem getClient() throws IOException;

  /**
   * @param context the FileSystemContext to use
   * @return a {@link FileSystem} client, using a specific context
   */
  public abstract FileSystem getClient(FileSystemContext context) throws IOException;

  /**
   * @return the local Alluxio master
   */
  protected abstract LocalAlluxioMaster getLocalAlluxioMaster();

  /**
   * Gets the proxy process.
   *
   * @return the proxy
   */
  public ProxyProcess getProxyProcess() {
    return mProxyProcess;
  }

  /**
   * Waits for all workers registered with master.
   *
   * @param timeoutMs the timeout to wait
   */
  public void waitForWorkersRegistered(int timeoutMs)
      throws TimeoutException, InterruptedException, IOException {
    try (MetaMasterClient client =
         new RetryHandlingMetaMasterClient(MasterClientContext
             .newBuilder(ClientContext.create(Configuration.global())).build())) {
      CommonUtils.waitFor("workers registered", () -> {
        try {
          return client.getMasterInfo(Collections.emptySet())
              .getWorkerAddressesList().size() == mNumWorkers;
        } catch (UnavailableException e) {
          return false;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setInterval(200).setTimeoutMs(timeoutMs));
    }
  }

  /**
   * Resets the cluster to original state.
   */
  protected void reset() {
    ClientTestUtils.resetClient(Configuration.modifiableGlobal());
    GroupMappingServiceTestUtils.resetCache();
  }

  /**
   * Resets the client pools to the original state.
   */
  protected void resetClientPools() {
    Configuration.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
  }

  /**
   * Sets hostname.
   */
  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  /**
   * Sets Alluxio work directory.
   *
   * @param name the name of the test/cluster
   */
  protected void setAlluxioWorkDirectory(String name) {
    if (name.contains(",")) {
      String normalizedName = name.replace(",", "_");
      LOG.warn("Alluxio work directory {} contains delimiter ',', renaming it to {}",
          name, normalizedName);
      name = normalizedName;
    }

    mWorkDirectory =
        AlluxioTestDirectory.createTemporaryDirectory(name).getAbsolutePath();
  }
}
