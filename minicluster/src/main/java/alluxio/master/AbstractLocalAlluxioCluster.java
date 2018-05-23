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
import alluxio.PropertyKey;
import alluxio.cli.Format;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.util.ClientTestUtils;
import alluxio.proxy.ProxyProcess;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.WorkerProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster.
 */
@NotThreadSafe
public abstract class AbstractLocalAlluxioCluster {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractLocalAlluxioCluster.class);

  private static final Random RANDOM_GENERATOR = new Random();

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
    // Reset the file system context to make sure the correct master RPC port is used.
    FileSystemContext.get().reset();
    startWorkers();
    startProxy();

    // Reset contexts so that they pick up the updated configuration.
    reset();
  }

  /**
   * Configures and starts the master(s).
   */
  protected abstract void startMasters() throws Exception;

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
  private void startProxy() throws Exception {
    mProxyProcess = ProxyProcess.Factory.create();
    Runnable runProxy = new Runnable() {
      @Override
      public void run() {
        try {
          mProxyProcess.start();
        } catch (InterruptedException e) {
          // this is expected
        } catch (Exception e) {
          // Log the exception as the RuntimeException will be caught and handled silently by JUnit
          LOG.error("Start proxy error", e);
          throw new RuntimeException(e + " \n Start Proxy Error \n" + e.getMessage(), e);
        }
      }
    };

    mProxyThread = new Thread(runProxy);
    mProxyThread.setName("ProxyThread-" + System.identityHashCode(mProxyThread));
    mProxyThread.start();
    mProxyProcess.waitForReady();
  }

  /**
   * Configures and starts the worker(s).
   */
  public void startWorkers() throws Exception {
    mWorkers = new ArrayList<>();
    for (int i = 0; i < mNumWorkers; i++) {
      mWorkers.add(WorkerProcess.Factory.create());
    }

    for (final WorkerProcess worker : mWorkers) {
      Runnable runWorker = new Runnable() {
        @Override
        public void run() {
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
        }
      };
      Thread thread = new Thread(runWorker);
      thread.setName("WorkerThread-" + System.identityHashCode(thread));
      mWorkerThreads.add(thread);
      thread.start();
    }

    for (WorkerProcess worker : mWorkers) {
      worker.waitForReady();
    }
  }

  /**
   * Sets up corresponding directories for tests.
   */
  protected void setupTest() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot();
    String underfsAddress = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);

    // Deletes the ufs dir for this test from to avoid permission problems
    UnderFileSystemUtils.deleteDirIfExists(ufs, underfsAddress);

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.get().
    UnderFileSystemUtils.mkdirIfNotExists(ufs, underfsAddress);

    // Creates storage dirs for worker
    int numLevel = Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = Configuration.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        FileUtils.createDir(dirPath);
      }
    }

    // Formats the journal
    Format.format(Format.Mode.MASTER);
  }

  /**
   * Stops both the alluxio and underfs service threads.
   */
  public void stop() throws Exception {
    stopFS();
    reset();
    LoginUserTestUtils.resetLoginUser();
    ConfigurationTestUtils.resetConfiguration();
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
   * Creates a default {@link Configuration} for testing.
   */
  public void initConfiguration() throws IOException {
    setAlluxioWorkDirectory();
    setHostname();

    for (Entry<PropertyKey, String> entry : ConfigurationTestUtils
        .testConfigurationDefaults(mHostname, mWorkDirectory).entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }

    Configuration.set(PropertyKey.TEST_MODE, true);
    Configuration.set(PropertyKey.MASTER_RPC_PORT, 0);
    Configuration.set(PropertyKey.MASTER_WEB_PORT, 0);
    Configuration.set(PropertyKey.PROXY_WEB_PORT, 0);
    Configuration.set(PropertyKey.WORKER_RPC_PORT, 0);
    Configuration.set(PropertyKey.WORKER_DATA_PORT, 0);
    Configuration.set(PropertyKey.WORKER_WEB_PORT, 0);
  }

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
   * Resets the cluster to original state.
   */
  protected void reset() {
    ClientTestUtils.resetClient();
    GroupMappingServiceTestUtils.resetCache();
  }

  /**
   * Resets the client pools to the original state.
   */
  protected void resetClientPools() throws IOException {
    FileSystemContext.get().reset();
  }

  /**
   * Sets hostname.
   */
  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  /**
   * Sets Alluxio work directory.
   */
  protected void setAlluxioWorkDirectory() {
    mWorkDirectory =
        AlluxioTestDirectory.createTemporaryDirectory("test-cluster").getAbsolutePath();
  }
}
