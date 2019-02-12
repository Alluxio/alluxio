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
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.cli.Format;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.util.ClientTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.proxy.ProxyProcess;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.WorkerProcess;
import alluxio.worker.WorkerProcess.Factory;
import alluxio.zookeeper.RestartableTestingServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Local Alluxio cluster.
 */
@NotThreadSafe
public final class LocalAlluxioCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalAlluxioCluster.class);

  private final List<LocalAlluxioMaster> mMasters = new ArrayList<>();
  private final List<LocalProcess<WorkerProcess>> mWorkers = new ArrayList<>();
  private final int mNumMasters;
  private final int mNumWorkers;

  private LocalProcess<ProxyProcess> mProxy;

  // Only set when zookeeper is enabled.
  private RestartableTestingServer mCuratorServer;

  private String mWorkDirectory;
  private String mHostname;

  /**
   * Constructs a local alluxio cluster.
   *
   * Callers should use {@link Builder#build()} instead of calling the constructor directly.
   *
   * @param numWorkers the number of workers to run
   * @param numMasters the number of masters to run
   */
  private LocalAlluxioCluster(int numMasters, int numWorkers) {
    mNumMasters = numMasters;
    mNumWorkers = numWorkers;
  }

  private boolean zookeeperEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED);
  }

  /**
   * Starts both master and a worker using the configurations in test conf respectively.
   */
  public void start() throws Exception {
    // Disable HDFS client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    resetClientPools();
    setupTest();
    validateConf();
    startCurator();
    startMasters();
    startWorkers();
    startProxy();

    // Reset contexts so that they pick up the updated configuration.
    reset();
  }

  private void validateConf() {
    if (mNumMasters > 1) {
      Preconditions.checkState(zookeeperEnabled(),
          "Running with multiple masters requires zookeeper to be enabled");
    }
  }

  private void startCurator() {
    if (zookeeperEnabled()) {
      try {
        mCuratorServer =
            new RestartableTestingServer(-1, AlluxioTestDirectory.createTemporaryDirectory("zk"));
        LOG.info("Started testing zookeeper: {}", mCuratorServer.getConnectString());
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Configures and starts the master(s).
   */
  public void startMasters() throws Exception {
    if (zookeeperEnabled()) {
      ServerConfiguration.set(PropertyKey.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
    }
    for (int i = 0; i < mNumMasters; i++) {
      LocalAlluxioMaster master = LocalAlluxioMaster.create(mWorkDirectory);
      master.start();
      LOG.info("master number {} started, address: {}", i, master.getAddress());
      mMasters.add(master);
      if (mNumMasters > 1) {
        // Additional masters should generate new ports for binding
        ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT, "0");
        ServerConfiguration.set(PropertyKey.MASTER_WEB_PORT, "0");
      }
    }
    mMasters.forEach(master -> TestUtils.waitForReady(master.getMasterProcess()));
  }

  /**
   * Configures and starts the proxy.
   */
  private void startProxy() throws Exception {
    mProxy = new LocalProcess<>("Proxy", () -> ProxyProcess.Factory.create());
    mProxy.start();
  }

  /**
   * Restarts the master(s).
   */
  public void restartMasters() throws Exception {
    stopMasters();
    startMasters();
  }
  /**
   * Configures and starts the worker(s).
   */
  public void startWorkers() {
    for (int i = 0; i < mNumWorkers; i++) {
      LocalProcess<WorkerProcess> worker = new LocalProcess<>("Worker", () -> Factory.create());
      mWorkers.add(worker);
    }
    mWorkers.forEach(worker -> worker.start());
    mWorkers.forEach(worker -> worker.waitForReady());
  }

  /**
   * Sets up corresponding directories for tests.
   */
  private void setupTest() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    String underfsAddress = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);

    // Deletes the ufs dir for this test from to avoid permission problems
    UnderFileSystemUtils.deleteDirIfExists(ufs, underfsAddress);

    // Creates ufs dir. This must be called before starting UFS with UnderFileSystemCluster.create()
    UnderFileSystemUtils.mkdirIfNotExists(ufs, underfsAddress);

    // Creates storage dirs for worker
    int numLevel = ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
    for (int level = 0; level < numLevel; level++) {
      PropertyKey tierLevelDirPath =
          PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(level);
      String[] dirPaths = ServerConfiguration.get(tierLevelDirPath).split(",");
      for (String dirPath : dirPaths) {
        FileUtils.createDir(dirPath);
      }
    }

    // Formats the journal
    Format.format(Format.Mode.MASTER, ServerConfiguration.global());
  }

  /**
   * Stops both the alluxio and underfs service threads.
   */
  public void stop() throws Exception {
    stopFS();
    reset();
    LoginUserTestUtils.resetLoginUser();
    ServerConfiguration.reset();
    if (mCuratorServer != null) {
      mCuratorServer.stop();
    }
    System.clearProperty("fs.hdfs.impl.disable.cache");
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
    Format.format(Format.Mode.MASTER, ServerConfiguration.global());
    startMasters();
  }

  /**
   * Stops the masters.
   */
  public void stopMasters() throws Exception {
    for (LocalAlluxioMaster master : mMasters) {
      master.stop();
    }
  }

  /**
   * Stops the proxy.
   */
  public void stopProxy() throws Exception {
    if (mProxy == null) {
      return;
    }
    mProxy.stop();
  }

  /**
   * Stops the workers.
   */
  public void stopWorkers() throws Exception {
    for (LocalProcess<WorkerProcess> worker : mWorkers) {
      worker.stop();
    }
    mWorkers.clear();
  }

  /**
   * Creates a default {@link ServerConfiguration} for testing.
   */
  public void initConfiguration() throws IOException {
    setAlluxioWorkDirectory();
    setHostname();
    for (Map.Entry<PropertyKey, String> entry : ConfigurationTestUtils
        .testConfigurationDefaults(ServerConfiguration.global(), mHostname, mWorkDirectory)
        .entrySet()) {
      ServerConfiguration.set(entry.getKey(), entry.getValue());
    }
    ServerConfiguration.set(PropertyKey.TEST_MODE, true);
    ServerConfiguration.set(PropertyKey.PROXY_WEB_PORT, 0);
  }

  /**
   * @return a {@link FileSystem} client
   */
  public FileSystem getClient() {
    return getFirstMaster().getClient();
  }

  /**
   * @param context the FileSystemContext to use
   * @return a {@link FileSystem} client, using a specific context
   */
  public FileSystem getClient(FileSystemContext context) {
    return getFirstMaster().getClient(context);
  }

  /**
   * @return the first Alluxio master
   */
  public LocalAlluxioMaster getFirstMaster() {
    return mMasters.get(0);
  }

  /**
   * @return the only Alluxio master
   */
  public LocalAlluxioMaster getLocalAlluxioMaster() {
    Preconditions.checkState(mNumMasters == 1,
        "Cannot call getLocalAlluxioMaster when there are multiple masters");
    return getFirstMaster();
  }

  /**
   * @return the URI for the only master
   */
  public String getMasterURI() {
    Preconditions.checkState(mNumMasters == 1,
        "Cannot call getMasterURI when there are multiple masters");
    return getFirstMaster().getUri();
  }

  /**
   * @return the first Alluxio master process
   */
  public AlluxioMasterProcess getFirstMasterProcess() {
    return mMasters.get(0).getMasterProcess();
  }

  /**
   * @return the first Alluxio worker
   */
  public LocalProcess<WorkerProcess> getFirstWorker() {
    return mWorkers.get(0);
  }

  /**
   * @return the only worker process
   */
  public WorkerProcess getWorkerProcess() {
    Preconditions.checkState(mNumWorkers == 1,
        "Cannot call getWorkerProcess when there are multiple workers");
    return getFirstWorker().getProcess();
  }

  /**
   * @return the address of the only worker
   */
  public WorkerNetAddress getWorkerAddress() {
    Preconditions.checkState(mNumWorkers == 1,
        "Cannot call getWorkerAddress when there are multiple workers");
    return getWorkerProcess().getAddress();
  }

  /**
   * @return the leader master, waiting for a leader to be selected if necessary
   */
  public AlluxioMasterProcess getLeaderProcess() throws TimeoutException, InterruptedException {
    waitForNewMaster(Constants.MINUTE_MS);
    for (int i = 0; i < mNumMasters; i++) {
      if (mMasters.get(i).isServing()) {
        return mMasters.get(i).getMasterProcess();
      }
    }
    throw new RuntimeException("Failed to determine the leader master");
  }

  /**
   * @return index of leader master in {@link #mMasters}, or -1 if there is no leader temporarily
   */
  public int getLeaderIndex() {
    for (int i = 0; i < mNumMasters; i++) {
      if (mMasters.get(i).isServing()) {
        return i;
      }
    }
    return -1;
  }

  /**
   * @return the master addresses
   */
  public List<InetSocketAddress> getMasterAddresses() {
    List<InetSocketAddress> addrs = new ArrayList<>();
    for (int i = 0; i < mNumMasters; i++) {
      addrs.add(mMasters.get(i).getAddress());
    }
    return addrs;
  }

  /**
   * @return the master's RPC port
   */
  public int getMasterRpcPort() {
    Preconditions.checkState(mNumMasters == 0,
        "Cannot call getMasterRpcPort with multiple masters");
    return getFirstMasterProcess().getRpcAddress().getPort();
  }

  /**
   * Iterates over the masters in the order of master creation, stops the first standby master.
   *
   * @return true if a standby master is successfully stopped, otherwise, false
   */
  public boolean stopStandby() {
    for (int k = 0; k < mNumMasters; k++) {
      if (!mMasters.get(k).isServing()) {
        try {
          LOG.info("master {} is a standby. stopping it...", k);
          mMasters.get(k).stop();
          LOG.info("master {} stopped.", k);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Iterates over the masters in the order of master creation, stops the leader master.
   *
   * @return true if the leader master is successfully stopped, false otherwise
   */
  public boolean stopLeader() {
    for (int k = 0; k < mNumMasters; k++) {
      if (mMasters.get(k).isServing()) {
        try {
          LOG.info("master {} is the leader. stopping it...", k);
          mMasters.get(k).stop();
          LOG.info("master {} stopped.", k);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          return false;
        }
        return true;
      }
    }
    return false;
  }

  /**
   * Waits for a new master to start until a timeout occurs.
   *
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public void waitForNewMaster(int timeoutMs) throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("the new leader master to start", () -> getLeaderIndex() != -1,
        WaitForOptions.defaults().setTimeoutMs(timeoutMs));
  }

  /**
   * Stops the cluster's Zookeeper service.
   */
  public void stopZk() throws Exception {
    mCuratorServer.stop();
  }

  /**
   * Restarts the cluster's Zookeeper service. It must first be stopped with {@link #stopZk()}.
   */
  public void restartZk() throws Exception {
    mCuratorServer.restart();
  }

  /**
   * @return the proxy process
   */
  public ProxyProcess getProxyProcess() {
    return mProxy.getProcess();
  }

  /**
   * @return the home path to Alluxio
   */
  public String getAlluxioHome() {
    return mWorkDirectory;
  }

  /**
   * @return the hostname of the cluster
   */
  public String getHostname() {
    return mHostname;
  }

  /**
   * Resets the cluster to original state.
   */
  private void reset() {
    ClientTestUtils.resetClient(ServerConfiguration.global());
    GroupMappingServiceTestUtils.resetCache();
  }

  /**
   * Resets the client pools to the original state.
   */
  private void resetClientPools() throws IOException {
    ServerConfiguration.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false);
  }

  /**
   * Sets hostname.
   */
  private void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  /**
   * Sets Alluxio work directory.
   */
  private void setAlluxioWorkDirectory() {
    mWorkDirectory =
        AlluxioTestDirectory.createTemporaryDirectory("test-cluster").getAbsolutePath();
  }

  /**
   * @return a new cluster builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for creating local alluxio clusters.
   */
  public static class Builder {
    private int mNumWorkers = 1;
    private int mNumMasters = 1;

    /**
     * @param numWorkers the number of workers to set
     * @return the updated builder
     */
    public Builder setNumWorkers(int numWorkers) {
      mNumWorkers = numWorkers;
      return this;
    }

    /**
     * @param numMasters the number of masters to set
     * @return the updated builder
     */
    public Builder setNumMasters(int numMasters) {
      mNumMasters = numMasters;
      return this;
    }

    /**
     * @return the built cluster
     */
    public LocalAlluxioCluster build() {
      return new LocalAlluxioCluster(mNumWorkers, mNumMasters);
    }
  }
}
