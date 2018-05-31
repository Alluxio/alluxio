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

package alluxio.multi.process;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.cli.Format;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystem.Factory;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.UnavailableException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.master.SingleMasterInquireClient;
import alluxio.master.ZkMasterInquireClient;
import alluxio.network.PortUtils;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.MasterInfo;
import alluxio.zookeeper.RestartableTestingServer;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for starting, stopping, and interacting with an Alluxio cluster where each master and
 * worker runs in its own process.
 *
 * Compared to {@link LocalAlluxioCluster}, {@link MultiProcessCluster} is
 *   - Slower
 *   - Black box testing only (No access to master/worker internals)
 *   - Destructible (You can kill -9 masters/workers)
 *   - More realistic of real deployments
 *
 * Due to the slower speed, [@link LocalAlluxioCluster} should generally be preferred.
 * {@link MultiProcessCluster} is primarily for tests which want to stop or restart servers.
 *
 * The synchronization strategy for this class is to synchronize all public methods.
 */
@ThreadSafe
public final class MultiProcessCluster implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(MultiProcessCluster.class);
  private static final File ARTIFACTS_DIR = new File(Constants.TEST_ARTIFACTS_DIR);
  private static final File TESTS_LOG = new File(Constants.TESTS_LOG);

  private final Map<PropertyKey, String> mProperties;
  private final Map<Integer, Map<PropertyKey, String>> mMasterProperties;
  private final Map<Integer, Map<PropertyKey, String>> mWorkerProperties;
  private final int mNumMasters;
  private final int mNumWorkers;
  private final String mClusterName;
  private final DeployMode mDeployMode;
  /** Closer for closing all resources that must be closed when the cluster is destroyed. */
  private final Closer mCloser;
  private final List<Master> mMasters;
  private final List<Worker> mWorkers;

  /** Base directory for storing configuration and logs. */
  private File mWorkDir;
  /** Addresses of all masters. Should have the same size as {@link #mMasters}. */
  private List<MasterNetAddress> mMasterAddresses;
  private State mState;
  private RestartableTestingServer mCuratorServer;
  /**
   * Tracks whether the test has succeeded. If mSuccess is never updated before {@link #destroy()},
   * the state of the cluster will be saved as a tarball in the artifacts directory.
   */
  private boolean mSuccess;

  private MultiProcessCluster(Map<PropertyKey, String> properties,
      Map<Integer, Map<PropertyKey, String>> masterProperties,
      Map<Integer, Map<PropertyKey, String>> workerProperties,
      int numMasters, int numWorkers, String clusterName, DeployMode mode) {
    mProperties = properties;
    mMasterProperties = masterProperties;
    mWorkerProperties = workerProperties;
    mNumMasters = numMasters;
    mNumWorkers = numWorkers;
    // Add a unique number so that different runs of the same test use different cluster names.
    mClusterName = clusterName + ThreadLocalRandom.current().nextLong();
    mDeployMode = mode;
    mMasters = new ArrayList<>();
    mWorkers = new ArrayList<>();
    mCloser = Closer.create();
    mState = State.NOT_STARTED;
    mSuccess = false;
  }

  /**
   * Starts the cluster, launching all server processes.
   */
  public synchronized void start() throws Exception {
    Preconditions.checkState(mState != State.STARTED, "Cannot start while already started");
    Preconditions.checkState(mState != State.DESTROYED, "Cannot start a destroyed cluster");
    mWorkDir = AlluxioTestDirectory.createTemporaryDirectory(mClusterName);
    mState = State.STARTED;

    mMasterAddresses = generateMasterAddresses(mNumMasters);
    LOG.info("Master addresses: {}", mMasterAddresses);
    switch (mDeployMode) {
      case NON_HA:
        MasterNetAddress masterAddress = mMasterAddresses.get(0);
        mProperties.put(PropertyKey.MASTER_HOSTNAME, masterAddress.getHostname());
        mProperties.put(PropertyKey.MASTER_RPC_PORT, Integer.toString(masterAddress.getRpcPort()));
        mProperties.put(PropertyKey.MASTER_WEB_PORT, Integer.toString(masterAddress.getWebPort()));
        break;
      case ZOOKEEPER_HA:
        mCuratorServer = mCloser.register(
            new RestartableTestingServer(-1, AlluxioTestDirectory.createTemporaryDirectory("zk")));
        mProperties.put(PropertyKey.ZOOKEEPER_ENABLED, "true");
        mProperties.put(PropertyKey.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
        break;
      default:
        throw new IllegalStateException("Unknown deploy mode: " + mDeployMode.toString());
    }

    for (Entry<PropertyKey, String> entry : ConfigurationTestUtils.testConfigurationDefaults(
        NetworkAddressUtils.getLocalHostName(), mWorkDir.getAbsolutePath()).entrySet()) {
      // Don't overwrite explicitly set properties.
      if (!mProperties.containsKey(entry.getKey())) {
        mProperties.put(entry.getKey(), entry.getValue());
      }
    }

    new File(Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS)).mkdirs();
    formatJournal();
    writeConf();

    // Start servers
    LOG.info("Starting alluxio cluster {} with base directory {}", mClusterName,
        mWorkDir.getAbsolutePath());
    for (int i = 0; i < mNumMasters; i++) {
      createMaster(i).start();
    }
    for (int i = 0; i < mNumWorkers; i++) {
      createWorker(i).start();
    }
  }

  /**
   * Kills the primary master.
   *
   * If no master is currently primary, this method blocks until a primary has been elected, then
   * kills it.
   *
   * @param timeoutMs maximum amount of time to wait, in milliseconds
   * @return the ID of the killed master
   */
  public synchronized int waitForAndKillPrimaryMaster(int timeoutMs) {
    final FileSystem fs = getFileSystemClient();
    final MasterInquireClient inquireClient = getMasterInquireClient();
    CommonUtils.waitFor("a primary master to be serving", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          // Make sure the leader is serving.
          fs.getStatus(new AlluxioURI("/"));
          return true;
        } catch (UnavailableException e) {
          return false;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    int primaryRpcPort;
    try {
      primaryRpcPort = inquireClient.getPrimaryRpcAddress().getPort();
    } catch (UnavailableException e) {
      throw new RuntimeException(e);
    }
    // Destroy the master whose RPC port matches the primary RPC port.
    for (int i = 0; i < mMasterAddresses.size(); i++) {
      if (mMasterAddresses.get(i).getRpcPort() == primaryRpcPort) {
        mMasters.get(i).close();
        return i;
      }
    }
    throw new RuntimeException(
        String.format("No master found with RPC port %d. Master addresses: %s", primaryRpcPort,
            mMasterAddresses));
  }

  /**
   * Waits for the number of live nodes in server configuration store
   * reached the number of nodes in this cluster and gets meta master client.
   *
   * @param timeoutMs maximum amount of time to wait, in milliseconds
   * @return  the meta master client
   */
  public synchronized MetaMasterClient waitForAllNodesRegistered(int timeoutMs) {
    final MetaMasterClient metaMasterClient = getMetaMasterClient();
    CommonUtils.waitFor("all nodes registered", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        try {
          MasterInfo masterInfo = metaMasterClient.getMasterInfo(new HashSet<>(Arrays
              .asList(MasterInfo.MasterInfoField.MASTER_ADDRESSES,
                  MasterInfo.MasterInfoField.WORKER_ADDRESSES)));
          int liveNodeNum = masterInfo.getMasterAddresses().size()
              + masterInfo.getWorkerAddresses().size();
          return liveNodeNum == (mNumMasters + mNumWorkers);
        } catch (UnavailableException e) {
          return false;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    return metaMasterClient;
  }

  /**
   * @return a client for interacting with the cluster
   */
  public synchronized FileSystem getFileSystemClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to get an fs client, but state was %s", mState);
    MasterInquireClient inquireClient = getMasterInquireClient();
    return Factory.get(mCloser.register(FileSystemContext.create(null, inquireClient)));
  }

  /**
   * @return a meta master client
   */
  public synchronized MetaMasterClient getMetaMasterClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to get a meta master client, but state was %s", mState);
    return new RetryHandlingMetaMasterClient(new MasterClientConfig()
        .withMasterInquireClient(getMasterInquireClient()));
  }

  /**
   * Informs the cluster that the test succeeded. If this method is never called, the cluster will
   * save a copy of its state during teardown.
   */
  public synchronized void notifySuccess() {
    mSuccess = true;
  }

  /**
   * Copies the work directory to the artifacts folder.
   */
  public synchronized void saveWorkdir() throws IOException {
    Preconditions.checkState(mState == State.STARTED,
        "cluster must be started before you can save its work directory");
    ARTIFACTS_DIR.mkdirs();

    File tarball = new File(mWorkDir.getParentFile(), mWorkDir.getName() + ".tar.gz");
    // Tar up the work directory.
    ProcessBuilder pb =
        new ProcessBuilder("tar", "-czf", tarball.getName(), mWorkDir.getName());
    pb.directory(mWorkDir.getParentFile());
    pb.redirectOutput(Redirect.appendTo(TESTS_LOG));
    pb.redirectError(Redirect.appendTo(TESTS_LOG));
    Process p = pb.start();
    try {
      p.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    // Move tarball to artifacts directory.
    File finalTarball = new File(ARTIFACTS_DIR, tarball.getName());
    FileUtils.moveFile(tarball, finalTarball);
    LOG.info("Saved cluster {} to {}", mClusterName, finalTarball.getAbsolutePath());
  }

  /**
   * Destroys the cluster. It may not be re-started after being destroyed.
   */
  public synchronized void destroy() throws IOException {
    if (mState == State.DESTROYED) {
      return;
    }
    if (!mSuccess) {
      saveWorkdir();
    }
    mCloser.close();
    LOG.info("Destroyed cluster {}", mClusterName);
    mState = State.DESTROYED;
  }

  /**
   * Starts the specified master.
   *
   * @param i the index of the master to start
   */
  public synchronized void startMaster(int i) throws IOException {
    Preconditions.checkState(mState == State.STARTED,
        "Must be in a started state to start masters");
    mMasters.get(i).start();
  }

  /**
   * Starts the specified worker.
   *
   * @param i the index of the worker to start
   */
  public synchronized void startWorker(int i) throws IOException {
    Preconditions.checkState(mState == State.STARTED,
        "Must be in a started state to start workers");
    mWorkers.get(i).start();
  }

  /**
   * @param i the index of the master to stop
   */
  public synchronized void stopMaster(int i) throws IOException {
    mMasters.get(i).close();
  }

  /**
   * @param i the index of the worker to stop
   */
  public synchronized void stopWorker(int i) throws IOException {
    mWorkers.get(i).close();
  }

  /**
   * @return return the list of master addresses
   */
  public synchronized List<MasterNetAddress> getMasterAddresses() {
    return mMasterAddresses;
  }

  /**
   * Stops the Zookeeper cluster.
   */
  public synchronized void stopZk() throws IOException {
    mCuratorServer.stop();
  }

  /**
   * Restarts the Zookeeper cluster.
   */
  public synchronized void restartZk() throws Exception {
    Preconditions.checkNotNull(mCuratorServer, "mCuratorServer");
    mCuratorServer.restart();
  }

  /**
   * Creates the specified master without starting it.
   *
   * @param i the index of the master to create
   */
  private synchronized Master createMaster(int i) throws IOException {
    Preconditions.checkState(mState == State.STARTED,
        "Must be in a started state to create masters");
    MasterNetAddress address = mMasterAddresses.get(i);
    File confDir = new File(mWorkDir, "conf-master" + i);
    File logsDir = new File(mWorkDir, "logs-master" + i);
    logsDir.mkdirs();
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "MASTER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, confDir.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, logsDir.getAbsolutePath());
    conf.put(PropertyKey.MASTER_HOSTNAME, address.getHostname());
    conf.put(PropertyKey.MASTER_RPC_PORT, Integer.toString(address.getRpcPort()));
    conf.put(PropertyKey.MASTER_WEB_PORT, Integer.toString(address.getWebPort()));
    Master master = mCloser.register(new Master(logsDir, conf));
    mMasters.add(master);
    return master;
  }

  /**
   * Creates the specified worker without starting it.
   *
   * @param i the index of the worker to create
   */
  private synchronized Worker createWorker(int i) throws IOException {
    Preconditions.checkState(mState == State.STARTED,
        "Must be in a started state to create workers");
    File confDir = new File(mWorkDir, "conf-worker" + i);
    File logsDir = new File(mWorkDir, "logs-worker" + i);
    File ramdisk = new File(mWorkDir, "ramdisk" + i);
    logsDir.mkdirs();
    ramdisk.mkdirs();
    int rpcPort = PortUtils.getFreePort();
    int dataPort = PortUtils.getFreePort();
    int webPort = PortUtils.getFreePort();

    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "WORKER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, confDir.getAbsolutePath());
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
        ramdisk.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, logsDir.getAbsolutePath());
    conf.put(PropertyKey.WORKER_RPC_PORT, Integer.toString(rpcPort));
    conf.put(PropertyKey.WORKER_DATA_PORT, Integer.toString(dataPort));
    conf.put(PropertyKey.WORKER_WEB_PORT, Integer.toString(webPort));

    Worker worker = mCloser.register(new Worker(logsDir, conf));
    mWorkers.add(worker);
    LOG.info("Created worker with (rpc, data, web) ports ({}, {}, {})", rpcPort, dataPort,
        webPort);
    return worker;
  }

  private void formatJournal() throws Exception {
    try (Closeable c = new ConfigurationRule(PropertyKey.MASTER_JOURNAL_FOLDER,
        mProperties.get(PropertyKey.MASTER_JOURNAL_FOLDER)).toResource()) {
      Format.format(Format.Mode.MASTER);
    }
  }

  private MasterInquireClient getMasterInquireClient() {
    switch (mDeployMode) {
      case NON_HA:
        Preconditions.checkState(mMasters.size() == 1,
            "Running with multiple masters requires Zookeeper to be enabled");
        return new SingleMasterInquireClient(new InetSocketAddress(
            mMasterAddresses.get(0).getHostname(), mMasterAddresses.get(0).getRpcPort()));
      case ZOOKEEPER_HA:
        return ZkMasterInquireClient.getClient(mCuratorServer.getConnectString(),
            Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
      default:
        throw new IllegalStateException("Unknown deploy mode: " + mDeployMode.toString());
    }
  }

  /**
   * Writes the contents of properties to the configuration file.
   */
  private void writeConf() throws IOException {
    for (int i = 0; i < mNumMasters; i++) {
      File confDir = new File(mWorkDir, "conf-master" + i);
      writeConfToFile(confDir, mMasterProperties.getOrDefault(i, new HashMap<>()));
    }
    for (int i = 0; i < mNumWorkers; i++) {
      File confDir = new File(mWorkDir, "conf-worker" + i);
      writeConfToFile(confDir, mWorkerProperties.getOrDefault(i, new HashMap<>()));
    }
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        try {
          destroy();
        } catch (IOException e) {
          LOG.warn("Failed to clean up test cluster processes: {}", e.toString());
        }
      }
    }));
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          start();
          base.evaluate();
        } finally {
          try {
            destroy();
          } catch (Throwable t) {
            LOG.error("Failed to destroy cluster", t);
          }
        }
      }
    };
  }

  /**
   * Creates the conf directory and file.
   * Writes the properties to the generated file.
   *
   * @param dir the conf directory to create
   * @param properties the specific properties of the current node
   */
  private void writeConfToFile(File dir, Map<PropertyKey, String> properties) throws IOException {
    // Generates the full set of properties to write
    Map<PropertyKey, String> map = new HashMap<>(mProperties);
    for (Map.Entry<PropertyKey, String> entry : properties.entrySet()) {
      map.put(entry.getKey(), entry.getValue());
    }

    StringBuilder sb = new StringBuilder();
    for (Entry<PropertyKey, String> entry : map.entrySet()) {
      sb.append(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
    }

    dir.mkdirs();
    try (FileOutputStream fos
        = new FileOutputStream(new File(dir, "alluxio-site.properties"))) {
      fos.write(sb.toString().getBytes(Charsets.UTF_8));
    }
  }

  private static List<MasterNetAddress> generateMasterAddresses(int numMasters) throws IOException {
    List<MasterNetAddress> addrs = new ArrayList<>();
    for (int i = 0; i < numMasters; i++) {
      addrs.add(new MasterNetAddress(NetworkAddressUtils.getLocalHostName(),
          PortUtils.getFreePort(), PortUtils.getFreePort()));
    }
    return addrs;
  }

  private enum State {
    NOT_STARTED, STARTED, DESTROYED;
  }

  /**
   * Deploy mode for the cluster.
   */
  public enum DeployMode {
    NON_HA, ZOOKEEPER_HA
  }

  /**
   * Builder for {@link MultiProcessCluster}.
   */
  public static final class Builder {
    private Map<PropertyKey, String> mProperties = new HashMap<>();
    private Map<Integer, Map<PropertyKey, String>> mMasterProperties = new HashMap<>();
    private Map<Integer, Map<PropertyKey, String>> mWorkerProperties = new HashMap<>();
    private int mNumMasters = 1;
    private int mNumWorkers = 1;
    private String mClusterName = "AlluxioMiniCluster";
    private DeployMode mDeployMode = DeployMode.NON_HA;

    private Builder() {} // Should only be instantiated by newBuilder().

    /**
     * @param key the property key to set
     * @param value the value to set
     * @return the builder
     */
    public Builder addProperty(PropertyKey key, String value) {
      Preconditions.checkState(!key.equals(PropertyKey.ZOOKEEPER_ENABLED),
          "Enable Zookeeper via #setDeployMode instead of #addProperty");
      mProperties.put(key, value);
      return this;
    }

    /**
     * @param properties alluxio properties for launched masters and workers
     * @return the builder
     */
    public Builder addProperties(Map<PropertyKey, String> properties) {
      for (Entry<PropertyKey, String> entry : properties.entrySet()) {
        addProperty(entry.getKey(), entry.getValue());
      }
      return this;
    }

    /**
     * Sets master specific properties.
     * The keys of the properties are the indexes of masters
     * which are numbers between 0 to the number of masters (exclusive).
     *
     * @param properties the master properties to set
     * @return the builder
     */
    public Builder setMasterProperties(Map<Integer, Map<PropertyKey, String>> properties) {
      mMasterProperties = properties;
      return this;
    }

    /**
     * Sets worker specific properties.
     * The keys of the properties are the indexes of workers
     * which are numbers between 0 to the number of workers (exclusive).
     *
     * @param properties the worker properties to set
     * @return the builder
     */
    public Builder setWorkerProperties(Map<Integer, Map<PropertyKey, String>> properties) {
      mWorkerProperties = properties;
      return this;
    }

    /**
     * @param mode the deploy mode for the cluster
     * @return the builder
     */
    public Builder setDeployMode(DeployMode mode) {
      mDeployMode = mode;
      return this;
    }

    /**
     * @param numMasters the number of masters for the cluster
     * @return the builder
     */
    public Builder setNumMasters(int numMasters) {
      mNumMasters = numMasters;
      return this;
    }

    /**
     * @param numWorkers the number of workers for the cluster
     * @return the builder
     */
    public Builder setNumWorkers(int numWorkers) {
      mNumWorkers = numWorkers;
      return this;
    }

    /**
     * @param clusterName a name for the cluster
     * @return the builder
     */
    public Builder setClusterName(String clusterName) {
      mClusterName = clusterName;
      return this;
    }

    /**
     * @return a constructed {@link MultiProcessCluster}
     */
    public MultiProcessCluster build() {
      Preconditions.checkState(mMasterProperties.keySet()
              .stream().filter(key -> (key >= mNumMasters || key < 0)).count() == 0,
          "The master indexes in master properties should be bigger or equal to zero "
              + "and small than %s", mNumMasters);
      Preconditions.checkState(mWorkerProperties.keySet()
              .stream().filter(key ->  (key >= mNumWorkers || key < 0)).count() == 0,
          "The worker indexes in worker properties should be bigger or equal to zero "
              + "and small than %s", mNumWorkers);
      return new MultiProcessCluster(mProperties, mMasterProperties, mWorkerProperties,
          mNumMasters, mNumWorkers, mClusterName, mDeployMode);
    }
  }

  /**
   * @return a new builder for an {@link MultiProcessCluster}
   */
  public static Builder newBuilder() {
    return new Builder();
  }
}
