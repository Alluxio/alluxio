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
import alluxio.ClientContext;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.cli.Format;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystem.Factory;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.client.journal.JournalMasterClient;
import alluxio.client.journal.RetryHandlingJournalMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.MasterInfo;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.master.SingleMasterInquireClient;
import alluxio.master.ZkMasterInquireClient;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.PortCoordination.ReservedPort;
import alluxio.network.PortUtils;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import net.bytebuddy.utility.RandomString;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.lang.Process;

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
public final class MultiProcessCluster {
  public static final String ALLUXIO_USE_FIXED_TEST_PORTS = "ALLUXIO_USE_FIXED_TEST_PORTS";
  public static final int PORTS_PER_MASTER = 3;
  public static final int PORTS_PER_WORKER = 3;

  private static final Logger LOG = LoggerFactory.getLogger(MultiProcessCluster.class);
  private static final File ARTIFACTS_DIR = new File(Constants.TEST_ARTIFACTS_DIR);
  private static final File TESTS_LOG = new File(Constants.TESTS_LOG);
  private static final int WAIT_MASTER_SERVING_TIMEOUT_MS = 30000;

  private final Map<PropertyKey, String> mProperties;
  private final Map<Integer, Map<PropertyKey, String>> mMasterProperties;
  private final Map<Integer, Map<PropertyKey, String>> mWorkerProperties;
  private int mNumMasters;
  private final int mNumWorkers;
  private final String mClusterName;
  /** Closer for closing all resources that must be closed when the cluster is destroyed. */
  private final Closer mCloser;
  private final List<Master> mMasters;
  private final List<Worker> mWorkers;
  private final List<ReservedPort> mPorts;
  private final boolean mNoFormat;

  private DeployMode mDeployMode;

  /** Base directory for storing configuration and logs. */
  private File mWorkDir;
  /** Addresses of all masters. Should have the same size as {@link #mMasters}. */
  private List<MasterNetAddress> mMasterAddresses;
  /** Unique IDs for all masters. Should have the same size as {@link #mMasters} */
  private List<String> mMasterIds = new ArrayList<>();
  private State mState;
  private TestingServer mCuratorServer;
  private FileSystemContext mFilesystemContext;
  /**
   * Tracks whether the test has succeeded. If mSuccess is never updated before {@link #destroy()},
   * the state of the cluster will be saved as a tarball in the artifacts directory.
   */
  private boolean mSuccess;

  private MultiProcessCluster(Map<PropertyKey, String> properties,
      Map<Integer, Map<PropertyKey, String>> masterProperties,
      Map<Integer, Map<PropertyKey, String>> workerProperties, int numMasters, int numWorkers,
      String clusterName, boolean noFormat,
      List<PortCoordination.ReservedPort> ports) {
    if (System.getenv(ALLUXIO_USE_FIXED_TEST_PORTS) != null) {
      Preconditions.checkState(
          ports.size() >= numMasters * PORTS_PER_MASTER + numWorkers * PORTS_PER_WORKER,
          "We require %s ports per master and %s ports per worker, but there are %s masters, "
              + "%s workers, and %s ports",
          PORTS_PER_MASTER, PORTS_PER_WORKER, numMasters, numWorkers, ports.size());
    }
    mProperties = properties;
    mMasterProperties = masterProperties;
    mWorkerProperties = workerProperties;
    mNumMasters = numMasters;
    mNumWorkers = numWorkers;
    // Add a unique number so that different runs of the same test use different cluster names.
    mClusterName = clusterName + "-" + Math.abs(ThreadLocalRandom.current().nextInt());
    mNoFormat = noFormat;
    mMasters = new ArrayList<>();
    mWorkers = new ArrayList<>();
    mPorts = new ArrayList<>(ports);
    mCloser = Closer.create();
    mState = State.NOT_STARTED;
    mSuccess = false;

    String journalType = mProperties.getOrDefault(PropertyKey.MASTER_JOURNAL_TYPE,
        ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_TYPE));
    mDeployMode = journalType.equals(JournalType.EMBEDDED.toString()) ? DeployMode.EMBEDDED
        : numMasters > 1 ? DeployMode.ZOOKEEPER_HA : DeployMode.UFS_NON_HA;
  }

  /**
   * Starts the cluster, launching all server processes.
   */
  public synchronized void start() throws Exception {
    Preconditions.checkState(mState != State.STARTED, "Cannot start while already started");
    Preconditions.checkState(mState != State.DESTROYED, "Cannot start a destroyed cluster");
    mWorkDir = AlluxioTestDirectory.createTemporaryDirectory(mClusterName);
    mState = State.STARTED;
    // Start servers
    LOG.info("Starting alluxio cluster {} with base directory {}", mClusterName,
        mWorkDir.getAbsolutePath());
    startNewMasters(mNumMasters, !mNoFormat);

    for (int i = 0; i < mNumWorkers; i++) {
      createWorker(i).start();
    }
    LOG.info("Starting alluxio cluster in directory {}", mWorkDir.getAbsolutePath());
    int primaryMasterIndex = getPrimaryMasterIndex(WAIT_MASTER_SERVING_TIMEOUT_MS);
    LOG.info("Alluxio primary master {} starts serving RPCs",
        mMasterAddresses.get(primaryMasterIndex));
  }

  /**
   * Start a number of new master nodes.
   *
   * @param count the count of the masters to start
   * @param format whether to format the master first
   * @throws Exception if any error occurs
   */
  public synchronized void startNewMasters(int count, boolean format) throws Exception {
    int startIndex = 0;
    if (mMasterAddresses != null) {
      startIndex = mMasterAddresses.size();
    } else {
      mMasterAddresses = new ArrayList<>();
    }
    List<MasterNetAddress> masterAddresses = generateMasterAddresses(count);
    for (MasterNetAddress newMasterAddress : masterAddresses) {
      String id = newMasterAddress.getEmbeddedJournalPort()
          + "-" + RandomString.make(RandomString.DEFAULT_LENGTH);
      mMasterIds.add(id);
    }
    mMasterAddresses.addAll(masterAddresses);
    mNumMasters = mMasterAddresses.size();

    LOG.info("Master addresses: {}", mMasterAddresses);
    switch (mDeployMode) {
      case UFS_NON_HA:
        MasterNetAddress masterAddress = mMasterAddresses.get(0);
        mProperties.put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString());
        mProperties.put(PropertyKey.MASTER_HOSTNAME, masterAddress.getHostname());
        mProperties.put(PropertyKey.MASTER_RPC_PORT, Integer.toString(masterAddress.getRpcPort()));
        mProperties.put(PropertyKey.MASTER_WEB_PORT, Integer.toString(masterAddress.getWebPort()));
        break;
      case EMBEDDED:
        List<String> journalAddresses = new ArrayList<>();
        List<String> rpcAddresses = new ArrayList<>();
        for (MasterNetAddress address : mMasterAddresses) {
          journalAddresses
              .add(String.format("%s:%d", address.getHostname(), address.getEmbeddedJournalPort()));
          rpcAddresses.add(String.format("%s:%d", address.getHostname(), address.getRpcPort()));
        }
        mProperties.put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString());
        mProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
            com.google.common.base.Joiner.on(",").join(journalAddresses));
        mProperties.put(PropertyKey.MASTER_RPC_ADDRESSES,
            com.google.common.base.Joiner.on(",").join(rpcAddresses));
        break;
      case ZOOKEEPER_HA:
        mCuratorServer = mCloser.register(
                new TestingServer(-1, AlluxioTestDirectory.createTemporaryDirectory("zk")));
        mProperties.put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString());
        mProperties.put(PropertyKey.ZOOKEEPER_ENABLED, "true");
        mProperties.put(PropertyKey.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
        break;
      default:
        throw new IllegalStateException("Unknown deploy mode: " + mDeployMode.toString());
    }

    for (Entry<PropertyKey, String> entry :
        ConfigurationTestUtils.testConfigurationDefaults(ServerConfiguration.global(),
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
            mWorkDir.getAbsolutePath()).entrySet()) {
      // Don't overwrite explicitly set properties.
      if (mProperties.containsKey(entry.getKey())) {
        continue;
      }
      // Keep the default RPC timeout.
      if (entry.getKey().equals(PropertyKey.USER_RPC_RETRY_MAX_DURATION)) {
        continue;
      }
      mProperties.put(entry.getKey(), entry.getValue());
    }
    mProperties.put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
        PathUtils.concatPath(mWorkDir, "underFSStorage"));
    new File(mProperties.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS)).mkdirs();
    if (format) {
      formatJournal();
    }
    writeConf();
    ServerConfiguration.merge(mProperties, Source.RUNTIME);

    for (int i = 0; i < count; i++) {
      createMaster(startIndex + i).start();
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
  public synchronized int waitForAndKillPrimaryMaster(int timeoutMs)
      throws TimeoutException, InterruptedException {
    int index = getPrimaryMasterIndex(timeoutMs);
    mMasters.get(index).close();
    return index;
  }

  /**
   * Gets the index of the primary master.
   *
   * @param timeoutMs maximum amount of time to wait, in milliseconds
   * @return the index of the primary master
   */
  public synchronized int getPrimaryMasterIndex(int timeoutMs)
      throws TimeoutException, InterruptedException {
    final FileSystem fs = getFileSystemClient();
    final MasterInquireClient inquireClient = getMasterInquireClient();
    CommonUtils.waitFor("a primary master to be serving", () -> {
      try {
        // Make sure the leader is serving.
        fs.getStatus(new AlluxioURI("/"));
        return true;
      } catch (Exception e) {
        LOG.error("Failed to get status of root directory:", e);
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    int primaryRpcPort;
    try {
      primaryRpcPort = inquireClient.getPrimaryRpcAddress().getPort();
    } catch (UnavailableException e) {
      throw new RuntimeException(e);
    }
    // Returns the master whose RPC port matches the primary RPC port.
    for (int i = 0; i < mMasterAddresses.size(); i++) {
      if (mMasterAddresses.get(i).getRpcPort() == primaryRpcPort) {
        return i;
      }
    }
    throw new RuntimeException(
        String.format("No master found with RPC port %d. Master addresses: %s", primaryRpcPort,
            mMasterAddresses));
  }

  /**
   * Waits for all nodes to be registered.
   *
   * @param timeoutMs maximum amount of time to wait, in milliseconds
   */
  public synchronized void waitForAllNodesRegistered(int timeoutMs)
      throws TimeoutException, InterruptedException {
    MetaMasterClient metaMasterClient = getMetaMasterClient();
    int nodeCount = mNumMasters + mNumWorkers;
    CommonUtils.waitFor("all nodes registered", () -> {
      try {
        MasterInfo masterInfo = metaMasterClient.getMasterInfo(Collections.emptySet());
        int liveNodeNum = masterInfo.getMasterAddressesList().size()
            + masterInfo.getWorkerAddressesList().size();
        if (liveNodeNum == nodeCount) {
          return true;
        } else {
          LOG.info("Master addresses: {}. Worker addresses: {}",
              masterInfo.getMasterAddressesList(), masterInfo.getWorkerAddressesList());
          return false;
        }
      } catch (UnavailableException e) {
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setInterval(200).setTimeoutMs(timeoutMs));
  }

  /**
   * @return the deploy mode
   */
  public synchronized DeployMode getDeployMode() {
    return mDeployMode;
  }

  /**
   * @return the {@link FileSystemContext} which can be used to access the cluster
   */
  public synchronized FileSystemContext getFilesystemContext() {
    if (mFilesystemContext == null) {
      mFilesystemContext = FileSystemContext.create(null, getMasterInquireClient(),
          ServerConfiguration.global());
      mCloser.register(mFilesystemContext);
    }
    return mFilesystemContext;
  }

  /**
   * @return a client for interacting with the cluster
   */
  public synchronized FileSystem getFileSystemClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to create an fs client, but state was %s", mState);

    return Factory.create(getFilesystemContext());
  }

  /**
   * @return a meta master client
   */
  public synchronized MetaMasterClient getMetaMasterClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to create a meta master client, but state was %s", mState);
    return new RetryHandlingMetaMasterClient(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global()))
        .setMasterInquireClient(getMasterInquireClient())
        .build());
  }

  /**
   * @return a metrics master client
   */
  public synchronized MetricsMasterClient getMetricsMasterClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to create a metrics master client, but state was %s", mState);
    return new RetryHandlingMetricsMasterClient(MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global()))
        .setMasterInquireClient(getMasterInquireClient())
        .build());
  }

  /**
   * @return a journal master client for alluxio master
   */
  public synchronized JournalMasterClient getJournalMasterClientForMaster() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to create a journal master client, but state was %s", mState);

    return new RetryHandlingJournalMasterClient(
        MasterClientContext.newBuilder(ClientContext.create(ServerConfiguration.global()))
            .setMasterInquireClient(getMasterInquireClient()).build());
  }

  /**
   * @return clients for communicating with the cluster
   */
  public synchronized Clients getClients() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to create a meta master client, but state was %s", mState);
    MasterClientContext config = MasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global()))
        .setMasterInquireClient(getMasterInquireClient()).build();
    return new Clients(getFileSystemClient(),
        new RetryHandlingFileSystemMasterClient(config),
        new RetryHandlingMetaMasterClient(config),
        new RetryHandlingBlockMasterClient(config));
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
    ServerConfiguration.reset();
    LOG.info("Destroyed cluster {}", mClusterName);
    mState = State.DESTROYED;
  }

  /**
   * Starts all masters.
   */
  public synchronized void startMasters() {
    mMasters.forEach(master -> master.start());
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
   * Stops all masters.
   */
  public synchronized void stopMasters() {
    mMasters.forEach(master -> master.close());
  }

  /**
   * @param i the index of the master to stop
   */
  public synchronized void stopMaster(int i) throws IOException {
    mMasters.get(i).close();
  }

  /**
   * Removes master i from the cluster.
   *
   * @param i index of the master to be removed
   * @throws IOException
   */
  public synchronized void stopAndRemoveMaster(int i) throws IOException {
    stopMaster(i);
    mMasterAddresses.remove(i);
    mMasterIds.remove(i);
    mMasters.remove(i);
    mMasterProperties.remove(i);
    mNumMasters--;
    switch (mDeployMode) {
      case EMBEDDED:
        String journalAddresses = mMasterAddresses.stream()
            .map(addr -> String.format("%s:%d", addr.getHostname(), addr.getEmbeddedJournalPort()))
            .collect(Collectors.joining(","));
        String rpcAddresses = mMasterAddresses.stream()
            .map(addr -> String.format("%s:%d", addr.getHostname(), addr.getRpcPort()))
            .collect(Collectors.joining(","));
        mProperties.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, journalAddresses);
        mProperties.put(PropertyKey.MASTER_RPC_ADDRESSES, rpcAddresses);
        ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, journalAddresses);
        ServerConfiguration.set(PropertyKey.MASTER_RPC_ADDRESSES, rpcAddresses);
        break;
      case ZOOKEEPER_HA: // zk will take care of fault tolerance
        break;
      default: // UFS_NON_HA: can't remove the only master in the cluster
        throw new IllegalStateException("Unimplemented deploy mode: " + mDeployMode);
    }
    // reset file system context after cluster size shrinks
    mFilesystemContext = null;
  }

  /**
   * Updates master configuration for all masters. This will take effect on a master the next time
   * the master is started.
   *
   * @param key the key to update
   * @param value the value to set, or null to unset the key
   */
  public synchronized void updateMasterConf(PropertyKey key, @Nullable String value) {
    mMasters.forEach(master -> master.updateConf(key, value));
  }

  /**
   * Updates internal master list with an external address.
   *
   * This API is to provide support when this cluster is joined by external masters.
   * Calling this API with an external master address will make this cluster aware of it
   * when creating clients/contexts etc.
   *
   * @param externalMasterAddress external master address
   */
  public synchronized void addExternalMasterAddress(MasterNetAddress externalMasterAddress) {
    mMasterAddresses.add(externalMasterAddress);
    // Reset current context to cause reinstantiation with new addresses.
    mFilesystemContext = null;
  }

  /**
   * Updates the cluster's deploy mode.
   *
   * @param mode the mode to set
   */
  public synchronized void updateDeployMode(DeployMode mode) {
    mDeployMode = mode;
    if (mDeployMode == DeployMode.EMBEDDED) {
      // Ensure that the journal properties are set correctly.
      for (int i = 0; i < mMasters.size(); i++) {
        Master master = mMasters.get(i);
        MasterNetAddress address = mMasterAddresses.get(i);
        master.updateConf(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT,
            Integer.toString(address.getEmbeddedJournalPort()));

        File journalDir = new File(mWorkDir, "journal" + i);
        journalDir.mkdirs();
        master.updateConf(PropertyKey.MASTER_JOURNAL_FOLDER, journalDir.getAbsolutePath());
      }
    }
  }

  /**
   * @param i the index of the worker to stop
   */
  public synchronized void stopWorker(int i) throws IOException {
    mWorkers.get(i).close();
  }

  /**
   * @return the journal directory
   */
  public synchronized String getJournalDir() {
    return mProperties.get(PropertyKey.MASTER_JOURNAL_FOLDER);
  }

  /**
   * @param masterId index of the master
   * @return the journal directory for the specified master
   */
  public String getJournalDir(int masterId) {
    return mMasters.get(masterId).getConf().get(PropertyKey.MASTER_JOURNAL_FOLDER);
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
    String extension = "-" + mMasterIds.get(i);
    File confDir = new File(mWorkDir, "conf-master" + extension);
    File metastoreDir = new File(mWorkDir, "metastore-master" + extension);
    File logsDir = new File(mWorkDir, "logs-master" + extension);
    logsDir.mkdirs();
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "MASTER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, confDir.getAbsolutePath());
    conf.put(PropertyKey.MASTER_METASTORE_DIR, metastoreDir.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, logsDir.getAbsolutePath());
    conf.put(PropertyKey.MASTER_HOSTNAME, address.getHostname());
    conf.put(PropertyKey.MASTER_RPC_PORT, Integer.toString(address.getRpcPort()));
    conf.put(PropertyKey.MASTER_WEB_PORT, Integer.toString(address.getWebPort()));
    conf.put(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT,
        Integer.toString(address.getEmbeddedJournalPort()));
    if (mDeployMode.equals(DeployMode.EMBEDDED)) {
      File journalDir = new File(mWorkDir, "journal" + extension);
      journalDir.mkdirs();
      conf.put(PropertyKey.MASTER_JOURNAL_FOLDER, journalDir.getAbsolutePath());
    }
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
    int rpcPort = getNewPort();
    int dataPort = getNewPort();
    int webPort = getNewPort();

    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.LOGGER_TYPE, "WORKER_LOGGER");
    conf.put(PropertyKey.CONF_DIR, confDir.getAbsolutePath());
    conf.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0),
        ramdisk.getAbsolutePath());
    conf.put(PropertyKey.LOGS_DIR, logsDir.getAbsolutePath());
    conf.put(PropertyKey.WORKER_RPC_PORT, Integer.toString(rpcPort));
    conf.put(PropertyKey.WORKER_WEB_PORT, Integer.toString(webPort));

    Worker worker = mCloser.register(new Worker(logsDir, conf));
    mWorkers.add(worker);
    LOG.info("Created worker with (rpc, data, web) ports ({}, {}, {})", rpcPort, dataPort,
        webPort);
    return worker;
  }

  /**
   * Formats the cluster journal.
   */
  public synchronized void formatJournal() throws IOException {
    if (mDeployMode == DeployMode.EMBEDDED) {
      for (Master master : mMasters) {
        File journalDir = new File(master.getConf().get(PropertyKey.MASTER_JOURNAL_FOLDER));
        FileUtils.deleteDirectory(journalDir);
        journalDir.mkdirs();
      }
      return;
    }
    try (Closeable c = new ConfigurationRule(mProperties, ServerConfiguration.global())
        .toResource()) {
      Format.format(Format.Mode.MASTER, ServerConfiguration.global());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return a client for determining the serving master
   */
  public synchronized MasterInquireClient getMasterInquireClient() {
    switch (mDeployMode) {
      case UFS_NON_HA:
        Preconditions.checkState(mMasters.size() == 1,
            "Running with multiple masters requires Zookeeper or Embedded Journal");
        return new SingleMasterInquireClient(InetSocketAddress.createUnresolved(
            mMasterAddresses.get(0).getHostname(), mMasterAddresses.get(0).getRpcPort()));
      case EMBEDDED:
        if (mMasterAddresses.size() > 1) {
          List<InetSocketAddress> addresses = new ArrayList<>(mMasterAddresses.size());
          for (MasterNetAddress address : mMasterAddresses) {
            addresses.add(
                InetSocketAddress.createUnresolved(address.getHostname(), address.getRpcPort()));
          }
          return new PollingMasterInquireClient(addresses, ServerConfiguration.global(),
              ServerUserState.global());
        } else {
          return new SingleMasterInquireClient(InetSocketAddress.createUnresolved(
              mMasterAddresses.get(0).getHostname(), mMasterAddresses.get(0).getRpcPort()));
        }
      case ZOOKEEPER_HA:
        return ZkMasterInquireClient.getClient(mCuratorServer.getConnectString(),
            ServerConfiguration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            ServerConfiguration.get(PropertyKey.ZOOKEEPER_LEADER_PATH),
            ServerConfiguration.getInt(PropertyKey.ZOOKEEPER_LEADER_INQUIRY_RETRY_COUNT),
            ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_AUTH_ENABLED));
      default:
        throw new IllegalStateException("Unknown deploy mode: " + mDeployMode.toString());
    }
  }

  /**
   * Writes the contents of properties to the configuration file.
   */
  private void writeConf() throws IOException {
    for (int i = 0; i < mNumMasters; i++) {
      String extension = "-" + mMasterIds.get(i);
      File confDir = new File(mWorkDir, "conf-master" + extension);
      writeConfToFile(confDir, mMasterProperties.getOrDefault(i, new HashMap<>()));
    }
    for (int i = 0; i < mNumWorkers; i++) {
      File confDir = new File(mWorkDir, "conf-worker" + i);
      writeConfToFile(confDir, mWorkerProperties.getOrDefault(i, new HashMap<>()));
    }
  }

  private int getNewPort() throws IOException {
    if (System.getenv(ALLUXIO_USE_FIXED_TEST_PORTS) == null) {
      return PortUtils.getFreePort();
    }
    Preconditions.checkState(!mPorts.isEmpty(), "Out of ports to reserve");
    return mPorts.remove(mPorts.size() - 1).getPort();
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

  private List<MasterNetAddress> generateMasterAddresses(int numMasters) throws IOException {
    List<MasterNetAddress> addrs = new ArrayList<>();
    for (int i = 0; i < numMasters; i++) {
      addrs.add(new MasterNetAddress(NetworkAddressUtils
          .getLocalHostName((int) ServerConfiguration
              .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
          getNewPort(), getNewPort(), getNewPort()));
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
    EMBEDDED,
    UFS_NON_HA, ZOOKEEPER_HA
  }

  /**
   * Builder for {@link MultiProcessCluster}.
   */
  public static final class Builder {
    private final List<ReservedPort> mReservedPorts;

    private Map<PropertyKey, String> mProperties = new HashMap<>();
    private Map<Integer, Map<PropertyKey, String>> mMasterProperties = new HashMap<>();
    private Map<Integer, Map<PropertyKey, String>> mWorkerProperties = new HashMap<>();
    private int mNumMasters = 1;
    private int mNumWorkers = 1;
    private String mClusterName = "AlluxioMiniCluster";
    private boolean mNoFormat = false;

    // Should only be instantiated by newBuilder().
    private Builder(List<ReservedPort> reservedPorts) {
      mReservedPorts = reservedPorts;
    }

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
     * @param noFormat whether to skip formatting the journal
     * @return the builder
     */
    public Builder setNoFormat(boolean noFormat) {
      mNoFormat = noFormat;
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
          mNumMasters, mNumWorkers, mClusterName, mNoFormat, mReservedPorts);
    }
  }

  /**
   * @param reservedPorts ports reserved for usage by this cluster
   * @return a new builder for an {@link MultiProcessCluster}
   */
  public static Builder newBuilder(List<ReservedPort> reservedPorts) {
    return new Builder(reservedPorts);
  }
}
