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

package alluxio.external;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.cli.Format;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.SingleMasterInquireClient;
import alluxio.master.ZkMasterInquireClient;
import alluxio.network.PortUtils;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.apache.commons.io.Charsets;
import org.apache.curator.test.TestingServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for starting, stopping, and interacting with an Alluxio cluster where each master and
 * worker runs in its own process.
 *
 * The synchronization strategy for this class is to synchronize all public methods.
 */
@ThreadSafe
public final class ExternalCluster implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalCluster.class);

  private final Map<PropertyKey, String> mProperties;
  private final int mNumMasters;
  private final int mNumWorkers;
  private final String mClusterName;

  /** Base directory for storing configuration and logs. */
  private File mBaseDir;
  /** Closer for closing all resources that must be closed when the cluster is destroyed. */
  private Closer mCloser;
  private List<Master> mMasters;
  private List<Worker> mWorkers;
  /** Addresses of all masters. Should have the same size as {@link #mMasters}. */
  private List<MasterNetAddress> mMasterAddresses;
  private State mState;
  private TestingServer mCuratorServer;

  private ExternalCluster(Map<PropertyKey, String> properties, int numMasters, int numWorkers,
      String clusterName) {
    mProperties = ConfigurationTestUtils.testConfigurationDefaults();
    // Allow explicitly set properties to override test defaults.
    mProperties.putAll(properties);
    mNumMasters = numMasters;
    mNumWorkers = numWorkers;
    // Add a unique number so that different runs of the same test use different cluster names.
    mClusterName = clusterName + ThreadLocalRandom.current().nextLong();
    mMasters = new ArrayList<>();
    mWorkers = new ArrayList<>();
    mCloser = Closer.create();
    mState = State.NOT_STARTED;
  }

  /**
   * Starts the cluster, launching all server processes.
   */
  public synchronized void start() throws Exception {
    Preconditions.checkState(mState != State.STARTED, "Cannot start while already started");
    Preconditions.checkState(mState != State.DESTROYED, "Cannot start a destroyed cluster");
    mState = State.STARTED;
    mMasterAddresses = generateMasterAddresses(mNumMasters);
    if (zkEnabled()) {
      mCuratorServer = mCloser
          .register(new TestingServer(-1, AlluxioTestDirectory.createTemporaryDirectory("zk")));
      mProperties.put(PropertyKey.ZOOKEEPER_ADDRESS, mCuratorServer.getConnectString());
    } else {
      mProperties.put(PropertyKey.MASTER_HOSTNAME, mMasterAddresses.get(0).getHostname());
      mProperties.put(PropertyKey.MASTER_RPC_PORT,
          Integer.toString(mMasterAddresses.get(0).getRpcPort()));
      mProperties.put(PropertyKey.MASTER_WEB_PORT,
          Integer.toString(mMasterAddresses.get(0).getWebPort()));
    }

    mBaseDir = AlluxioTestDirectory.createTemporaryDirectory(mClusterName);
    setupRamdisk();
    setupUfs();
    setupJournal();
    // This must happen after setting up ramdisk, ufs, and journal because they must update
    // configuration properties.
    writeConf();

    // Start servers
    LOG.info("Starting alluxio cluster {} with base directory {}", mClusterName,
        mBaseDir.getAbsolutePath());
    for (int i = 0; i < mNumMasters; i++) {
      startMaster(i);
    }
    for (int i = 0; i < mNumWorkers; i++) {
      startWorker(i);
    }
  }

  /**
   * Kills the primary master.
   *
   * If no master is currently primary, this method blocks until a primary has been elected, then
   * kills it.
   */
  public synchronized void killPrimaryMaster() {
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
    });
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
        return;
      }
    }
    throw new RuntimeException(
        String.format("No master found with RPC port %d. Master addresses: %s", primaryRpcPort,
            mMasterAddresses));
  }

  /**
   * @return a client for interacting with the cluster
   */
  public synchronized FileSystem getFileSystemClient() {
    Preconditions.checkState(mState == State.STARTED,
        "must be in the started state to get an fs client, but state was %s", mState);
    MasterInquireClient inquireClient = getMasterInquireClient();
    return FileSystem.Factory.get(FileSystemContext.create(null, inquireClient));
  }

  /**
   * Destroys the cluster. It may not be re-started after being destroyed.
   */
  public synchronized void destroy() throws IOException {
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
    Master master = mCloser.register(new Master(mBaseDir, i, mMasterAddresses.get(i)));
    mMasters.add(master);
    master.start();
  }

  /**
   * Starts the specified worker.
   *
   * @param i the index of the worker to start
   */
  public synchronized void startWorker(int i) throws IOException {
    Worker worker = mCloser.register(new Worker(mBaseDir, i));
    mWorkers.add(worker);
    worker.start();
  }

  private void setupRamdisk() {
    // Use a local file as a fake ramdisk. This doesn't require sudo and perf isn't important here.
    String ramdisk = new File(mBaseDir, "ramdisk").getAbsolutePath();
    mProperties.put(PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(0), ramdisk);
  }

  private void setupUfs() {
    File ufs = new File(mBaseDir, "underStorage");
    ufs.mkdirs();
    mProperties.put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, ufs.getAbsolutePath());
  }

  private void setupJournal() throws Exception {
    String journalFolder = new File(mBaseDir, "journal").getAbsolutePath();
    mProperties.put(PropertyKey.MASTER_JOURNAL_FOLDER, journalFolder);
    try (Closeable c =
        new ConfigurationRule(PropertyKey.MASTER_JOURNAL_FOLDER, journalFolder).toResource()) {
      Format.format(Format.Mode.MASTER);
    }
  }

  private MasterInquireClient getMasterInquireClient() {
    if (zkEnabled()) {
      return ZkMasterInquireClient.getClient(mCuratorServer.getConnectString(),
          Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
          Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
    } else {
      Preconditions.checkState(mMasters.size() == 1,
          "Running with multiple masters requires Zookeeper to be enabled");
      return new SingleMasterInquireClient(new InetSocketAddress(
          mMasterAddresses.get(0).getHostname(), mMasterAddresses.get(0).getRpcPort()));
    }
  }

  private boolean zkEnabled() {
    return mProperties.containsKey(PropertyKey.ZOOKEEPER_ENABLED)
        && mProperties.get(PropertyKey.ZOOKEEPER_ENABLED).equalsIgnoreCase("true");
  }

  /**
   * Writes the contents of {@link #mProperties} to the configuration file.
   */
  private void writeConf() throws IOException {
    File confDir = new File(mBaseDir, "conf");
    confDir.mkdirs();
    StringBuilder sb = new StringBuilder();
    for (Entry<PropertyKey, String> entry : mProperties.entrySet()) {
      sb.append(String.format("%s=%s%n", entry.getKey(), entry.getValue()));
    }
    try (
        FileOutputStream fos = new FileOutputStream(new File(confDir, "alluxio-site.properties"))) {
      fos.write(sb.toString().getBytes(Charsets.UTF_8));
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
          destroy();
        }
      }
    };
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
   * Builder for {@link ExternalCluster}.
   */
  public final static class Builder {
    private Map<PropertyKey, String> mProperties = new HashMap<>();
    private int mNumMasters = 1;
    private int mNumWorkers = 1;
    private String mClusterName = "AlluxioMiniCluster";

    private Builder() {} // Should only be instantiated by newBuilder().

    /**
     * @param key the property key to set
     * @param value the value to set
     * @return the builder
     */
    public Builder addProperty(PropertyKey key, String value) {
      mProperties.put(key, value);
      return this;
    }

    /**
     * @param properties alluxio properties for launched masters and workers
     * @return the builder
     */
    public Builder addProperties(Map<PropertyKey, String> properties) {
      mProperties.putAll(properties);
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
     * @return a constructed {@link ExternalCluster}
     */
    public ExternalCluster build() {
      return new ExternalCluster(mProperties, mNumMasters, mNumWorkers, mClusterName);
    }
  }

  /**
   * @return a new builder for an {@link ExternalCluster}
   */
  public static Builder newBuilder() {
    return new Builder();
  }
}
