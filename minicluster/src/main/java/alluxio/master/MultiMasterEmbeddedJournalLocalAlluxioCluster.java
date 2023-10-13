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

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.NodeState;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.PortCoordination;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.WorkerProcess;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local Alluxio cluster with multiple masters using embedded journal.
 * Because the cluster run in a single process, a single configuration instance
 * is shared across masters and workers.
 * If this causes issues, considering switching to {@link alluxio.multi.process.MultiProcessCluster}
 */
@NotThreadSafe
public final class MultiMasterEmbeddedJournalLocalAlluxioCluster
    extends AbstractLocalAlluxioCluster {
  private static final Logger LOG = LoggerFactory.getLogger(
      MultiMasterEmbeddedJournalLocalAlluxioCluster.class);

  private int mNumOfMasters = 0;

  private final List<LocalAlluxioMaster> mMasters = new ArrayList<>();
  private final List<PortCoordination.ReservedPort> mPorts;

  private final List<MasterNetAddress> mMasterAddresses;
  private final List<String> mJournalFolders = new ArrayList<>();

  /**
   * @param numMasters the number of masters to run
   * @param numWorkers the number of workers to run
   * @param reservedPorts reserved ports
   */
  public MultiMasterEmbeddedJournalLocalAlluxioCluster(
      int numMasters, int numWorkers, List<PortCoordination.ReservedPort> reservedPorts)
      throws IOException {
    super(numWorkers);
    mNumOfMasters = numMasters;
    mPorts = new ArrayList<>(reservedPorts);
    mMasterAddresses = generateMasterAddresses(numMasters);
  }

  private List<MasterNetAddress> generateMasterAddresses(int numMasters) throws IOException {
    int timeout = (int) Configuration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);
    List<MasterNetAddress> addrs = new ArrayList<>();
    for (int i = 0; i < numMasters; i++) {
      addrs.add(new MasterNetAddress(
          NetworkAddressUtils.getLocalHostName(timeout), getNewPort(), getNewPort(), getNewPort()));
    }
    return addrs;
  }

  private int getNewPort() throws IOException {
    Preconditions.checkState(!mPorts.isEmpty(), "Out of ports to reserve");
    return mPorts.remove(mPorts.size() - 1).getPort();
  }

  @Override
  public void initConfiguration(String name) throws IOException {
    setAlluxioWorkDirectory(name);
    setHostname();
    for (Map.Entry<PropertyKey, Object> entry : ConfigurationTestUtils
        .testConfigurationDefaults(Configuration.global(),
            mHostname, mWorkDirectory).entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    Configuration.set(PropertyKey.TEST_MODE, true);
    Configuration.set(PropertyKey.JOB_WORKER_THROTTLING, false);
    Configuration.set(PropertyKey.PROXY_WEB_PORT, 0);
    Configuration.set(PropertyKey.WORKER_RPC_PORT, 0);
    Configuration.set(PropertyKey.WORKER_WEB_PORT, 0);

    List<String> journalAddresses = new ArrayList<>();
    List<String> rpcAddresses = new ArrayList<>();
    for (MasterNetAddress address : mMasterAddresses) {
      journalAddresses
          .add(String.format("%s:%d", address.getHostname(), address.getEmbeddedJournalPort()));
      rpcAddresses.add(String.format("%s:%d", address.getHostname(), address.getRpcPort()));
    }
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
    Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
        com.google.common.base.Joiner.on(",").join(journalAddresses));
    Configuration.set(PropertyKey.MASTER_RPC_ADDRESSES,
        com.google.common.base.Joiner.on(",").join(rpcAddresses));
  }

  @Override
  public synchronized FileSystem getClient() throws IOException {
    return getLocalAlluxioMaster().getClient();
  }

  @Override
  public FileSystem getClient(FileSystemContext context) throws IOException {
    return getLocalAlluxioMaster().getClient(context);
  }

  @Override
  public LocalAlluxioMaster getLocalAlluxioMaster() {
    for (LocalAlluxioMaster master : mMasters) {
      // Return the leader master, if possible.
      if (master.isServing()
          && master.getMasterProcess().mLeaderSelector.getState() == NodeState.PRIMARY) {
        return master;
      }
    }
    return mMasters.get(0);
  }

  /**
   * @param index the index
   * @return the local alluxio master
   */
  public LocalAlluxioMaster getLocalAlluxioMasterByIndex(int index) {
    return mMasters.get(index);
  }

  /**
   * @param index the index
   * @return the worker process by index
   */
  public WorkerProcess getWorkerProcess(int index) {
    return mWorkers.get(index);
  }

  /**
   * @return index of leader master in {@link #mMasters}, or -1 if there is no leader temporarily
   */
  public int getLeaderIndex() {
    for (int i = 0; i < mNumOfMasters; i++) {
      if (mMasters.get(i).isServing()
          && mMasters.get(i).getMasterProcess().mLeaderSelector.getState() == NodeState.PRIMARY) {
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
    for (int i = 0; i < mNumOfMasters; i++) {
      addrs.add(mMasters.get(i).getAddress());
    }
    return addrs;
  }

  /**
   * Iterates over the masters in the order of master creation, stops the first standby master.
   *
   * @return true if a standby master is successfully stopped, otherwise, false
   */
  public boolean stopStandby() {
    for (int k = 0; k < mNumOfMasters; k++) {
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
    int leaderId = getLeaderIndex();
    try {
      LOG.info("master {} is the leader. stopping it...", leaderId);
      getLocalAlluxioMasterByIndex(leaderId).stop();
      LOG.info("master {} stopped.", leaderId);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

  /**
   * Waits for the primary master to start until a timeout occurs.
   *
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public void waitForPrimaryMasterServing(int timeoutMs)
      throws TimeoutException, InterruptedException {
    CommonUtils.waitFor("the primary leader master to start",
        () -> {
          int leaderId = getLeaderIndex();
          if (leaderId == -1) {
            return false;
          }
          try {
            getLocalAlluxioMasterByIndex(leaderId).getClient().listStatus(
                new AlluxioURI("/"));
            return true;
          } catch (Exception e) {
            return false;
          }
        },
        WaitForOptions.defaults().setTimeoutMs(timeoutMs));
  }

  @Override
  protected void startMasters() throws IOException {
    // Because all masters run in the same process, they share the same configuration.
    // Whenever we start a master, we modify these configurations to its dedicated ones.
    // Masters are started one by one so that each can read the correct configurations.
    // These configurations are mostly ports and will only be used when the master starts.
    // However, if unluckily some places read these configurations during runtime,
    // it might cause incorrect behaviors or errors because these configurations might be
    // overridden by the late coming masters.
    // If this happens, considering switching to MultiProcessCluster.
    // Also, please do not rely on these configurations in your test cases
    // because these configurations essentially reflect the configurations of
    // the last master we started.
    for (int k = 0; k < mNumOfMasters; k++) {
      Configuration.set(PropertyKey.MASTER_METASTORE_DIR,
          PathUtils.concatPath(mWorkDirectory, "metastore-" + k));
      MasterNetAddress address = mMasterAddresses.get(k);
      Configuration.set(PropertyKey.LOGGER_TYPE, "MASTER_LOGGER");
      Configuration.set(PropertyKey.MASTER_HOSTNAME, address.getHostname());
      Configuration.set(PropertyKey.MASTER_RPC_PORT, address.getRpcPort());
      Configuration.set(PropertyKey.MASTER_WEB_PORT, address.getWebPort());
      Configuration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT,
          address.getEmbeddedJournalPort());
      Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mJournalFolders.get(k));

      final LocalAlluxioMaster master = LocalAlluxioMaster.create(mWorkDirectory, false);
      master.start();
      LOG.info("master NO.{} started, isServing: {}, address: {}", k, master.isServing(),
          master.getAddress());
      mMasters.add(master);
    }

    LOG.info("all {} masters started.", mNumOfMasters);
    LOG.info("waiting for a leader.");
    try {
      waitForMasterServing();
    } catch (Exception e) {
      throw new IOException(e);
    }
    // Use first master port
    Configuration.set(PropertyKey.MASTER_RPC_PORT,
        getLocalAlluxioMaster().getRpcLocalPort());
  }

  @Override
  public void startWorkers() throws Exception {
    super.startWorkers();
  }

  @Override
  public void stopFS() throws Exception {
    super.stopFS();
  }

  @Override
  public void stopMasters() throws Exception {
    for (int k = 0; k < mNumOfMasters; k++) {
      mMasters.get(k).stop();
    }
  }

  @Override
  protected void formatJournal() {
    for (int i = 0; i < mNumOfMasters; ++i) {
      String extension = "-" + i;
      File journalDir = new File(mWorkDirectory, "journal" + extension);
      journalDir.mkdirs();
      mJournalFolders.add(journalDir.getAbsolutePath());
    }
  }
}
