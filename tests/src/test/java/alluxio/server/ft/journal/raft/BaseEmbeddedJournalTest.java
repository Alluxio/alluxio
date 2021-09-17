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

package alluxio.server.ft.journal.raft;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.grpc.QuorumServerState;
import alluxio.master.AlluxioMasterProcess;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination.ReservedPort;
import alluxio.testutils.BaseIntegrationTest;

import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import org.junit.After;
import org.junit.Rule;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class BaseEmbeddedJournalTest extends BaseIntegrationTest {

  @Rule
  public ConfigurationRule mConf =
          new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, "false",
                  ServerConfiguration.global());

  public MultiProcessCluster mCluster;
  // Used to grow cluster.
  protected List<AlluxioMasterProcess> mNewMasters = new ArrayList<>();

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
    for (AlluxioMasterProcess newMasterProcess : mNewMasters) {
      newMasterProcess.stop();
    }
    mNewMasters.clear();
  }

  public List<MasterNetAddress> addNewMastersToCluster(List<ReservedPort> ports) throws Exception {
    List<MasterNetAddress> newMasterAddresses = new ArrayList<>();
    for (int i = 0; i < ports.size(); i += 3) {
      // Create and start a new master to join to existing cluster.
      // Get new master address.
      MasterNetAddress newMasterAddress = new MasterNetAddress(
          NetworkAddressUtils.getLocalHostName(
              (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)),
          ports.get(i).getPort(),
          ports.get(i + 1).getPort(),
          ports.get(i + 2).getPort());
      newMasterAddresses.add(newMasterAddress);

      // Update EmbeddedJournal addresses with the new master address.
      String newEbjList = ServerConfiguration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES)
          + "," + newMasterAddress.getHostname() + ":" + newMasterAddress.getEmbeddedJournalPort();
      ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, newEbjList);
      // Update RPC addresses with the new master address.
      String newRpcList = ServerConfiguration.get(PropertyKey.MASTER_RPC_ADDRESSES) + ","
          + newMasterAddress.getHostname() + ":" + newMasterAddress.getRpcPort();
      ServerConfiguration.set(PropertyKey.MASTER_RPC_ADDRESSES, newRpcList);

      File newMasterWorkDir = mCluster.getWorkDir();
      String extension = "-" + newMasterAddress.getEmbeddedJournalPort();
      // Create new dirs for new master and update configuration.
      File newMasterConfDir = new File(newMasterWorkDir, "conf-master" + extension);
      newMasterConfDir.mkdir();
      ServerConfiguration.set(PropertyKey.CONF_DIR, newMasterConfDir);
      File newMasterMetastoreDir = new File(newMasterWorkDir, "metastore-master" + extension);
      newMasterMetastoreDir.mkdir();
      ServerConfiguration.set(PropertyKey.MASTER_METASTORE_DIR, newMasterMetastoreDir);
      File newMasterLogsDir = new File(newMasterWorkDir, "logs-master" + extension);
      newMasterLogsDir.mkdir();
      ServerConfiguration.set(PropertyKey.LOGS_DIR, newMasterLogsDir);
      File newMasterJournalDir = new File(newMasterWorkDir, "journal" + extension);
      newMasterJournalDir.mkdirs();
      ServerConfiguration.set(PropertyKey.MASTER_JOURNAL_FOLDER, newMasterJournalDir);

      // Update network settings for the new master.
      ServerConfiguration.set(PropertyKey.MASTER_HOSTNAME, newMasterAddress.getHostname());
      ServerConfiguration.set(PropertyKey.MASTER_RPC_PORT,
          Integer.toString(newMasterAddress.getRpcPort()));
      ServerConfiguration.set(PropertyKey.MASTER_WEB_PORT,
          Integer.toString(newMasterAddress.getWebPort()));
      ServerConfiguration.set(PropertyKey.MASTER_EMBEDDED_JOURNAL_PORT,
          Integer.toString(newMasterAddress.getEmbeddedJournalPort()));

      // Create and start the new master.
      AlluxioMasterProcess newMaster = AlluxioMasterProcess.Factory.create();
      mNewMasters.add(newMaster);
      // Update cluster with the new address for further queries to
      // include the new master. Otherwise clients could fail if stopping
      // a master causes the new master to become the leader.
      mCluster.addExternalMasterAddress(newMasterAddress);

      // Submit a common task for starting the master.
      ForkJoinPool.commonPool().execute(() -> {
        try {
          newMaster.start();
        } catch (Exception e) {
          throw new RuntimeException("Failed to start new master.", e);
        }
      });
      // Wait until new master is part of the quorum.
      waitForQuorumPropertySize(info ->  info.getServerState() == QuorumServerState.AVAILABLE
            && info.getServerAddress().equals(masterEBJAddr2NetAddr(newMasterAddress)), 1);
    }
    return newMasterAddresses;
  }

  protected NetAddress masterEBJAddr2NetAddr(MasterNetAddress masterAddr) {
    return NetAddress.newBuilder().setHost(masterAddr.getHostname())
        .setRpcPort(masterAddr.getEmbeddedJournalPort()).build();
  }

  protected void waitForQuorumPropertySize(Predicate<? super QuorumServerInfo> pred, int size)
      throws InterruptedException, TimeoutException {
    final int TIMEOUT_1MIN = 60 * 1000; // in ms
    CommonUtils.waitFor("quorum property", () -> {
      try {
        return mCluster.getJournalMasterClientForMaster().getQuorumInfo().getServerInfoList()
            .stream().filter(pred).count() == size;
      } catch (AlluxioStatusException e) {
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_1MIN));
  }
}
