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

package alluxio.server.cross.cluster;

import static alluxio.testutils.CrossClusterTestUtils.CREATE_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.assertFileDoesNotExist;
import static alluxio.testutils.CrossClusterTestUtils.checkNonCrossClusterWrite;
import static alluxio.testutils.CrossClusterTestUtils.fileExists;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.MountPOptions;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;

public class CrossClusterIntegrationTest extends BaseIntegrationTest {

  @Rule
  public ConfigurationRule mConf =
      new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false,
          Configuration.modifiableGlobal());

  public MultiProcessCluster mCluster1;
  public MultiProcessCluster mCluster2;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(10000);

  final Map<PropertyKey, Object> mBaseProperties = ImmutableMap.<PropertyKey, Object>builder()
      .put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
      .put(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
      .put(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true).build();

  @After
  public void after() throws Exception {
    if (mCluster1 != null) {
      mCluster1.destroy();
    }
    if (mCluster2 != null) {
      mCluster2.destroy();
    }
  }

  public String clusterSetup(AlluxioURI mountPath, FileSystemCrossCluster ... clients)
      throws Exception {
    String ufsPath = mFolder.newFolder().getAbsoluteFile().toString();
    String ufsUri = "file://" + ufsPath;
    MountPOptions options = MountPOptions.newBuilder().setCrossCluster(true).build();

    for (FileSystemCrossCluster client : clients) {
      // Mount ufs1 to /mnt1 with specified options.
      client.mount(mountPath, new AlluxioURI(ufsUri), options);
    }
    return ufsPath;
  }

  String addressesToString(List<MasterNetAddress> addresses) {
    StringBuilder builder = new StringBuilder();
    for (MasterNetAddress address : addresses) {
      builder.append(address.getHostname());
      builder.append(":");
      builder.append(address.getRpcPort());
      builder.append(",");
    }
    builder.deleteCharAt(builder.length() - 1);
    return builder.toString();
  }

  @Test
  public void crossClusterChangeLeader() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "localhost:1234")
        .includeCrossClusterStandalone()
        .build();
    mCluster1.start();
    mCluster2 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER2)
        .setClusterName("crossCluster_test_write2")
        .setNumMasters(3)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES,
            addressesToString(mCluster1.getCrossClusterAddresses()))
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
        .build();
    mCluster2.start();

    AlluxioURI mountPath = new AlluxioURI("/mnt1");

    FileSystemCrossCluster client1 = mCluster1.getCrossClusterClient();
    FileSystemCrossCluster client2 = mCluster2.getCrossClusterClient();
    String ufsPath = clusterSetup(mountPath, client1, client2);

    checkNonCrossClusterWrite(ufsPath, mountPath, client1, client2);

    AlluxioURI file1 = mountPath.join("file1");
    assertFileDoesNotExist(file1, client1, client2);

    // kill the primary master on cluster2
    mCluster2.waitForAndKillPrimaryMaster(5000);

    // be sure the file becomes visible on cluster2
    client1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);

    // be sure new files are synced
    AlluxioURI file2 = mountPath.join("file2");
    assertFileDoesNotExist(file2, client1, client2);
    client1.createFile(file2, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file2, client1, client2),
        mWaitOptions);

    mCluster2.stopMasters();
  }

  @Test
  public void crossClusterWrite() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
        .includeCrossClusterStandalone()
        .build();
    mCluster1.start();
    mCluster2 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER2)
        .setClusterName("crossCluster_test_write2")
        .setNumMasters(NUM_MASTERS)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES,
            addressesToString(mCluster1.getCrossClusterAddresses()))
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
        .build();
    mCluster2.start();

    AlluxioURI mountPath = new AlluxioURI("/mnt1");

    FileSystemCrossCluster client1 = mCluster1.getCrossClusterClient();
    FileSystemCrossCluster client2 = mCluster2.getCrossClusterClient();
    String ufsPath = clusterSetup(mountPath, client1, client2);

    checkNonCrossClusterWrite(ufsPath, mountPath, client1, client2);

    AlluxioURI file1 = mountPath.join("file1");
    assertFileDoesNotExist(file1, client1, client2);
    assertFileDoesNotExist(file1, client1, client2);

    client1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);
  }

  @Test
  public void crossClusterRestartNameService() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "localhost:1234")
        .includeCrossClusterStandalone()
        .build();
    mCluster1.start();
    mCluster2 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER2)
        .setClusterName("crossCluster_test_write2")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES,
            addressesToString(mCluster1.getCrossClusterAddresses()))
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
        .build();
    mCluster2.start();

    AlluxioURI mountPath = new AlluxioURI("/mnt1");

    FileSystemCrossCluster client1 = mCluster1.getCrossClusterClient();
    FileSystemCrossCluster client2 = mCluster2.getCrossClusterClient();
    String ufsPath = clusterSetup(mountPath, client1, client2);

    checkNonCrossClusterWrite(ufsPath, mountPath, client1, client2);

    AlluxioURI file1 = mountPath.join("file1");
    assertFileDoesNotExist(file1, client1, client2);

    // kill the cross cluster master standalone process
    mCluster1.killCrossClusterStandalone();

    // be sure the file becomes visible on cluster2
    client1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);

    // be sure new files are synced
    AlluxioURI file2 = mountPath.join("file2");
    assertFileDoesNotExist(file2, client1, client2);
    client1.createFile(file2, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file2, client1, client2),
        mWaitOptions);

    mCluster2.stopMasters();
  }

}
