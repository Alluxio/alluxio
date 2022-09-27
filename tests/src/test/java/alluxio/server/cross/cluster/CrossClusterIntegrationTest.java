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

import static alluxio.multi.process.MultiProcessCluster.addressesToString;
import static alluxio.testutils.CrossClusterTestUtils.CREATE_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.assertFileDoesNotExist;
import static alluxio.testutils.CrossClusterTestUtils.assertFileExists;
import static alluxio.testutils.CrossClusterTestUtils.checkClusterSyncAcrossAll;
import static alluxio.testutils.CrossClusterTestUtils.checkNonCrossClusterWrite;
import static alluxio.testutils.CrossClusterTestUtils.fileExists;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.MountPOptions;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class CrossClusterIntegrationTest extends BaseIntegrationTest {

  @Rule
  public ConfigurationRule mConf =
      new ConfigurationRule(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false,
          Configuration.modifiableGlobal());

  public MultiProcessCluster mCluster1;
  public MultiProcessCluster mCluster2;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  public static final int START_TIMEOUT = 30_000;
  public static final int KILL_TIMEOUT = 10_000;
  public static final int SYNC_TIMEOUT = 10_000;

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(SYNC_TIMEOUT);

  final Map<PropertyKey, Object> mBaseProperties = ImmutableMap.<PropertyKey, Object>builder()
      .put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED)
      .put(PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS, "5min")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MIN_ELECTION_TIMEOUT, "750ms")
      .put(PropertyKey.MASTER_EMBEDDED_JOURNAL_MAX_ELECTION_TIMEOUT, "1500ms")
      .put(PropertyKey.CROSS_CLUSTER_MASTER_RPC_RETRY_MAX_DURATION, "10sec")
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

  @Test
  public void crossClusterChangeLeader() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
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
    mCluster2.waitForAndKillPrimaryMaster(KILL_TIMEOUT);
    mCluster2.getPrimaryMasterIndex(START_TIMEOUT);

    // be sure the file becomes visible on cluster2
    client1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);

    // be sure new files are synced from both clusters
    checkClusterSyncAcrossAll(mountPath, client1, client2);
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

    // be sure new files are synced from both clusters
    checkClusterSyncAcrossAll(mountPath, client1, client2);
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

    // restart the cross cluster master standalone process
    mCluster1.killCrossClusterStandalone();
    // write a file to the mount point while the master is down
    client1.createFile(file1, CREATE_OPTIONS).close();
    mCluster1.startNewCrossClusterMaster(true);

    // be sure the file becomes visible on cluster 2
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);

    // create a new mount point
    AlluxioURI mountPath2 = new AlluxioURI("/mnt2");
    clusterSetup(mountPath2, client1, client2);

    // be sure new files are synced from both clusters
    checkClusterSyncAcrossAll(mountPath2, client1, client2);
  }

  @Test
  public void crossClusterMasterStopped() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
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
    // be sure we are unable to mount a new cross cluster mount
    AlluxioURI mountPath2 = new AlluxioURI("/mnt2");
    Assert.assertThrows(AlluxioException.class, () -> clusterSetup(mountPath2, client1));
    Assert.assertThrows(AlluxioException.class, () -> clusterSetup(mountPath2, client2));

    // restart the cross cluster mater standalone process
    mCluster1.startNewCrossClusterMaster(true);

    // we should be able to mount the new path
    clusterSetup(mountPath2, client1, client2);

    checkClusterSyncAcrossAll(mountPath2, client1, client2);
  }

  @Test
  public void crossClusterRestartNameServiceAndLeader() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
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

    // kill the cross cluster master standalone process
    mCluster1.killCrossClusterStandalone();
    // write a file to the mount point while the master is down
    // be sure new files are synced from both clusters
    checkClusterSyncAcrossAll(mountPath, client1, client2);

    // restart a master on cluster two
    mCluster2.waitForAndKillPrimaryMaster(KILL_TIMEOUT);
    mCluster2.getPrimaryMasterIndex(START_TIMEOUT);

    // files will not be synced because the new master does not know
    // about the external mounts yet
    // write a file on the mount point while the standalone is down
    AlluxioURI file2 = mountPath.join("file2");
    assertFileDoesNotExist(file2, client1, client2);
    client1.createFile(file2, CREATE_OPTIONS).close();
    // be sure it is not synced on cluster 2
    Assert.assertThrows(TimeoutException.class, () ->
        CommonUtils.waitFor("File synced across clusters",
            () -> fileExists(file2, client2), WaitForOptions.defaults().setTimeoutMs(
                SYNC_TIMEOUT)));

    // restart the cross cluster master standalone
    mCluster1.startNewCrossClusterMaster(true);
    // the file should be synced now
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file2, client2), WaitForOptions.defaults().setTimeoutMs(SYNC_TIMEOUT));

    // create a new mount point
    AlluxioURI mountPath2 = new AlluxioURI("/mnt2");
    clusterSetup(mountPath2, client1, client2);

    // be sure new files are synced from both clusters on the new mount point
    checkClusterSyncAcrossAll(mountPath2, client1, client2);
  }

  @Test
  public void crossClusterChangeNameServiceAddress() throws Exception {
    final int NUM_WORKERS = 1;
    mCluster1 = MultiProcessCluster.newBuilder(PortCoordination.CROSS_CLUSTER_CLUSTER1)
        .setClusterName("crossCluster_test_write1")
        .setNumMasters(1)
        .setNumWorkers(NUM_WORKERS)
        .addProperties(mBaseProperties)
        .addProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
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
    client1.createFile(file1, CREATE_OPTIONS).close();
    // be sure the file becomes visible on cluster 2
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, client1, client2),
        mWaitOptions);

    // restart the cross cluster master standalone process with a new address
    mCluster1.killCrossClusterStandalone();
    mCluster1.startNewCrossClusterMaster(false);

    // it should not be possible to create a new mount
    AlluxioURI mountPath2 = new AlluxioURI("/mnt2");
    Assert.assertThrows(AlluxioException.class, () -> clusterSetup(mountPath2, client1));
    Assert.assertThrows(AlluxioException.class, () -> clusterSetup(mountPath2, client2));

    // inform the clusters of the new address
    mCluster1.setCrossClusterClientAddresses(mCluster1.getCrossClusterAddresses());
    mCluster2.setCrossClusterClientAddresses(mCluster1.getCrossClusterAddresses());

    // create a new mount point
    clusterSetup(mountPath2, client1, client2);

    // be sure new files are synced from both clusters
    checkClusterSyncAcrossAll(mountPath, client1, client2);
  }

  @Test
  public void crossClusterInvalidateOnReconnection() throws Exception {
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

    AlluxioURI mountPath = new AlluxioURI("/mnt1");

    FileSystemCrossCluster client1 = mCluster1.getCrossClusterClient();
    String ufsPath = clusterSetup(mountPath, client1);

    AlluxioURI file1 = mountPath.join("file1");
    assertFileDoesNotExist(file1, client1);
    client1.createFile(file1, CREATE_OPTIONS).close();
    assertFileExists(file1, client1);

    // kill the cross cluster master standalone process
    mCluster1.killCrossClusterStandalone();

    // simulate another cluster being able to connect to the standalone
    // cross cluster master, and mounting and deleting file1 while
    // cluster 1 is disconnected
    Files.delete(Paths.get(ufsPath, "file1"));
    // be sure the delete operation is not seen on cluster 1
    Assert.assertThrows(TimeoutException.class, () ->
        CommonUtils.waitFor("File synced across clusters",
            () -> !fileExists(file1, client1), WaitForOptions.defaults().setTimeoutMs(
                SYNC_TIMEOUT)));

    // restart the cross cluster mater standalone process
    mCluster1.startNewCrossClusterMaster(true);

    // be sure file 1 is synced, as it will invalidate its cache when it reconnects
    // to the standalone
    CommonUtils.waitFor("File synced across clusters",
        () -> !fileExists(file1, client1),
        mWaitOptions);
  }
}
