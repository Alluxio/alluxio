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

package alluxio.server.ft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ListStatusPRequest;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MasterNetAddress;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.AlluxioOperationThread;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.CommonUtils;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for Alluxio high availability when Zookeeper has failures.
 */
public class ZookeeperFailureIntegrationTest extends BaseIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperFailureIntegrationTest.class);

  @Rule
  public ConfigurationRule mConf = new ConfigurationRule(ImmutableMap.of(
      PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1000",
      PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS, "500",
      PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "500",
      PropertyKey.USER_RPC_RETRY_MAX_DURATION, "2500"), ServerConfiguration.global()
  );

  public MultiProcessCluster mCluster;

  @After
  public void after() throws Exception {
    if (mCluster != null) {
      mCluster.destroy();
    }
  }

  /*
   * This test starts alluxio in HA mode, kills Zookeeper, waits for Alluxio to fail, then restarts
   * Zookeeper. Alluxio should recover when Zookeeper is restarted.
   */
  @Test
  public void zkFailure() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_FAILURE)
        .setClusterName("ZookeeperFailure")
        .setNumMasters(2)
        .setNumWorkers(1)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    mCluster.start();

    AlluxioOperationThread thread =
        new AlluxioOperationThread(mCluster.getFileSystemClient());
    thread.start();
    CommonUtils.waitFor("a successful operation to be performed", () -> thread.successes() > 0);
    mCluster.stopZk();
    long zkStopTime = System.currentTimeMillis();
    // Wait until 3 different failures are encountered on the thread.
    // PS: First failures could be related to worker capacity depending on process shutdown order,
    // thus still leaving RPC server reachable.
    AtomicInteger failureCounter = new AtomicInteger(3);
    AtomicReference<Throwable> lastFailure = new AtomicReference<>(null);
    CommonUtils.waitFor("operations to start failing", () -> failureCounter.getAndAdd(
        (lastFailure.getAndSet(thread.getLatestFailure()) != lastFailure.get()) ? -1 : 0) <= 0);

    assertFalse(rpcServiceAvailable());
    LOG.info("First operation failed {}ms after stopping the Zookeeper cluster",
        System.currentTimeMillis() - zkStopTime);
    final long successes = thread.successes();
    mCluster.restartZk();
    long zkStartTime = System.currentTimeMillis();
    CommonUtils.waitFor("another successful operation to be performed",
        () -> thread.successes() > successes);
    thread.interrupt();
    thread.join();
    LOG.info("Recovered after {}ms", System.currentTimeMillis() - zkStartTime);
    mCluster.notifySuccess();
  }

  @Test
  public void zkConnectionPolicy_Standard() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_CONNECTION_POLICY_STANDARD)
        .setClusterName("ZookeeperConnectionPolicy_Standard")
        .setNumMasters(2)
        .setNumWorkers(0)
        .addProperty(PropertyKey.ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY, "STANDARD")
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    mCluster.start();

    int leaderIdx = getPrimaryMasterIndex();
    mCluster.restartZk();
    // Leader will step down under STANDARD connection error policy.
    int leaderIdx2 = getPrimaryMasterIndex();
    assertNotEquals(leaderIdx, leaderIdx2);

    mCluster.notifySuccess();
  }

  @Test
  public void zkConnectionPolicy_Session() throws Exception {
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.ZOOKEEPER_CONNECTION_POLICY_SESSION)
        .setClusterName("ZookeeperConnectionPolicy_Session")
        .setNumMasters(2)
        .setNumWorkers(0)
        .addProperty(PropertyKey.ZOOKEEPER_LEADER_CONNECTION_ERROR_POLICY, "SESSION")
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
        .build();
    mCluster.start();

    int leaderIdx = getPrimaryMasterIndex();
    mCluster.restartZk();
    // Leader will retain its status under SESSION connection error policy.
    int leaderIdx2 = getPrimaryMasterIndex();
    assertEquals(leaderIdx, leaderIdx2);

    mCluster.notifySuccess();
  }

  /**
   * Used to get primary master index with retries for when zk server is down.
   */
  private int getPrimaryMasterIndex() throws Exception {
    AtomicInteger primaryIndex = new AtomicInteger();
    CommonUtils.waitFor("Getting primary master index", () -> {
      try {
        primaryIndex.set(mCluster.getPrimaryMasterIndex(30000));
        return true;
      } catch (Exception e) {
        LOG.warn("Could not get primary master index.", e);
        return false;
      }
    });

    return primaryIndex.get();
  }
  /*
   * This method uses a client with an explicit master address to ensure that the master has shut
   * down its rpc service.
   */
  private boolean rpcServiceAvailable() throws Exception {
    MasterNetAddress netAddress = mCluster.getMasterAddresses().get(0);
    InetSocketAddress address =
        new InetSocketAddress(netAddress.getHostname(), netAddress.getRpcPort());
    try {
      GrpcChannel channel = GrpcChannelBuilder
          .newBuilder(GrpcServerAddress.create(address), ServerConfiguration.global()).build();
      FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceBlockingStub client =
          FileSystemMasterClientServiceGrpc.newBlockingStub(channel);
      client.listStatus(ListStatusPRequest.getDefaultInstance());
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
