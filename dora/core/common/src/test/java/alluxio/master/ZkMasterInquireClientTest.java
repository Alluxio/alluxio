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
import alluxio.Constants;
import alluxio.exception.status.UnavailableException;
import alluxio.util.io.PathUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

public class ZkMasterInquireClientTest {

  private static final String ELECTION_PATH = "/election";
  private static final String LEADER_PATH = "/leader";
  private static final String LOOPBACK_IP = "127.0.0.1";
  private static final int TESTING_SERVER_PORT = 11111;
  private static final int INQUIRE_RETRY_COUNT = 2;
  private static final boolean ZOOKEEPER_AUTH_ENABLED = true;

  private TestingServer mZkServer;

  @Before
  public void before() throws Exception {
    mZkServer =
        new TestingServer(TESTING_SERVER_PORT, AlluxioTestDirectory.createTemporaryDirectory("zk"));
  }

  @After
  public void after() throws Exception {
    if (mZkServer != null) {
      mZkServer.close();
    }
  }

  @Test
  public void testNoParticipant() throws Exception {
    // Create zk inquire client.
    MasterInquireClient zkInquirer = ZkMasterInquireClient.getClient(mZkServer.getConnectString(),
        ELECTION_PATH, LEADER_PATH, INQUIRE_RETRY_COUNT, ZOOKEEPER_AUTH_ENABLED);
    // Create curator client for manipulating the leader path.
    CuratorFramework client = CuratorFrameworkFactory.newClient(mZkServer.getConnectString(),
        new ExponentialBackoffRetry(Constants.SECOND_MS, INQUIRE_RETRY_COUNT));
    // Create the leader path with no participants.
    client.start();
    client.create().forPath(LEADER_PATH);
    client.close();
    // Query should fail with no leader under path.
    boolean queryFailed = false;
    try {
      zkInquirer.getPrimaryRpcAddress();
    } catch (UnavailableException e) {
      // Expected.
      queryFailed = true;
    }
    Assert.assertTrue("Master query should have been failed.", queryFailed);
  }

  @Test
  public void testNoPath() throws Exception {
    // Create zk inquire client.
    MasterInquireClient zkInquirer = ZkMasterInquireClient.getClient(mZkServer.getConnectString(),
        ELECTION_PATH, LEADER_PATH, INQUIRE_RETRY_COUNT, ZOOKEEPER_AUTH_ENABLED);
    // Query should fail with no leader path created.
    boolean queryFailed = false;
    try {
      zkInquirer.getPrimaryRpcAddress();
    } catch (UnavailableException e) {
      // Expected.
      queryFailed = true;
    }
    Assert.assertTrue("Master query should have been failed.", queryFailed);
  }

  @Test
  public void testSingleLeader() throws Exception {
    // Create zk inquire client.
    MasterInquireClient zkInquirer = ZkMasterInquireClient.getClient(mZkServer.getConnectString(),
        ELECTION_PATH, LEADER_PATH, INQUIRE_RETRY_COUNT, ZOOKEEPER_AUTH_ENABLED);
    // Create curator client for manipulating the leader path.
    CuratorFramework client = CuratorFrameworkFactory.newClient(mZkServer.getConnectString(),
        new ExponentialBackoffRetry(Constants.SECOND_MS, INQUIRE_RETRY_COUNT));
    // Create the leader path with single(localhost) participant.
    InetSocketAddress localLeader = InetSocketAddress.createUnresolved(LOOPBACK_IP, 12345);
    client.start();
    client.create().forPath(LEADER_PATH);
    client.create().forPath(PathUtils.concatPath(LEADER_PATH, localLeader));
    client.close();
    // Verify that leader is fetched.
    Assert.assertEquals(localLeader, zkInquirer.getPrimaryRpcAddress());
  }

  @Test
  public void testMultipleLeaders() throws Exception {
    // Create zk inquire client.
    MasterInquireClient zkInquirer = ZkMasterInquireClient.getClient(mZkServer.getConnectString(),
        ELECTION_PATH, LEADER_PATH, INQUIRE_RETRY_COUNT, ZOOKEEPER_AUTH_ENABLED);
    // Create curator client for manipulating the leader path.
    CuratorFramework client = CuratorFrameworkFactory.newClient(mZkServer.getConnectString(),
        new ExponentialBackoffRetry(Constants.SECOND_MS, INQUIRE_RETRY_COUNT));
    // Create the leader path with multiple participants.
    InetSocketAddress localLeader1 = InetSocketAddress.createUnresolved(LOOPBACK_IP, 12345);
    InetSocketAddress localLeader2 = InetSocketAddress.createUnresolved(LOOPBACK_IP, 54321);
    client.start();
    client.create().forPath(LEADER_PATH);
    client.create().forPath(PathUtils.concatPath(LEADER_PATH, localLeader1));
    client.create().forPath(PathUtils.concatPath(LEADER_PATH, localLeader2));
    client.close();
    // Verify that the latest written value is fetched.
    Assert.assertEquals(localLeader2, zkInquirer.getPrimaryRpcAddress());
  }
}
