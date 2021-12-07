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

package alluxio.hub.manager.util;

import static alluxio.hub.manager.util.HubTestUtils.generateNodeAddress;
import static alluxio.hub.manager.util.HubTestUtils.hubStatusToAlluxioStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import alluxio.TestLoggerRule;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.HubNodeState;
import alluxio.hub.test.BaseHubTest;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HubClusterTest extends BaseHubTest {

  private HubCluster mCluster;
  private Random mRand;
  private ScheduledExecutorService mSvc;

  @Rule public TestLoggerRule mLogger = new TestLoggerRule();

  @Before
  public void before() {
    // A direct executor is required to make sure event handlers are run inline with the changes
    // to the cluster.
    mSvc = Executors.newSingleThreadScheduledExecutor();
    mCluster = new HubCluster(mSvc, MoreExecutors.newDirectExecutorService(), 100000,
            1000000,
        new AlluxioCluster(mSvc));
    mRand = new Random();
  }

  @After
  public void after() {
    mCluster.close();
    mSvc.shutdownNow();
  }

  @Test
  public void testAdd() {
    HubNodeAddress s = HubNodeAddress.newBuilder().setHostname("test-host").setRpcPort(1234)
            .build();
    mCluster.add(s);
    assertEquals(1, mCluster.size());
    HubNodeAddress s2 =
        HubNodeAddress.newBuilder().setHostname("test-host").setRpcPort(12345).build();
    mCluster.add(s2);
    assertEquals(2, mCluster.size());
  }

  @Test
  public void testAddSame() {
    HubNodeAddress s = HubNodeAddress.newBuilder().setHostname("test-host").setRpcPort(1234)
            .build();
    mCluster.add(s);
    assertEquals(1, mCluster.size());
    HubNodeAddress s2 =
        HubNodeAddress.newBuilder().setHostname("test-host").setRpcPort(1234).build();
    mCluster.add(s2);
    assertEquals(1, mCluster.size());
  }

  @Test
  public void testToProtoEmpty() {
    alluxio.hub.proto.AlluxioCluster cluster =
        new AlluxioCluster(MoreExecutors.newDirectExecutorService()).toProto();
    List<AlluxioNodeStatus> l = cluster.getNodeList();
    assertEquals(0, l.size());
  }

  @Test
  public void testToProtoNotEmpty() {
    int limit = 4;
    mRand.ints(limit).mapToObj(i -> generateNodeAddress()).forEach(o -> mCluster.add(o));
    alluxio.hub.proto.HubCluster proto = mCluster.toProto();
    assertEquals(limit, proto.getNodeCount());
    proto.getNodeList().forEach(node -> {
      assertTrue(node.hasNode());
      assertTrue(node.getNode().hasHostname());
      assertTrue(node.getNode().hasRpcPort());
      assertTrue(node.hasState());
    });
  }

  @Test
  public void testRemove() {
    HubNodeAddress a = generateNodeAddress();
    mCluster.add(a);
    assertEquals(1, mCluster.size());
    mCluster.remove(a);
    assertEquals(0, mCluster.size());
    mCluster.add(a);
    assertEquals(1, mCluster.size());
  }

  @Test
  public void testLostNode() throws InterruptedException {
    HubNodeAddress addr = generateNodeAddress();
    mCluster = new HubCluster(mSvc, 250, 400);
    mCluster.heartbeat(addr);
    assertEquals(1, mCluster.size());
    Thread.sleep(255); // wait until the lost time has passed
    mCluster.scanNodes(); // scan over the nodes
    assertEquals(1, mCluster.size());
    assertEquals(HubNodeState.LOST, mCluster.toProto().getNode(0).getState());
    Thread.sleep(150);
    mCluster.scanNodes(); // scan over the nodes again
    // the node should be deleted
    assertEquals(0, mCluster.size());
  }

  @Test
  public void testLostThenAlive() throws InterruptedException {
    HubNodeAddress addr = generateNodeAddress();
    mCluster = new HubCluster(mSvc, 100, 400);
    mCluster.heartbeat(addr);
    assertEquals(1, mCluster.size());
    Thread.sleep(105); // wait until the lost time has passed
    mCluster.scanNodes(); // scan over the nodes
    assertEquals(1, mCluster.size());
    assertEquals(HubNodeState.LOST, mCluster.toProto().getNode(0).getState());
    mCluster.heartbeat(addr);
    mCluster.scanNodes(); // scan over the nodes again
    // the node should be deleted
    assertEquals(1, mCluster.size());
    assertEquals(HubNodeState.ALIVE, mCluster.toProto().getNode(0).getState());
  }

  @Test
  public void testExecAllNodes() throws Exception {
    List<HubNodeAddress> nodes =
        Stream.of(1, 2, 3, 4, 5).map(i -> generateNodeAddress()).collect(Collectors.toList());
    nodes.forEach(mCluster::add);
    try (MockedStatic<GrpcChannelBuilder> mock = Mockito.mockStatic(GrpcChannelBuilder.class)) {
      GrpcChannelBuilder mockBuilder = mock(GrpcChannelBuilder.class);
      doReturn(mockBuilder).when(mockBuilder).setSubject(any());
      doReturn(mock(GrpcChannel.class)).when(mockBuilder).build();
      mock.when(() -> GrpcChannelBuilder.newBuilder(any(), any())).thenReturn(mockBuilder);
      AtomicInteger i = new AtomicInteger(0);
      // Test node single exec
      mCluster.exec(Collections.singleton(nodes.get(0)), getTestConfig(),
          (client) -> i.incrementAndGet(), mSvc);
      assertEquals(1, i.get());
      i.set(0);
      // Test multiple node exec
      mCluster.exec(new HashSet<>(nodes.subList(0, 3)), getTestConfig(),
          (client) -> i.incrementAndGet(), mSvc);
      assertEquals(3, i.get());
      i.set(0);
      // Test 0 node exec
      mCluster
          .exec(Collections.emptySet(), getTestConfig(), (client) -> i.incrementAndGet(),
              mSvc);
      assertEquals(0, i.get());
      i.set(0);
      // test non existing node exec
      mCluster.exec(Collections.singleton(generateNodeAddress()), getTestConfig(),
          (client) -> i.incrementAndGet(), mSvc);
      assertEquals(0, i.get());
    }
  }

  @Test
  public void testNodesFromAlluxio() {
    AlluxioCluster ac = mCluster.getAlluxioCluster();
    HubNodeAddress addr1 = generateNodeAddress();
    HubNodeAddress addr2 = generateNodeAddress();
    mCluster.add(addr1);
    mCluster.add(addr2);
    ac.heartbeat(hubStatusToAlluxioStatus(addr1, AlluxioNodeType.MASTER,
            AlluxioNodeType.JOB_MASTER));
    ac.heartbeat(hubStatusToAlluxioStatus(addr2, AlluxioNodeType.WORKER,
            AlluxioNodeType.JOB_WORKER));
    assertTrue(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.MASTER).contains(addr1));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.MASTER).contains(addr2));
    assertTrue(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.JOB_MASTER).contains(addr1));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.JOB_MASTER).contains(addr2));
    assertTrue(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.WORKER).contains(addr2));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.WORKER).contains(addr1));
    assertTrue(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.JOB_WORKER).contains(addr2));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.JOB_WORKER).contains(addr1));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.PROXY).contains(addr1));
    assertFalse(mCluster.nodesFromAlluxio(ac, AlluxioNodeType.PROXY).contains(addr2));
  }
}
