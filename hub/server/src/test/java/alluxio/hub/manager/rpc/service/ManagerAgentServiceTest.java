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

package alluxio.hub.manager.rpc.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.hub.manager.process.ManagerProcessContext;
import alluxio.hub.proto.AgentHeartbeatRequest;
import alluxio.hub.proto.AgentHeartbeatResponse;
import alluxio.hub.proto.AlluxioCluster;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.HubNodeState;
import alluxio.hub.proto.HubNodeStatus;
import alluxio.hub.proto.ManagerAgentServiceGrpc;
import alluxio.hub.proto.ProcessState;
import alluxio.hub.proto.RegisterAgentRequest;
import alluxio.hub.proto.RegisterAgentResponse;
import alluxio.hub.test.BaseHubTest;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ManagerAgentServiceTest extends BaseHubTest {

  private ManagerProcessContext mContext;
  private String mServerName;
  private Server mServer;
  private ManagedChannel mChannel;
  private ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub mClient;

  @Before
  public void before() throws Exception {
    mContext = getTestManagerContext();
    mServerName = InProcessServerBuilder.generateName();
    mServer = InProcessServerBuilder.forName(mServerName)
        .directExecutor()
        .addService(new ManagerAgentService(mContext))
        .build();
    mServer.start();
    mChannel = InProcessChannelBuilder.forName(mServerName).directExecutor().build();
    mClient = ManagerAgentServiceGrpc.newBlockingStub(mChannel);
  }

  @After
  public void after() throws Exception {
    mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    mServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void registerAgentTest() {
    RegisterAgentRequest req = RegisterAgentRequest.newBuilder()
        .setNode(HubNodeAddress.newBuilder()
            .setHostname("test-host")
            .setRpcPort(54321)
            .build())
        .build();
    RegisterAgentResponse resp = mClient.registerAgent(req);
    assertTrue(resp.hasOk());
    assertEquals(1, mContext.getHubCluster().size());
    assertTrue(mContext.getHubCluster().hosts().contains("test-host"));
    HubNodeStatus node = mContext.getHubCluster().toProto().getNode(0);
    assertTrue(node.hasNode());
    assertTrue(node.hasState());
    assertEquals("test-host", node.getNode().getHostname());
    assertEquals(54321, node.getNode().getRpcPort());
    assertEquals(HubNodeState.ALIVE, node.getState());
  }

  @Test
  public void agentHeartbeatTest() {
    AlluxioNodeStatus s = AlluxioNodeStatus.newBuilder()
        .setHostname(UUID.randomUUID().toString())
        .addProcess(AlluxioProcessStatus.newBuilder()
            .setState(ProcessState.RUNNING)
            .setNodeType(AlluxioNodeType.MASTER)
            .build())
        .build();
    HubNodeAddress addr = HubNodeAddress.newBuilder()
        .setHostname(UUID.randomUUID().toString())
        .setRpcPort(56565)
        .build();
    AgentHeartbeatRequest request = AgentHeartbeatRequest.newBuilder()
        .setAlluxioStatus(s)
        .setHubNode(addr)
        .build();
    AgentHeartbeatResponse resp = mClient.agentHeartbeat(request);
    assertTrue(resp.hasOk());
    assertTrue(resp.getOk());
    assertEquals(1, mContext.getAlluxioCluster().size());
    AlluxioCluster c = mContext.getAlluxioCluster().toProto();
    AlluxioNodeStatus nodeStatus = c.getNode(0);
    assertEquals(s.getHostname(), nodeStatus.getHostname());
    assertEquals(AlluxioNodeType.MASTER, nodeStatus.getProcess(0).getNodeType());
    assertEquals(ProcessState.RUNNING, nodeStatus.getProcess(0).getState());
  }
}
