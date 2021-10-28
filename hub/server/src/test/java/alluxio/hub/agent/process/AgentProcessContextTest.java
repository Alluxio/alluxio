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

package alluxio.hub.agent.process;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hub.common.InProcessRpcClient;
import alluxio.hub.common.RpcClient;
import alluxio.hub.manager.process.ManagerProcessContext;
import alluxio.hub.manager.rpc.service.ManagerAgentService;
import alluxio.hub.proto.AlluxioCluster;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.HubNodeStatus;
import alluxio.hub.proto.ManagerAgentServiceGrpc;
import alluxio.hub.test.BaseHubTest;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class AgentProcessContextTest extends BaseHubTest {

  private ManagerProcessContext mContext;
  private String mServerName;
  private Server mServer;
  private ManagedChannel mChannel;
  private RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> mClient;

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

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
    mClient = new InProcessRpcClient<>(InetSocketAddress.createUnresolved(mServerName, 0),
        ManagerAgentServiceGrpc::newBlockingStub,
        (addr) -> InProcessChannelBuilder.forName(mServerName).directExecutor().build());
  }

  @After
  public void after() throws Exception {
    mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    mServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void testHeartbeatFail() {
    assertThrows(Exception.class, () -> {
      AgentProcessContext ctx = new AgentProcessContext(getTestSpecificConfig(), mClient);
      ctx.startHeartbeat();
    });
    assertThrows(Exception.class, () -> {
      AgentProcessContext ctx = new AgentProcessContext(getTestSpecificConfig(), mClient);
      ctx.startHeartbeat();
      ctx.close();
      ctx.startHeartbeat();
    });
  }

  @Test
  public void testHeartbeatSuccess() throws Exception {
    InstancedConfiguration config = getTestSpecificConfig();
    AgentProcessContext ctx = new AgentProcessContext(config, mClient);
    CommonUtils.waitFor("cluster size to be > 0", () ->
        mContext.getAlluxioCluster().size() > 0,
        WaitForOptions.defaults().setInterval(100).setTimeoutMs(5000));
    assertEquals(1, mContext.getAlluxioCluster().size());
    AlluxioCluster c = mContext.getAlluxioCluster().toProto();
    AlluxioNodeStatus nodeStatus = c.getNode(0);
    String host = NetworkAddressUtils
        .getConnectHost(NetworkAddressUtils.ServiceType.HUB_AGENT_RPC, config);
    assertEquals(host, nodeStatus.getHostname());
    assertThrows(Exception.class, ctx::startHeartbeat);
  }

  @Test
  public void testRegisterAgentFail() {
    RetryPolicy retry = new ExponentialBackoffRetry(5, 50, 10);
    assertThrows(Exception.class, () -> {
      AgentProcessContext ctx = new AgentProcessContext(getTestSpecificConfig(), mClient);
      mServer.shutdownNow();
      ctx.registerAgent(retry);
    });
  }

  @Test
  public void testRegisterAgentSuccess() throws Exception {
    assertEquals(0, mContext.getHubCluster().size());
    InstancedConfiguration config = getTestSpecificConfig();
    AgentProcessContext ctx = new AgentProcessContext(config, mClient);
    RetryPolicy retry = new ExponentialBackoffRetry(5, 50, 10);
    ctx.registerAgent(retry);
    assertEquals(1, mContext.getHubCluster().size());
    HubNodeStatus node = mContext.getHubCluster().toProto().getNode(0);
    String host =
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.HUB_AGENT_RPC, config);
    assertEquals(host, node.getNode().getHostname());
    assertEquals(config.getInt(PropertyKey.HUB_AGENT_RPC_PORT), node.getNode().getRpcPort());
  }

  public InstancedConfiguration getTestSpecificConfig() throws Exception {
    InstancedConfiguration c = getTestConfig();
    c.set(PropertyKey.HUB_AGENT_RPC_PORT, 5555);
    c.set(PropertyKey.HUB_AGENT_HEARTBEAT_INTERVAL, "1s");
    mTemp.newFolder("bin");
    mTemp.newFile("bin/alluxio-start.sh");
    c.set(PropertyKey.HOME, mTemp.getRoot().getAbsolutePath());
    return c;
  }
}
