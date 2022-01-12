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

package alluxio.hub.agent.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.agent.process.AgentProcessContext;
import alluxio.hub.agent.process.TestAgentProcessContextFactory;
import alluxio.hub.test.BaseHubTest;

import org.junit.Test;

import java.net.InetSocketAddress;

public class AgentRpcServerTest extends BaseHubTest {

  @Test
  public void testSimpleStartup() throws Exception {
    AlluxioConfiguration conf = getTestSpecificTestConfig();
    AgentRpcServer server = new AgentRpcServer(conf,
        TestAgentProcessContextFactory.simpleContext(conf));
    // Server is running if assigned port is != 0
    assertTrue(server.isServing());
    assertTrue(server.getPort() != 0);
  }

  @Test
  public void testGetAddress() {
    InstancedConfiguration c =
        new InstancedConfiguration(ServerConfiguration.global().copyProperties());
    c.set(PropertyKey.HUB_AGENT_RPC_BIND_HOST, "127.6.5.4");
    c.set(PropertyKey.HUB_AGENT_RPC_PORT, 0);
    InetSocketAddress addr = AgentRpcServer.getConfiguredAddress(c);
    assertEquals("127.6.5.4", addr.getHostString());
    assertEquals(0, addr.getPort());
  }

  @Test
  public void testAwaitTermination() throws Exception {
    AlluxioConfiguration conf = getTestSpecificTestConfig();
    AgentProcessContext ctx = TestAgentProcessContextFactory.simpleContext(conf);
    AgentRpcServer server = new AgentRpcServer(conf, ctx);
    Thread t = new Thread(server::close); // shut down server once started.
    assertTrue(server.isServing());
    t.start();
    server.awaitTermination();
    t.join();
    assertFalse(server.isServing());
  }

  private InstancedConfiguration getTestSpecificTestConfig() {
    InstancedConfiguration c =
            new InstancedConfiguration(ServerConfiguration.global().copyProperties());
    c.set(PropertyKey.HUB_AGENT_RPC_PORT, 0);
    return c;
  }
}
