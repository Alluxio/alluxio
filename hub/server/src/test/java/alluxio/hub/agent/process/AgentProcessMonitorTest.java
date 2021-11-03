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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.manager.process.ManagerProcess;
import alluxio.hub.test.BaseHubTest;
import alluxio.retry.CountingRetry;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;

public class AgentProcessMonitorTest extends BaseHubTest {

  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @Test
  public void testProcessMonitorFailure() throws Exception {
    assertThrows(Exception.class, () -> {
      CountingRetry cntRetry = new CountingRetry(5);
      AgentProcessMonitor.pingService(new InetSocketAddress("127.0.0.1", 6789), cntRetry, 200);
    });
  }

  @Test
  public void testProcessMonitorSuccess() throws Exception {
    InstancedConfiguration c = getTestConfig();
    mTempDir.newFolder("bin");
    mTempDir.newFile("bin/alluxio-start.sh");
    c.set(PropertyKey.HOME, mTempDir.getRoot().getAbsolutePath());
    try (ManagerProcess mProc = new ManagerProcess(c)) {
      InstancedConfiguration agentConf = new InstancedConfiguration(c.copyProperties());
      agentConf.set(PropertyKey.HUB_MANAGER_RPC_PORT, mProc.getRpcPort());
      try (AgentProcess proc = new AgentProcess(agentConf)) {
        CountingRetry cntRetry = new CountingRetry(5);
        AgentProcessMonitor
            .pingService(new InetSocketAddress("127.0.0.1", proc.getRpcPort()), cntRetry, 200);
      }
    } catch (Exception e) {
      fail("Should not have thrown an exception: " + e);
    }
  }
}
