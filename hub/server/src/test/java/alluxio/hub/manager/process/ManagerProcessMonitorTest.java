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

package alluxio.hub.manager.process;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.hub.test.BaseHubTest;
import alluxio.retry.CountingRetry;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;

public class ManagerProcessMonitorTest extends BaseHubTest {

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
      ManagerProcessMonitor.pingService(new InetSocketAddress("127.0.0.1", 6789), cntRetry, 200);
    });
  }

  @Test
  public void testProcessMonitorSuccess() throws Exception {
    InstancedConfiguration conf = getTestConfig();
    try (ManagerProcess proc = new ManagerProcess(conf)) {
      CountingRetry cntRetry = new CountingRetry(5);
      CommonUtils.waitFor("rpc server is serving", proc::isServing,
          WaitForOptions.defaults().setTimeoutMs(10000));
      assertNotEquals(0, proc.getRpcPort());
      ManagerProcessMonitor
          .pingService(new InetSocketAddress("127.0.0.1", proc.getRpcPort()), cntRetry, 2000);
    } catch (Exception e) {
      fail("Should not have thrown an exception: " + e);
    }
  }
}
