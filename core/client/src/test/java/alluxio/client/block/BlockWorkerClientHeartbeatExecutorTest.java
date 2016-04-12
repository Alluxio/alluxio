/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests for the {@link BlockWorkerClientHeartbeatExecutor} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockWorkerClient.class)
public class BlockWorkerClientHeartbeatExecutorTest {

  /**
   * Tests to ensure heartbeat calls BlockWorkerClient.periodicHeartbeat.
   *
   * @throws Exception when the periodicHeartbeat is not called once
   */
  @Test
  public void heartbeatCallsPeriodicHeartbeat() throws Exception {
    BlockWorkerClient mock = PowerMockito.mock(BlockWorkerClient.class);
    BlockWorkerClientHeartbeatExecutor heartbeatExecutor =
        new BlockWorkerClientHeartbeatExecutor(mock);

    heartbeatExecutor.heartbeat();
    heartbeatExecutor.close();

    Mockito.verify(mock).periodicHeartbeat();
  }
}
