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

package alluxio.heartbeat;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link HeartbeatContext}.
 */
public class HeartbeatContextTest {
  @Test
  public void allThreadsUseSleepingTimer() {
    for (String threadName : HeartbeatContext.getTimerClasses().keySet()) {
      Class<? extends HeartbeatTimer> timerClass = HeartbeatContext.getTimerClass(threadName);
      Assert.assertTrue(timerClass.isAssignableFrom(SleepingTimer.class));
    }
  }

  @Test
  public void canUpdateToScheduledTimer() throws Exception {
    try (ManuallyScheduleHeartbeat.Resource h = new ManuallyScheduleHeartbeat.Resource(
        ImmutableList.of(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING))) {
      Class<? extends HeartbeatTimer> timerClass =
          HeartbeatContext.getTimerClass(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING);
      Assert.assertTrue(timerClass.isAssignableFrom(ScheduledTimer.class));
    }
  }
}
