/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.heartbeat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link HeartbeatContext}.
 */
public class HeartbeatContextTest {

  @Test
  public void timerClassesCheckTest() throws Exception {
    checkNotNull(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING);
    checkNotNull(HeartbeatContext.MASTER_FILE_RECOMPUTATION);
    checkNotNull(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    checkNotNull(HeartbeatContext.MASTER_TTL_CHECK);
    checkNotNull(HeartbeatContext.WORKER_CLIENT);
    checkNotNull(HeartbeatContext.WORKER_LINEAGE_SYNC);
  }

  @Test
  public void checkInstanceOfTest() throws Exception {
    checkInstanceOf(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_FILE_RECOMPUTATION,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_LOST_WORKER_DETECTION,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_CLIENT, HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_LINEAGE_SYNC, HeartbeatContext.SLEEPING_TIMER_CLASS);
  }

  @Test
  public void addNewTimerClassesTest() throws Exception {
    String testSleeping = "TEST_SLEEPING_%s";
    String testScheduled = "TEST_SCHEDULED_%s";

    Map<Class<HeartbeatTimer>, List<String>> timerMap =
        new HashMap<Class<HeartbeatTimer>, List<String>>();
    timerMap.put(HeartbeatContext.SLEEPING_TIMER_CLASS,
        Arrays.asList(String.format(testSleeping, "1"), String.format(testSleeping, "2")));
    timerMap.put(HeartbeatContext.SCHEDULED_TIMER_CLASS,
        Arrays.asList(String.format(testScheduled, "2"), String.format(testScheduled, "1"),
            String.format(testScheduled, "3")));

    for (Class<HeartbeatTimer> timerClass : timerMap.keySet()) {
      for (String name : timerMap.get(timerClass)) {
        HeartbeatContext.setTimerClass(name, timerClass);
        checkInstanceOf(name, timerClass);
      }
    }

    // check that the standard classes are still in place
    timerClassesCheckTest();
    checkInstanceOfTest();
  }

  private void checkNotNull(String name) {
    Assert.assertNotNull(String.format("%s must be valued", name),
        HeartbeatContext.getTimerClass(name));
  }

  private void checkInstanceOf(String name, Class<HeartbeatTimer> timerClass) {
    Assert.assertTrue(
        String.format("%s must be an instance of %s", name, timerClass.getCanonicalName()),
        HeartbeatContext.getTimerClass(name).isAssignableFrom(timerClass));
  }
}
