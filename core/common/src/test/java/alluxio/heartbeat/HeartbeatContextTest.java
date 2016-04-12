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

package alluxio.heartbeat;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link HeartbeatContext}.
 */
public class HeartbeatContextTest {

  /**
   * Tests the timer classes to be not null.
   */
  @Test
  public void timerClassesCheckTest() {
    checkNotNull(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING);
    checkNotNull(HeartbeatContext.MASTER_FILE_RECOMPUTATION);
    checkNotNull(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
    checkNotNull(HeartbeatContext.MASTER_TTL_CHECK);
    checkNotNull(HeartbeatContext.WORKER_BLOCK_SYNC);
    checkNotNull(HeartbeatContext.WORKER_CLIENT);
    checkNotNull(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC);
    checkNotNull(HeartbeatContext.WORKER_PIN_LIST_SYNC);
  }

  /**
   * Tests that the instances of the context are correctly.
   */
  @Test
  public void checkInstanceOfTest() {
    checkInstanceOf(HeartbeatContext.MASTER_CHECKPOINT_SCHEDULING,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_FILE_RECOMPUTATION,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_LOST_WORKER_DETECTION,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_BLOCK_SYNC, HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_CLIENT, HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
    checkInstanceOf(HeartbeatContext.WORKER_PIN_LIST_SYNC, HeartbeatContext.SLEEPING_TIMER_CLASS);
  }

  /**
   * Tests that a new timer class can be added correctly.
   */
  @Test
  public void addNewTimerClassesTest() throws Exception {
    String testSleeping = "TEST_SLEEPING_%s";
    String testScheduled = "TEST_SCHEDULED_%s";

    Map<Class<? extends HeartbeatTimer>, List<String>> timerMap =
        new HashMap<Class<? extends HeartbeatTimer>, List<String>>();
    timerMap.put(HeartbeatContext.SLEEPING_TIMER_CLASS,
        Arrays.asList(String.format(testSleeping, "1"), String.format(testSleeping, "2")));
    timerMap.put(HeartbeatContext.SCHEDULED_TIMER_CLASS,
        Arrays.asList(String.format(testScheduled, "2"), String.format(testScheduled, "1"),
            String.format(testScheduled, "3")));

    for (Class<? extends HeartbeatTimer> timerClass : timerMap.keySet()) {
      for (String name : timerMap.get(timerClass)) {
        Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", name, timerClass);
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

  private void checkInstanceOf(String name, Class<? extends HeartbeatTimer> timerClass) {
    Assert.assertTrue(
        String.format("%s must be an instance of %s", name, timerClass.getCanonicalName()),
        HeartbeatContext.getTimerClass(name).isAssignableFrom(timerClass));
  }
}
