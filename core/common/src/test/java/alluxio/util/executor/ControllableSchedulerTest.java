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

package alluxio.util.executor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Tests the {@link ControllableSchedulerTest}.
 */
public class ControllableSchedulerTest {
  @Test
  public void submit() throws Exception {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    scheduler.submit(task);
    assertFalse(scheduler.schedulerIsIdle());
    scheduler.runNextPendingCommand();
    assertEquals(1, task.runTimes());
    assertTrue(scheduler.schedulerIsIdle());
  }

  @Test
  public void schedule() throws Exception {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    scheduler.schedule(task, 5, TimeUnit.HOURS);
    assertTrue(scheduler.schedulerIsIdle());
    scheduler.jumpAndExecute(5, TimeUnit.HOURS);
    assertEquals(1, task.runTimes());
  }

  @Test
  public void scheduleAtFixedRate() throws Exception {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    int delayDays = 5;
    int periodDays = 10;
    scheduler.scheduleAtFixedRate(task, delayDays, periodDays, TimeUnit.DAYS);

    scheduler.jumpAndExecute(3 * 24, TimeUnit.HOURS);
    assertEquals(0, task.runTimes());

    scheduler.jumpAndExecute(2 * 24 * 60, TimeUnit.MINUTES);
    assertEquals(1, task.runTimes());

    Random random = new Random();
    int daysToJump = random.nextInt(1000000000);
    scheduler.jumpAndExecute(daysToJump, TimeUnit.DAYS);
    assertEquals(daysToJump / periodDays + 1, task.runTimes());
    assertTrue(scheduler.schedulerIsIdle());
  }

  @Test
  public void cancel() {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    ScheduledFuture<?> future = scheduler.schedule(task, 5, TimeUnit.HOURS);
    assertTrue(scheduler.schedulerIsIdle());
    future.cancel(true);
    scheduler.jumpAndExecute(5, TimeUnit.HOURS);
    assertEquals(0, task.runTimes());
  }

  /**
   * A count task tracking how many times this task runs.
   */
  private class CountTask implements Runnable {
    private int mNum;

    /**
     * Constructs a new {@link CountTask}.
     */
    CountTask() {
      mNum = 0;
    }

    @Override
    public void run() {
      mNum++;
    }

    /**
     * @return how many times this count task runs
     */
    public int runTimes() {
      return mNum;
    }
  }
}
