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

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
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
    Assert.assertFalse(scheduler.schedulerIsIdle());
    scheduler.runNextPendingCommand();
    Assert.assertEquals(1, task.runTimes());
    Assert.assertTrue(scheduler.schedulerIsIdle());
  }

  @Test
  public void schedule() throws Exception {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    scheduler.schedule(task, 5, TimeUnit.HOURS);
    Assert.assertTrue(scheduler.schedulerIsIdle());
    scheduler.jumpAndExecute(5, TimeUnit.HOURS);
    Assert.assertEquals(1, task.runTimes());
  }

  @Test
  public void scheduleAtFixedRate() throws Exception {
    CountTask task = new CountTask();
    ControllableScheduler scheduler = new ControllableScheduler();
    int delayDays = 5;
    int periodDays = 10;
    scheduler.scheduleAtFixedRate(task, delayDays, periodDays, TimeUnit.DAYS);

    scheduler.jumpAndExecute(3 * 24, TimeUnit.HOURS);
    Assert.assertEquals(0, task.runTimes());

    scheduler.jumpAndExecute(2 * 24 * 60, TimeUnit.MINUTES);
    Assert.assertEquals(1, task.runTimes());

    Random random = new Random();
    int daysToJump = random.nextInt(1000000000);
    scheduler.jumpAndExecute(daysToJump, TimeUnit.DAYS);
    Assert.assertEquals(daysToJump / periodDays + 1, task.runTimes());
    Assert.assertTrue(scheduler.schedulerIsIdle());
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
