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

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.security.user.UserState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Unit tests for {@link HeartbeatThread}. This test uses
 * {@link alluxio.heartbeat.HeartbeatScheduler} to have synchronous tests.
 *
 * Instructions for testing heartbeats using {@link ScheduledTimer}.
 *
 * Using {@link ScheduledTimer} for testing heartbeats removes the dependence on sleep() for
 * thread timing. Instead, test cases can dictate an ordering between threads. Here are the
 * steps required for using {@link ScheduledTimer}.
 *
 * 1. Set the timer class to use {@link ScheduledTimer}. This tells the heartbeat context
 * that the given heartbeat thread will be using a schedule based timer, instead of a
 * sleeping based timer. This is done with:
 *
 * HeartbeatContext.setTimerClass(THREAD_NAME, HeartbeatContext.SCHEDULED_TIMER_CLASS);
 *
 * 2. Call await() to make sure the first heartbeat is ready to run. In tests, the result
 * should be within an assertTrue(). Here is an example:
 *
 * Assert.assertTrue(HeartbeatScheduler.await(THREAD_NAME, 5, TimeUnit.SECONDS));
 *
 * 3. Call schedule() and await() each time the heartbeat thread should be triggered. Test
 * cases can now dictate the behavior of the heartbeat thread by calling schedule() and
 * await() right afterwards. After await() returns successfully, it is guaranteed that the
 * heartbeat thread ran exactly once. Here is an example of scheduling the heartbeat to run:
 *
 * HeartbeatScheduler.schedule(THREAD_NAME);
 * Assert.assertTrue(HeartbeatScheduler.await(THREAD_NAME, 5, TimeUnit.SECONDS));
 */
public final class HeartbeatThreadTest {

  private static final String THREAD_NAME = "heartbeat-thread-test-thread-name";

  private static final int NUMBER_OF_THREADS = 10;

  private ExecutorService mExecutorService;

  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    mExecutorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
  }

  @After
  public void after() {
    mExecutorService.shutdownNow();
  }

  /**
   * This is a basic test of the heartbeat scheduler logic. It steps through the execution of a
   * single dummy executor.
   */
  @Test
  public void serialHeartbeatThread() throws Exception {
    FutureTask<Void> task = new FutureTask<>(new DummyHeartbeatTestCallable());
    Thread thread = new Thread(task);
    thread.start();
    thread.join();
    task.get();
  }

  /**
   * This is a stress test of the heartbeat scheduler logic. It concurrently steps through the
   * execution of multiple dummy executors.
   */
  @Test
  public void concurrentHeartbeatThread() throws Exception {
    List<FutureTask<Void>> tasks = new ArrayList<>();

    // Start the threads.
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      FutureTask<Void> task = new FutureTask<>(new DummyHeartbeatTestCallable(i));
      Thread thread = new Thread(task);
      thread.start();
      tasks.add(task);
    }

    // Wait for the threads to finish.
    for (FutureTask<Void> task: tasks) {
      task.get();
    }
  }

  /**
   * Executes a dummy heartbeat executor using {@link HeartbeatScheduler}.
   */
  private class DummyHeartbeatTestCallable implements Callable<Void>  {
    private final String mThreadName;

    /**
     * Creates a new {@link DummyHeartbeatTestCallable}.
     */
    public DummyHeartbeatTestCallable() {
      mThreadName = THREAD_NAME;
    }

    /**
     * Creates a new {@link DummyHeartbeatTestCallable}.
     *
     * @param id the thread id
     */
    public DummyHeartbeatTestCallable(int id) {
      mThreadName = THREAD_NAME + "-" + id;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      try (ManuallyScheduleHeartbeat.Resource r =
          new ManuallyScheduleHeartbeat.Resource(Arrays.asList(mThreadName))) {
        DummyHeartbeatExecutor executor = new DummyHeartbeatExecutor();
        HeartbeatThread ht = new HeartbeatThread(mThreadName, executor, 1, mConfiguration,
            UserState.Factory.create(mConfiguration));

        // Run the HeartbeatThread.
        mExecutorService.submit(ht);

        final int numIterations = 5000;
        for (int i = 0; i < numIterations; i++) {
          HeartbeatScheduler.execute(mThreadName);
        }

        assertEquals("The executor counter is wrong.", numIterations, executor.getCounter());
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
      return null;
    }
  }

  /**
   * The dummy heartbeat executor.
   */
  private class DummyHeartbeatExecutor implements HeartbeatExecutor {

    private int mCounter = 0;

    @Override
    public void heartbeat() {
      mCounter++;
    }

    @Override
    public void close() {
      // Nothing to clean up
    }

    public int getCounter() {
      return mCounter;
    }
  }
}
