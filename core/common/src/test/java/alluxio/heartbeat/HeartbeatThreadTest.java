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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

  private static ExecutorService sExecutorService;

  @BeforeClass
  public static void beforeClass() {
    sExecutorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
  }

  @AfterClass
  public static void afterClass() {
    sExecutorService.shutdownNow();
  }

  /**
   * This is a basic test of the heartbeat scheduler logic. It steps through the execution of a
   * single dummy executor.
   *
   * @throws Exception when joining a thread
   */
  @Test
  public void serialHeartbeatThreadTest() throws Exception {
    Thread thread = new DummyHeartbeatTestThread();
    thread.start();
    thread.join();
  }

  /**
   * This is a stress test of the heartbeat scheduler logic. It concurrently steps through the
   * execution of multiple dummy executors.
   *
   * @throws Exception when joining a thread
   */
  @Test
  public void concurrentHeartbeatThreadTest() throws Exception {
    List<Thread> mThreads = new LinkedList<Thread>();

    // Start the threads.
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      Thread thread = new DummyHeartbeatTestThread(i);
      thread.start();
      mThreads.add(thread);
    }

    // Wait for the threads to finish.
    for (Thread thread : mThreads) {
      thread.join();
    }
  }

  private class DummyHeartbeatTestThread extends Thread  {
    private String mThreadName;

    public DummyHeartbeatTestThread() {
      mThreadName = THREAD_NAME;
    }

    public DummyHeartbeatTestThread(int id) {
      mThreadName = THREAD_NAME + "-" + id;
    }

    @Override
    public void run()  {
      try {
        Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", mThreadName,
            HeartbeatContext.SCHEDULED_TIMER_CLASS);

        DummyHeartbeatExecutor executor = new DummyHeartbeatExecutor();
        HeartbeatThread ht = new HeartbeatThread(mThreadName, executor, 1);

        // Run the HeartbeatThread.
        sExecutorService.submit(ht);

        // Wait for the DummyHeartbeatExecutor executor to be ready to execute its heartbeat.
        Assert.assertTrue("Initial wait failed.",
            HeartbeatScheduler.await(mThreadName, 5, TimeUnit.SECONDS));

        final int numIterations = 100000;
        for (int i = 0; i < numIterations; i++) {
          HeartbeatScheduler.schedule(mThreadName);
          Assert.assertTrue("Iteration " + i + " failed.",
              HeartbeatScheduler.await(mThreadName, 5, TimeUnit.SECONDS));
        }

        Assert.assertEquals("The executor counter is wrong.", numIterations, executor.getCounter());
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }

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
