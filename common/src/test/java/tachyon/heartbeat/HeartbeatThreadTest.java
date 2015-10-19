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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link HeartbeatThread}. This test uses
 * {@link tachyon.heartbeat.HeartbeatScheduler} to have synchronous tests.
 */
public final class HeartbeatThreadTest {

  private static final String THREAD_NAME = "thread-name";

  private static final int NUMBER_OF_THREADS = 1;

  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

  @Before
  public void setup() {
    HeartbeatContext.setTimerClass(THREAD_NAME, HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @Test
  public void createHeartbeatThreadTest() throws Exception {
    DummyHeartbeatExecutor executor = new DummyHeartbeatExecutor();
    HeartbeatThread ht = new HeartbeatThread(THREAD_NAME, executor, 1);

    // run the HeartbeatThread
    mExecutorService.submit(ht);

    // Wait for the DummyHeartbeatExecutor executor to be ready to execute its heartbeat.
    Assert.assertTrue(HeartbeatScheduler.await(THREAD_NAME, 5, TimeUnit.SECONDS));

    for (int i = 0; i < 100; i ++) {
      HeartbeatScheduler.schedule(THREAD_NAME);
      Assert.assertTrue(HeartbeatScheduler.await(THREAD_NAME, 1, TimeUnit.SECONDS));
    }

    Assert.assertEquals("The executor counter is wrong", 100, executor.getCounter());
  }

  private class DummyHeartbeatExecutor implements HeartbeatExecutor {

    private int mCounter = 0;

    @Override
    public void heartbeat() {
      mCounter ++;
    }

    public int getCounter() {
      return mCounter;
    }
  }
}
