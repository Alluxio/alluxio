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

package tachyon;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link HeartbeatThread}. This test runs
 * {@link HeartbeatThreadTest#NUMBER_OF_THREADS} {@link HeartbeatThread} in parallel with different
 * {@link HeartbeatThread#mFixedExecutionIntervalMs}. It checks that every scheduled thread executes
 * a heartbeat for the specified interval with a variance lesser than
 * {@link HeartbeatThreadTest#VARIANCE_UPPER_BOUND}.
 */
public final class HeartbeatThreadTest {

  private static final String THREAD_NAME = "thread-name-%s";

  private static final int NUMBER_OF_THREADS = 3;

  private static final long EXECUTION_TIME = 2000;

  private static final double VARIANCE_UPPER_BOUND = 5.0;

  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

  private final List<DummyHeartbeatExecutor> mHeartbeatExecutors =
      new ArrayList<DummyHeartbeatExecutor>();

  @Test
  public void createHeartbeatThreadTest() throws Exception {

    for (int i = 1; i <= NUMBER_OF_THREADS; i ++) {
      long interval = i * 10;
      mExecutorService.submit(createHeartbeatThread(String.format(THREAD_NAME, i), interval));
    }

    Thread.sleep(EXECUTION_TIME);
    mExecutorService.shutdown();

    for (DummyHeartbeatExecutor hbe : mHeartbeatExecutors) {
      Assert.assertTrue("The HeartbeatThread has been scheduled with a bad precision",
          hbe.getVariance() < VARIANCE_UPPER_BOUND);
    }
  }

  private HeartbeatThread createHeartbeatThread(String threadName, long fixedExecutionIntervalMs) {
    DummyHeartbeatExecutor heartbeatExecutor = new DummyHeartbeatExecutor(threadName);
    mHeartbeatExecutors.add(heartbeatExecutor);
    return new HeartbeatThread(threadName, heartbeatExecutor, fixedExecutionIntervalMs);
  }

  private class DummyHeartbeatExecutor implements HeartbeatExecutor {

    private final String mName;

    private int mCount;

    private long mStartingTimeMs = -1;

    private long mLastExecutionTimeMs = -1;

    private List<Long> mIntervals = new ArrayList<Long>();

    public DummyHeartbeatExecutor(String name) {
      mName = name;
    }

    @Override
    public void heartbeat() {
      if (mStartingTimeMs == -1) {
        mStartingTimeMs = System.currentTimeMillis();
        mLastExecutionTimeMs = mStartingTimeMs;
      } else {
        long mCurrTime = System.currentTimeMillis();
        getIntervals().add(mCurrTime - mLastExecutionTimeMs);
        mLastExecutionTimeMs = mCurrTime;
        mCount ++;
      }
    }

    /**
     * This method is synchronized to avoid to get a
     * {@link java.util.ConcurrentModificationException}.
     */
    public synchronized List<Long> getIntervals() {
      return mIntervals;
    }

    public double getMean() {
      return mCount != 0 ? (mLastExecutionTimeMs - mStartingTimeMs) * 1d / mCount : 0d;
    }

    public String getName() {
      return mName;
    }

    private double getVariance() {
      double mean = getMean();
      double temp = 0d;
      for (Long x : getIntervals()) {
        temp += (mean - x) * (mean - x);
      }
      return temp / getIntervals().size();
    }
  }
}
