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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A rule which will temporarily change a heartbeat to being manually scheduled. See
 * {@link HeartbeatScheduler}.
 */
public final class ManuallyScheduleHeartbeat implements TestRule {

  private final List<String> mThreads;

  /**
   * @param threads names of the threads to manually schedule; names are defined in
   *        {@link HeartbeatContext}
   */
  public ManuallyScheduleHeartbeat(List<String> threads) {
    mThreads = threads;
  }

  /**
   * @param threads names of the threads to manually schedule; names are defined in
   *        {@link HeartbeatContext}
   */
  public ManuallyScheduleHeartbeat(String... threads) {
    this(Arrays.asList(threads));
  }

  /**
   * Stores executor threads and corresponding timer class.
   */
  public static class Resource implements AutoCloseable {
    private final List<String> mThreads;
    private final Map<String, Class<? extends HeartbeatTimer>> mPrevious;

    public Resource(List<String> threads) {
      mThreads = threads;
      mPrevious = new HashMap<>();
      for (String threadName : mThreads) {
        try {
          mPrevious.put(threadName, HeartbeatContext.getTimerClass(threadName));
          Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
              HeartbeatContext.SCHEDULED_TIMER_CLASS);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void close() throws Exception {
      for (String threadName : mThreads) {
        Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
            mPrevious.get(threadName));
      }
    }
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (Resource resource = new Resource(mThreads)) {
          statement.evaluate();
        }
      }
    };
  }
}
