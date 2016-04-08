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

import com.google.common.base.Throwables;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.List;

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

  @Override
  public Statement apply(final Statement statement, Description description) {
    for (String threadName : mThreads) {
      try {
        Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
            HeartbeatContext.SCHEDULED_TIMER_CLASS);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        } finally {
          for (String threadName : mThreads) {
            Whitebox.invokeMethod(HeartbeatContext.class, "setTimerClass", threadName,
                HeartbeatContext.SLEEPING_TIMER_CLASS);
          }
        }
      }
    };
  }
}
