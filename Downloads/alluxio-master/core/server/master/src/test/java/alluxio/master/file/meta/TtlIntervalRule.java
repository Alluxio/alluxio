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

package alluxio.master.file.meta;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

/**
 * Rule for temporarily setting the TTL (time to live) checking interval.
 */
public final class TtlIntervalRule implements TestRule {
  private final long mIntervalMs;

  /**
   * @param intervalMs the global checking interval (in ms) to temporarily set
   */
  public TtlIntervalRule(long intervalMs) {
    mIntervalMs = intervalMs;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        long previousValue = TtlBucket.getTtlIntervalMs();
        Whitebox.setInternalState(TtlBucket.class, "sTtlIntervalMs", mIntervalMs);
        try {
          statement.evaluate();
        } finally {
          Whitebox.setInternalState(TtlBucket.class, "sTtlIntervalMs", previousValue);
        }
      }
    };
  }
}
