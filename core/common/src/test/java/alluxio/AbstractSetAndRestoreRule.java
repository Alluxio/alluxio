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

package alluxio;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A rule for test rule to set and restore for each test case.
 */
@NotThreadSafe
public abstract class AbstractSetAndRestoreRule implements TestRule {

  /**
   * Sets the value for the rule.
   *
   * @throws Exception if error happens
   */
  protected abstract void set() throws Exception;

  /**
   * Restores the value for the rule.
   *
   * @throws Exception if error happens
   */
  protected abstract void restore() throws Exception;

  /**
   * @return an AutoCloseable resource that makes the modification of the rule on construction and
   *         restore the rule to the previous value on close.
   */
  public AutoCloseable toResource() throws Exception {
    return new AutoCloseable() {
      {
        set();
      }

      @Override
      public void close() throws Exception {
        restore();
      }
    };
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (AutoCloseable s = toResource()) {
          statement.evaluate();
        }
      }
    };
  }
}
