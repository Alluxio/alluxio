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

/**
 * A rule for setting a system property to a value and then restoring the property to its old value.
 */
public final class SystemPropertyRule implements TestRule {
  private final String mPropertyName;
  private final String mValue;

  /**
   * @param propertyName the name of the property to set
   * @param value the value to set it to
   */
  public SystemPropertyRule(String propertyName, String value) {
    mPropertyName = propertyName;
    mValue = value;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (SetAndRestoreSystemProperty c =
            new SetAndRestoreSystemProperty(mPropertyName, mValue)) {
          statement.evaluate();
        }
      }
    };
  }
}
