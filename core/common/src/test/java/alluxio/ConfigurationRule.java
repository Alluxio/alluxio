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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A rule for modifying Alluxio configuration during a test suite.
 */
public class ConfigurationRule implements TestRule {
  private final Map<String, String> mKeyValuePairs;

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   */
  public ConfigurationRule(Map<String, String> keyValuePairs) {
    mKeyValuePairs = keyValuePairs;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Map<String, String> originalValues = new HashMap<>();
        for (Entry<String, String> entry : mKeyValuePairs.entrySet()) {
          String key = entry.getKey();
          String value = entry.getValue();
          originalValues.put(key, Configuration.get(key));
          Configuration.set(key, value);
        }
        try {
          statement.evaluate();
        } finally {
          for (Entry<String, String> entry : originalValues.entrySet()) {
            Configuration.set(entry.getKey(), entry.getValue());
          }
        }
      }
    };
  }
}
