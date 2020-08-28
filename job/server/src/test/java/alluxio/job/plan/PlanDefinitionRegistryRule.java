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

package alluxio.job.plan;

import alluxio.job.JobConfig;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

import java.util.Map;

/**
 * Rule for adding to the job registry and then cleaning up the change.
 */
public final class PlanDefinitionRegistryRule implements TestRule {
  private Class<? extends JobConfig> mConfig;
  private PlanDefinition<?, ?, ?> mDefinition;

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   */
  public PlanDefinitionRegistryRule(Class<? extends JobConfig> config,
                                    PlanDefinition<?, ?, ?> definition) {
    mConfig = config;
    mDefinition = definition;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        @SuppressWarnings("unchecked")
        Map<Class<?>, PlanDefinition<?, ?, ?>> registry =
            Whitebox.getInternalState(PlanDefinitionRegistry.INSTANCE, Map.class);
        registry.put(mConfig, mDefinition);
        try {
          statement.evaluate();
        } finally {
          registry.remove(mConfig);
        }
      }
    };
  }
}
