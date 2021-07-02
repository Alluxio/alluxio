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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.TestPlanConfig;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;

/**
 * Unit tests for {@link PlanDefinitionRegistry}.
 */
public final class PlanDefinitionRegistryTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void getPlanDefinitionTest() throws Exception {
    PlanDefinition<TestPlanConfig, ?, ?> definition = PlanDefinitionRegistry.INSTANCE
        .getJobDefinition(new TestPlanConfig("test"));
    Assert.assertTrue(definition instanceof TestPlanDefinition);
  }

  @Test
  public void getNonexistingPlanDefinitionTest() throws Exception {
    DummyPlanConfig planConfig = new DummyPlanConfig();

    mThrown.expect(JobDoesNotExistException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(planConfig.getName()));

    PlanDefinitionRegistry.INSTANCE.getJobDefinition(planConfig);
  }

  class DummyPlanConfig implements PlanConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }

    @Override
    public Collection<String> affectedPaths() {
      return Collections.EMPTY_LIST;
    }
  }
}
