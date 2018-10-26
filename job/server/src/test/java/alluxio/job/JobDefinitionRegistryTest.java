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

package alluxio.job;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link JobDefinitionRegistry}.
 */
public final class JobDefinitionRegistryTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void getJobDefinitionTest() throws Exception {
    JobDefinition<TestJobConfig, ?, ?> definition = JobDefinitionRegistry.INSTANCE
        .getJobDefinition(new TestJobConfig("test"));
    Assert.assertTrue(definition instanceof TestJobDefinition);
  }

  @Test
  public void getNonexistingJobDefinitionTest() throws Exception {
    DummyJobConfig jobConfig = new DummyJobConfig();

    mThrown.expect(JobDoesNotExistException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));

    JobDefinitionRegistry.INSTANCE.getJobDefinition(jobConfig);
  }

  class DummyJobConfig implements JobConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }
  }
}
