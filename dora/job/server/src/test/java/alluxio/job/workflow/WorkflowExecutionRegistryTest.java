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

package alluxio.job.workflow;

import static org.junit.Assert.assertTrue;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.job.workflow.composite.CompositeExecution;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;

public class WorkflowExecutionRegistryTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void getExecutionFactoryTest() throws Exception {
    WorkflowExecution execution = WorkflowExecutionRegistry.INSTANCE.getExecution(
        new CompositeConfig(Lists.newArrayList(), true));

    assertTrue(execution instanceof CompositeExecution);
  }

  @Test
  public void getNonexistingExecution() throws Exception {
    DummyWorkflowConfig config = new DummyWorkflowConfig();

    mThrown.expect(JobDoesNotExistException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(config.getName()));

    WorkflowExecutionRegistry.INSTANCE.getExecution(config);
  }

  class DummyWorkflowConfig implements WorkflowConfig {

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
