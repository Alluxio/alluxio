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

package alluxio.job.workflow.composite;

import alluxio.job.workflow.WorkflowExecution;
import alluxio.job.workflow.WorkflowExecutionFactory;

/**
 * The factory for generating {@link CompositeExecution} from {@link CompositeConfig}.
 */
public class CompositeExecutionFactory implements WorkflowExecutionFactory<CompositeConfig> {

  @Override
  public Class<CompositeConfig> getWorkflowConfigClass() {
    return CompositeConfig.class;
  }

  @Override
  public WorkflowExecution create(CompositeConfig config) {
    return new CompositeExecution(config);
  }
}
