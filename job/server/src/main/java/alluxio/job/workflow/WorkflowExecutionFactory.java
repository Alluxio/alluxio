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

/**
 * The workflow execution factory.
 * @param <T> The workflow configuration class this workflow execution factory corresponds to
 */
public interface WorkflowExecutionFactory<T extends WorkflowConfig> {

  /**
   * @return the class of the associated workflow config
   */
  Class<T> getWorkflowConfigClass();

  /**
   * Creates a new {@link WorkflowExecution} based on the workflow configuration.
   * @param config the workflow configuration
   * @return new {@link WorkflowExecution}
   */
  WorkflowExecution create(T config);
}
