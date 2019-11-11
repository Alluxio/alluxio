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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * TODO(bradley).
 */
public enum WorkflowExecutionFactoryRegistry {
  INSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowExecutionFactoryRegistry.class);
  private final Map<Class<?>, WorkflowExecutionFactory<?>> mExecutionFactories = new HashMap<>();

  static {
    INSTANCE.discoverWorkflowExecutionFactories();
  }

  private void discoverWorkflowExecutionFactories() {
    ServiceLoader<WorkflowExecutionFactory> discoveredFactories =
        ServiceLoader.load(WorkflowExecutionFactory.class,
            WorkflowExecutionFactory.class.getClassLoader());

    for (WorkflowExecutionFactory executionFactory : discoveredFactories) {
      mExecutionFactories.put(executionFactory.getWorkflowConfigClass(), executionFactory);
      LOG.info("Loaded execution factory " + executionFactory.getClass().getSimpleName()
          + " for config " + executionFactory.getWorkflowConfigClass().getName());
    }
  }

  /**
   * TODO(bradley).
   * @param workflowConfig TO DO(bradley)
   * @param <T> TODO(bradley)
   * @return TODO(bradley)
   * @throws JobDoesNotExistException TODO(bradley)
   */
  public synchronized <T extends WorkflowConfig> WorkflowExecutionFactory<T> getExecutionFactory(
      T workflowConfig) throws JobDoesNotExistException {
    if (!mExecutionFactories.containsKey(workflowConfig.getClass())) {
      throw new JobDoesNotExistException(ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST
          .getMessage(workflowConfig.getClass().getName()));
    }
    return (WorkflowExecutionFactory<T>) mExecutionFactories.get(workflowConfig.getClass());
  }
}
