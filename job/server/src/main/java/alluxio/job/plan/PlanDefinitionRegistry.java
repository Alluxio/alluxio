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
import alluxio.job.JobConfig;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The central registry of all the job definitions.
 */
@ThreadSafe
public enum PlanDefinitionRegistry {
  INSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(PlanDefinitionRegistry.class);
  private final Map<Class<?>, PlanDefinition<?, ?, ?>> mDefinitions = new HashMap<>();

  // all the static fields must be defined before the static initialization
  static {
    // Discover and register the available definitions
    INSTANCE.discoverJobDefinitions();
  }

  @SuppressWarnings("unchecked")
  private void discoverJobDefinitions() {
    @SuppressWarnings({"rawtypes"})
    ServiceLoader<PlanDefinition> discoveredDefinitions =
        ServiceLoader.load(PlanDefinition.class, PlanDefinition.class.getClassLoader());

    for (@SuppressWarnings("rawtypes") PlanDefinition definition : discoveredDefinitions) {
      add(definition.getJobConfigClass(), definition);
      LOG.info("Loaded job definition " + definition.getClass().getSimpleName() + " for config "
          + definition.getJobConfigClass().getName());
    }
  }

  private PlanDefinitionRegistry() {}

  /**
   * Adds a mapping from the job configuration to the definition.
   */
  private <T extends JobConfig> void add(Class<T> jobConfig,
                                         PlanDefinition<T, ?, ?> definition) {
    mDefinitions.put(jobConfig, definition);
  }

  /**
   * Gets the job definition from the job configuration.
   *
   * @param jobConfig the job configuration
   * @param <T> the job configuration class
   * @return the job definition corresponding to the configuration
   * @throws JobDoesNotExistException when the job definition does not exist
   */
  @SuppressWarnings("unchecked")
  public synchronized <T extends JobConfig> PlanDefinition<T, Serializable, Serializable>
        getJobDefinition(T jobConfig) throws JobDoesNotExistException {
    if (!mDefinitions.containsKey(jobConfig.getClass())) {
      throw new JobDoesNotExistException(
          ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));
    }
    try {
      return mDefinitions.get(jobConfig.getClass()).getClass().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }
}
