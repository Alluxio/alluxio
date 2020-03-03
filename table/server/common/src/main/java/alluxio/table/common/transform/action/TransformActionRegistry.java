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

package alluxio.table.common.transform.action;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The registry of transform actions.
 */
public class TransformActionRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TransformActionRegistry.class);

  private static final Map<String, TransformActionFactory> FACTORIES = new HashMap<>();

  static {
    refresh();
  }

  private TransformActionRegistry() {} // prevent instantiation

  /**
   * Creates a new instance of a {@link TransformAction}.
   *
   * @param definition the raw definition of the action
   * @param name the name of the transform action
   * @param args a list of string args
   * @param options a string-string map of options
   * @return a new instance of an action
   */
  public static TransformAction create(String definition, String name, List<String> args,
      Map<String, String> options) {
    TransformActionFactory factory = FACTORIES.get(name);
    if (factory == null) {
      throw new IllegalStateException(
          String.format("TransformActionFactory for name '%s' does not exist.", name));
    }
    return factory.create(definition, args, options);
  }

  /**
   * Refreshes the registry by service loading classes.
   */
  private static void refresh() {
    FACTORIES.clear();
    for (TransformActionFactory factory : ServiceLoader
        .load(TransformActionFactory.class, TransformActionFactory.class.getClassLoader())) {
      TransformActionFactory existingFactory = FACTORIES.get(factory.getName());
      if (existingFactory != null) {
        LOG.warn(
            "Ignoring duplicate transform action '{}' found in factory {}. Existing factory: {}",
            factory.getName(), factory.getClass(), existingFactory.getClass());
      }
      FACTORIES.put(factory.getName(), factory);
    }
    LOG.info("Registered Transform actions: " + String.join(",", FACTORIES.keySet()));
  }
}
