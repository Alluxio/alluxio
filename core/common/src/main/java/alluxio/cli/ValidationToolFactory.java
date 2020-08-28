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

package alluxio.cli;

import java.util.Map;

/**
 * The validation tool factory interface.
 */
public interface ValidationToolFactory {

  /**
   * @return the type of validation tool for the factory
   */
  String getType();

  /**
   * Creates a new instance of {@link ValidationTool}.
   * Creation must not interact with external services.
   *
   * @param configMap a map from config key to config value
   * @return a new validation tool instance
   */
  ValidationTool create(Map<Object, Object> configMap);
}
