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

package alluxio.table.common.transform;

import alluxio.table.common.transform.action.TransformAction;
import alluxio.table.common.transform.action.TransformActionRegistry;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * The definition of a transformation.
 */
public class TransformDefinition {
  private final String mDefinition;
  private final List<TransformAction> mActions;
  private final Properties mProperties;

  /**
   * The user-provided definition is normalized by:
   * 1. trimming whitespaces and semicolon from the beginning and end;
   * 2. normalize to lower case.
   * @param definition the string definition
   * @param actions the list of actions
   * @param properties the list of properties extracted from definition
   */
  private TransformDefinition(String definition, List<TransformAction> actions,
                              Properties properties) {
    // TODO(bradley): derive definition string from properties or vice versa
    mDefinition = normalize(definition);
    mActions = actions;
    mProperties = properties;
  }

  private String normalize(String definition) {
    definition = definition.trim();
    if (definition.endsWith(";")) {
      definition = definition.substring(0, definition.length() - 1);
    }
    return definition.toLowerCase();
  }

  /**
   * @return the normalized user-provided definition
   */
  public String getDefinition() {
    return mDefinition;
  }

  /**
   * @return the list of actions for this transformation
   */
  public List<TransformAction> getActions() {
    return mActions;
  }

  /**
   * @return the list of properties extracted from the user-provided definition
   */
  public Properties getProperties() {
    return mProperties;
  }

  /**
   * @param definition the string definition
   * @return the {@link TransformDefinition} representation
   */
  public static TransformDefinition parse(String definition) {
    definition = definition.trim();

    if (definition.isEmpty()) {
      return new TransformDefinition(definition, Collections.emptyList(), new Properties());
    }

    // accept semicolon as new lines for inline definitions
    definition = definition.replace(";", "\n");

    final Properties properties = new Properties();

    final StringReader reader = new StringReader(definition);

    try {
      properties.load(reader);
    } catch (IOException e) {
      // The only way this throws an IOException is if the definition is null which isn't possible.
      return new TransformDefinition(definition, Collections.emptyList(), properties);
    }

    final List<TransformAction> actions = TransformActionRegistry.create(properties);

    return new TransformDefinition(definition, actions, properties);
  }
}
