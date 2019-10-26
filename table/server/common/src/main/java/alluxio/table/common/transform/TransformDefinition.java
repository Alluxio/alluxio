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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The definition of a transformation.
 */
public class TransformDefinition {
  private final String mDefinition;
  private final List<TransformAction> mActions;

  /**
   * The user-provided definition is normalized by:
   * 1. trimming whitespaces and semicolon from the beginning and end;
   * 2. normalize to lower case.
   *
   * @param definition the string definition
   * @param actions the list of actions
   */
  private TransformDefinition(String definition, List<TransformAction> actions) {
    mDefinition = normalize(definition);
    mActions = actions;
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
   * @param definition the string definition
   * @return the {@link TransformDefinition} representation
   */
  public static TransformDefinition parse(String definition) {
    // TODO(gpang): use real lexer/parser
    definition = definition.trim();

    if (definition.isEmpty()) {
      return new TransformDefinition(definition, Collections.emptyList());
    }

    // ';' separates actions
    String[] parts = definition.split(";");
    List<TransformAction> actions = new ArrayList<>(parts.length);
    for (String actionPart : parts) {
      actions.add(TransformAction.Parser.parse(actionPart));
    }

    return new TransformDefinition(definition, actions);
  }
}
