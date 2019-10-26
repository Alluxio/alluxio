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

import alluxio.exception.ExceptionMessage;
import alluxio.job.JobConfig;
import alluxio.table.common.Layout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The definition of an individual transformation action.
 */
public interface TransformAction {
  /**
   * @param base the layout to transform from
   * @param transformed the layout to transform to
   * @return the job configuration for this action
   */
  JobConfig generateJobConfig(Layout base, Layout transformed);

  /**
   * A class to parse transform actions.
   */
  class Parser {
    private static final Pattern COMPONENT_REGEX =
        Pattern.compile("^(?<name>[a-zA-Z]+\\w*)\\((?<args>[a-zA-Z_0-9., \\n\\t]*?)\\)");
    private static final Pattern ARG_REGEX = Pattern.compile("^[a-zA-Z_0-9.]+$");

    /**
     * @param definition the string definition
     * @return the {@link TransformAction} representation
     */
    public static TransformAction parse(String definition) {
      definition = definition.trim();

      String actionName = null;
      List<String> argList = Collections.emptyList();
      Map<String, String> options = new HashMap<>();
      while (!definition.isEmpty()) {
        Matcher matcher = COMPONENT_REGEX.matcher(definition);
        if (!matcher.find()) {
          throw new IllegalArgumentException(
              ExceptionMessage.TRANSFORM_ACTION_PARSE_FAILED.getMessage(definition));
        }
        String name = matcher.group("name");
        String args = matcher.group("args");
        definition = definition.substring(matcher.end()).trim();
        if (!definition.isEmpty()) {
          if (!definition.startsWith(".")) {
            throw new IllegalArgumentException("Missing '.' at: " + definition);
          }
          definition = definition.substring(1);
        }
        if (actionName == null) {
          actionName = name;
          argList = parseArgList(args);
          // continue to parse the next component.
          continue;
        }

        // any additional components must be the 'option' component.
        if (!name.equals("option")) {
          throw new IllegalArgumentException("Expected an 'option()' component at: " + definition);
        }

        // This is an option() component.
        List<String> optionArgs = parseArgList(args);
        if (optionArgs.size() != 2) {
          throw new IllegalArgumentException(
              String.format("Incorrect # args for option() component. args: %s", args));
        }
        options.put(optionArgs.get(0), optionArgs.get(1));
      }

      return TransformActionRegistry.create(definition, actionName, argList, options);
    }

    private static List<String> parseArgList(String args) {
      args = args.trim();
      if (args.isEmpty()) {
        return Collections.emptyList();
      }
      String[] argParts = args.split(",");
      List<String> argList = new ArrayList<>(argParts.length);
      for (String argPart : argParts) {
        argPart = argPart.trim();
        if (!ARG_REGEX.matcher(argPart).matches()) {
          throw new IllegalArgumentException(
              String.format("Failed to parse argument '%s' from '%s'", argPart, args));
        }
        argList.add(argPart);
      }
      return argList;
    }
  }
}
