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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The validation tool interface.
 */
public interface ValidationTool {

  /**
   * @return a mapping of task names to tasks that this tool can run
   */
  Map<String, ValidationTask> getTasks();

  /**
   * Runs validation tests.
   *
   * @return a json string of the test results
   */
  List<ValidationTaskResult> runAllTests() throws InterruptedException;

  /**
   * Converts the results of the {@link #runAllTests()} method into a map for digestion by
   * other components.
   *
   * @param results a result from a validation tool
   * @return a map representing the result input
   */
  static ValidationResults convertResults(
      List<ValidationTaskResult> results) {
    // group by state
    Map<ValidationUtils.State, List<ValidationTaskResult>> map = new HashMap<>();
    results.forEach(r -> map.computeIfAbsent(r.getState(), k -> new ArrayList<>()).add(r));
    ValidationResults finalResults = new ValidationResults();
    finalResults.setResult(map);
    return finalResults;
  }

  /**
   * Convert to Json format of the validation result.
   *
   * @param map result stored in a map
   * @return a string containing json representation of the result
   */
  static String toJson(ValidationResults map) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(map);
  }
}
