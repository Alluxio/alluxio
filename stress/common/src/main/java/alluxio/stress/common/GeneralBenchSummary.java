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

package alluxio.stress.common;

import alluxio.stress.Summary;
import alluxio.stress.TaskResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * abstract class for BenchSummary.
 *
 * @param <T> the type of task result
 */
public abstract class GeneralBenchSummary<T extends TaskResult> implements Summary {
  protected float mThroughput;
  protected Map<String, T> mNodeResults  = new HashMap<>();

  /**
   * @return the throughput
   */
  public float getThroughput() {
    return mThroughput;
  }

  /**
   * @param throughput the throughput
   */
  public void setThroughput(float throughput) {
    mThroughput = throughput;
  }

  /**
   * @return the list of nodes
   */
  public Map<String, T> getNodeResults() {
    return mNodeResults;
  }

  /**
   * @param nodes the list of nodes
   */
  public void setNodeResults(Map<String, T> nodes) {
    mNodeResults = nodes;
  }

  /**
   * @return the error information
   */
  public List<String> collectErrorsFromAllNodes() {
    List<String> errors = new ArrayList<>();
    for (T node : mNodeResults.values()) {
      // add all the errors for this node, with the node appended to prefix
      for (String err : node.getErrors()) {
        errors.add(String.format("%s :%s", node.getBaseParameters().mId, err));
      }
    }
    return errors;
  }
}
