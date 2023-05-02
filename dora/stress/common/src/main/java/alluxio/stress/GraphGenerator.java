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

package alluxio.stress;

import alluxio.stress.graph.Graph;

import java.util.List;

/**
 * This generates graphs from a list of summaries.
 */
public abstract class GraphGenerator {
  /**
   * @param results the list of summaries
   * @return a list of graphs generated from the input list of summaries
   */
  public abstract List<Graph> generate(List<? extends Summary> results);
}
