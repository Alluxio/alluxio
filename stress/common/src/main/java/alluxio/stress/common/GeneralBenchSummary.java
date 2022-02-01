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

import java.util.List;

/**
 * abstract class for BenchSummary.
 */
public abstract class GeneralBenchSummary implements Summary {
  protected float mThroughput;

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
   * @return the error information
   */
  public abstract List<String> collectErrorsFromAllNodes();
}
