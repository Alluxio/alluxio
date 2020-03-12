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

import alluxio.job.JobConfig;
import alluxio.table.common.Layout;
import alluxio.table.common.transform.action.TransformAction;

import java.util.ArrayList;

/**
 * The plan for a transformation.
 */
public class TransformPlan {
  /**
   * The base layout to transform from.
   */
  private final Layout mBaseLayout;
  /**
   * The layout to transform to.
   */
  private final Layout mTransformedLayout;
  /**
   * The list of jobs to execute the plan.
   */
  private final ArrayList<JobConfig> mJobConfigs;

  /**
   * A list of jobs will be computed based on the provided transform definition.
   *
   * @param baseLayout the layout to transform from
   * @param transformedLayout the layout to transform to
   * @param definition the transformation definition
   */
  public TransformPlan(Layout baseLayout, Layout transformedLayout,
                       TransformDefinition definition) {
    mBaseLayout = baseLayout;
    mTransformedLayout = transformedLayout;
    mJobConfigs = computeJobConfigs(definition);
  }

  private ArrayList<JobConfig> computeJobConfigs(TransformDefinition definition) {
    ArrayList<JobConfig> actions = new ArrayList<>();
    Layout baseLayout = mBaseLayout;
    boolean deleteSrc = false;

    for (TransformAction action : definition.getActions()) {
      actions.add(action.generateJobConfig(baseLayout, mTransformedLayout, deleteSrc));
      baseLayout = mTransformedLayout;
      deleteSrc = true;
    }

    if (actions.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one action should be defined for the transformation");
    }

    return actions;
  }

  /**
   * @return the base layout
   */
  public Layout getBaseLayout() {
    return mBaseLayout;
  }

  /**
   * @return the transformed layout
   */
  public Layout getTransformedLayout() {
    return mTransformedLayout;
  }

  /**
   * @return the list of job configurations to be executed sequentially
   */
  public ArrayList<JobConfig> getJobConfigs() {
    return mJobConfigs;
  }
}
