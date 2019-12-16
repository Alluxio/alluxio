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

package alluxio.job.workflow.composite;

import alluxio.job.JobConfig;
import alluxio.job.workflow.WorkflowExecution;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Set;

/**
 * Job execution for {@link CompositeConfig}.
 */
public class CompositeExecution extends WorkflowExecution {

  private final ArrayList<JobConfig> mJobs;
  private final boolean mSequential;

  private int mPosition;

  /**
   * Default constructor.
   * @param compositeConfig the {@link CompositeConfig}
   */
  public CompositeExecution(CompositeConfig compositeConfig) {
    mJobs = compositeConfig.getJobs();
    mSequential = compositeConfig.isSequential();

    mPosition = 0;
  }

  @Override
  public String getName() {
    return "Composite";
  }

  @Override
  protected Set<JobConfig> nextJobs() {
    if (mPosition >= mJobs.size()) {
      return Sets.newHashSet();
    }

    if (mSequential) {
      return Sets.newHashSet(mJobs.get(mPosition++));
    }

    mPosition = mJobs.size();
    return Sets.newHashSet(mJobs);
  }
}
