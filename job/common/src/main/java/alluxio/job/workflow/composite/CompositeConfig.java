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
import alluxio.job.workflow.WorkflowConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A composite job is a list of jobs to be executed either sequentially or concurrently.
 *
 * A composite job can contain other composite jobs, this can be used to implement patterns like:
 * execute a set of jobs concurrently, each job can run run another set of sub jobs sequentially.
 */
@ThreadSafe
public final class CompositeConfig implements WorkflowConfig {
  private static final long serialVersionUID = 959533283484491854L;

  private static final String NAME = "Composite";

  private final ArrayList<JobConfig> mJobs;
  /**
   * If true, the jobs should be executed sequentially;
   * otherwise, they should be executed concurrently.
   */
  private final boolean mSequential;

  /**
   * @param jobs the list of jobs
   * @param sequential whether to execute jobs sequentially
   */
  public CompositeConfig(@JsonProperty("jobs") ArrayList<JobConfig> jobs,
      @JsonProperty("sequential") Boolean sequential) {
    mJobs = Preconditions.checkNotNull(jobs, "jobs");
    mSequential = sequential;
  }

  /**
   * @return the list of jobs
   */
  public ArrayList<JobConfig> getJobs() {
    return mJobs;
  }

  /**
   * @return whether to execute the jobs sequentially
   */
  public boolean isSequential() {
    return mSequential;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof CompositeConfig)) {
      return false;
    }
    CompositeConfig that = (CompositeConfig) obj;
    return mJobs.equals(that.mJobs)
        && mSequential == that.mSequential;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobs, mSequential);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobs", mJobs)
        .add("sequential", mSequential)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    final ArrayList<String> paths = new ArrayList<>();
    for (JobConfig job : mJobs) {
      paths.addAll(job.affectedPaths());
    }
    return Collections.unmodifiableList(paths);
  }
}
