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

package alluxio.job.composite;

import alluxio.job.JobConfig;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Jobs in this task are executed sequentially.
 */
public final class CompositeTask implements Serializable {
  private static final long serialVersionUID = -2840432899424126340L;

  private final ArrayList<JobConfig> mJobs;

  /**
   * @param jobs a list of jobs
   */
  public CompositeTask(ArrayList<JobConfig> jobs) {
    mJobs = jobs;
  }

  /**
   * @return the jobs
   */
  public ArrayList<JobConfig> getJobs() {
    return mJobs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompositeTask)) {
      return false;
    }
    CompositeTask that = (CompositeTask) o;
    return mJobs.equals(that.mJobs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobs);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobs", mJobs)
        .toString();
  }
}
