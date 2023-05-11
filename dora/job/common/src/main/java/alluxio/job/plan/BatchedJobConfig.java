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

package alluxio.job.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The configuration of loading a file.
 */
@ThreadSafe
public class BatchedJobConfig implements PlanConfig {
  public static final String NAME = "BatchedJob";
  private static final long serialVersionUID = -5482449086646391059L;
  private final String mJobType;
  private final Set<Map<String, String>> mJobConfigs;

  /**
   * @param jobType the job type for batched job
   * @param jobConfigs the configs for each job in the batched job
   */
  public BatchedJobConfig(@JsonProperty("jobType") String jobType,
      @JsonProperty("jobConfig") Set<Map<String, String>> jobConfigs) {
    mJobType = Preconditions.checkNotNull(jobType, "The file path cannot be null");
    mJobConfigs = Preconditions.checkNotNull(jobConfigs, "The job config cannot be null");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BatchedJobConfig)) {
      return false;
    }
    BatchedJobConfig that = (BatchedJobConfig) obj;
    return mJobType.equals(that.mJobType) && mJobConfigs == that.mJobConfigs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobType, mJobConfigs);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("JobType", mJobType)
        .add("JobConfigs", mJobConfigs)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return Collections.EMPTY_LIST;
  }

  /**
   * @return job type
   */
  public String getJobType() {
    return mJobType;
  }

  /**
   * @return batch of job configs
   */
  public Set<Map<String, String>> getJobConfigs() {
    return mJobConfigs;
  }
}
