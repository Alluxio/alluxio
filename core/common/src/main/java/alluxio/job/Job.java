/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job;

import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A Job that can run at Alluxio.
 */
@SuppressWarnings("serial")
@ThreadSafe
public abstract class Job implements Serializable {
  private final JobConf mJobConf;

  /**
   * Constructs with job configuration.
   *
   * @param jobConf the job configuration
   */
  public Job(JobConf jobConf) {
    mJobConf = Preconditions.checkNotNull(jobConf);
  }

  /**
   * @return the job configuration
   */
  public JobConf getJobConf() {
    return mJobConf;
  }

  /**
   * Runs the job.
   *
   * @return true if the run succeeds, false otherwise
   */
  public abstract boolean run();
}
