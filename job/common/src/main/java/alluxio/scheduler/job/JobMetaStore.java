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

package alluxio.scheduler.job;

import java.util.Set;

/**
 *  Job meta store that store job information.
 */
public interface JobMetaStore {

  /**
   * Update existing job in the meta store with the new job.
   * @param job the job used to update the existing job in the meta store
   */
  void updateJob(Job<?> job);

  /**
   * @return all the jobs in the meta store
   */
  Set<Job<?>> getJobs();
}
