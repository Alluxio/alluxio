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

package alluxio.table.common.transform.action;

import alluxio.job.JobConfig;
import alluxio.table.common.Layout;

/**
 * The definition of an individual transformation action.
 */
public interface TransformAction {

  /**
   * @param base the layout to transform from
   * @param transformed the layout to transform to
   * @param deleteSrc whether the src file should be deleted
   * @return the job configuration for this action
   */
  JobConfig generateJobConfig(Layout base, Layout transformed, boolean deleteSrc);
}
