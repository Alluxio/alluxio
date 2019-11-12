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

import alluxio.job.JobConfig;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.io.Serializable;
import java.util.Map;

/**
 * An abstract job definition where the run task method does not return a value.
 *
 * @param <T> the job configuration type
 * @param <P> the argument type
 */
public abstract class AbstractVoidPlanDefinition<T extends JobConfig, P extends Serializable>
    implements PlanDefinition<T, P, SerializableVoid> {

  @Override
  public String join(T config, Map<WorkerInfo, SerializableVoid> taskResults) throws Exception {
    return "";
  }
}
