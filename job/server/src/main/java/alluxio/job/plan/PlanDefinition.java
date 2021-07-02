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

import alluxio.collections.Pair;
import alluxio.job.JobConfig;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.wire.WorkerInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A job definition. A definition has two important parts: (1) a
 * {@link PlanDefinition#selectExecutors(JobConfig, List, SelectExecutorsContext)} method runs at
 * the master node and selects the workers to run the executors.
 * (2) a {@link #runTask(JobConfig, Serializable, RunTaskContext)} method runs at each selected
 * executor on the worker node.
 *
 * @param <T> the job configuration
 * @param <P> the parameters to pass to each task
 * @param <R> the return type from the task
 */
public interface PlanDefinition<T extends JobConfig, P extends Serializable,
    R extends Serializable> {
  /**
   * @return the class of the associated {@link JobConfig}
   */
  Class<T> getJobConfigClass();

  /**
   * Selects the workers to run the task.
   *
   * @param config the job configuration
   * @param jobWorkerInfoList the list of available workers' information
   * @param selectExecutorsContext the context containing information used to select executors
   * @return a set of pairs of selected workers to the parameters to pass along
   * @throws Exception if any error occurs
   */
  Set<Pair<WorkerInfo, P>> selectExecutors(T config, List<WorkerInfo> jobWorkerInfoList,
      SelectExecutorsContext selectExecutorsContext) throws Exception;

  /**
   * Runs the task in the executor.
   *
   * @param config the job configuration
   * @param args the arguments passed in
   * @param runTaskContext the context containing information used to execute a task
   * @return the task result
   * @throws Exception if any error occurs
   */
  R runTask(T config, P args, RunTaskContext runTaskContext) throws Exception;

  /**
   * Joins the task results on the master.
   *
   * @param config the job configuration
   * @param taskResults the task results
   * @return the joined results
   * @throws Exception if any error occurs
   */
  String join(T config, Map<WorkerInfo, R> taskResults) throws Exception;
}
