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

package alluxio.job.plan.batch;

import alluxio.collections.Pair;
import alluxio.job.JobConfig;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.plan.BatchedJobConfig;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.plan.load.LoadDefinition;
import alluxio.job.plan.migrate.MigrateDefinition;
import alluxio.job.plan.persist.PersistDefinition;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.beust.jcommander.internal.Sets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class BatchedJobDefinition
    extends AbstractVoidPlanDefinition<BatchedJobConfig, BatchedJobDefinition.BatchedJobTask> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchedJobDefinition.class);

  /**
   * Constructs a new {@link BatchedJobDefinition}.
   */
  public BatchedJobDefinition() {}

  @Override
  public Set<Pair<WorkerInfo, BatchedJobTask>> selectExecutors(BatchedJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) throws Exception {
    // get job type and config
    String jobType = config.getJobType();

    PlanDefinition plan = JobDefinitionFactory.create(jobType);
    // convert map to config
    final ObjectMapper mapper = new ObjectMapper();
    Class<?> jobConfigClass = plan.getJobConfigClass();
    Set<Pair<WorkerInfo, BatchedJobTask>> allTasks = Sets.newHashSet();
    for (Map<String, String> configMap : config.getJobConfigs()) {
      JobConfig jobConfig = (JobConfig) mapper.convertValue(configMap, jobConfigClass);
      Set<Pair<WorkerInfo, Serializable>> tasks =
          plan.selectExecutors(jobConfig, jobWorkerInfoList, context);
      for (Pair<WorkerInfo, Serializable> task : tasks) {
        BatchedJobTask batchedTask = new BatchedJobTask(jobConfig, task.getSecond());
        allTasks.add(new Pair<>(task.getFirst(), batchedTask));
      }
    }
    return allTasks;
  }

  @Override
  public SerializableVoid runTask(BatchedJobConfig config, BatchedJobTask task,
      RunTaskContext context) throws Exception {
    String jobType = config.getJobType();
    @SuppressWarnings("rawtypes")
    PlanDefinition plan = JobDefinitionFactory.create(jobType);
    plan.runTask(task.getJobConfig(), task.getJobTaskArgs(), context);
    return null;
  }

  /**
   * A task representing loading a block into the memory of a worker.
   */
  public static class BatchedJobTask implements Serializable {
    private static final long serialVersionUID = -3643377264144315329L;
    final Serializable mJobTasks;
    final JobConfig mJobConfig;

    /**
     * @param config job config for specific job type
     * @param jobTasks job tasks
     */
    public BatchedJobTask(JobConfig config, Serializable jobTasks) {
      mJobConfig = config;
      mJobTasks = jobTasks;
    }

    /**
     * @return job tasks
     */
    public Serializable getJobTaskArgs() {
      return mJobTasks;
    }

    /**
     * @return job config
     */
    public JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("jobConfig", mJobConfig)
          .add("jobTaskArgs", mJobTasks).toString();
    }
  }

  @Override
  public Class<BatchedJobConfig> getJobConfigClass() {
    return BatchedJobConfig.class;
  }

  /**
   * Factory class for get the specific job definition.
   */
  public static class JobDefinitionFactory {
    /**
     * @param jobName name of job
     * @return job definition
     */
    public static PlanDefinition create(String jobName) {
      switch (jobName) {
        case "Load":
          return new LoadDefinition();
        case "Migrate":
          return new MigrateDefinition();
        case "Persist":
          return new PersistDefinition();
        default:
          throw new IllegalStateException(
              "Batched Job currently doesn't support this jobType: " + jobName);
      }
    }
  }
}
