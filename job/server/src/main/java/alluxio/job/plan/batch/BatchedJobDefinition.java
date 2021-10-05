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
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple loading job that loads the blocks of a file in a distributed and round-robin fashion.
 */
@NotThreadSafe
public final class BatchedJobDefinition
    extends AbstractVoidPlanDefinition<BatchedJobConfig, ArrayList<BatchedJobDefinition.BatchedJobTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchedJobDefinition.class);

  /**
   * Constructs a new {@link BatchedJobDefinition}.
   */
  public BatchedJobDefinition() {
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<BatchedJobTask>>> selectExecutors(BatchedJobConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context)
      throws Exception {
    //get job type and config
    String jobType = config.getJobType();
    Class jobClass = Class.forName(jobType);
    PlanDefinition plan = (PlanDefinition) jobClass.newInstance();
    // convert map to config
    final ObjectMapper mapper = new ObjectMapper();
    Class jobConfigClass = plan.getJobConfigClass();

    JobConfig jobConfig = (JobConfig) mapper.convertValue(config.getJobConfigs(), jobConfigClass);


    // call the specific PlanDefinition.selectExecutors
    Set<Pair<WorkerInfo,?>> jobs = plan.selectExecutors(jobConfig, jobWorkerInfoList, context);
    Map<WorkerInfo, List<?>> map = Collections.emptyMap();
    for(Pair<WorkerInfo,?> job:jobs){
      //put workerInfo as key and job as list of value

    }
    Set<Pair<WorkerInfo, ArrayList<BatchedJobTask>>> batchedJobs = new HashSet(map.values());
    return batchedJobs;
  }


  @Override
  public SerializableVoid runTask(BatchedJobConfig config, ArrayList<BatchedJobTask> tasks,
      RunTaskContext context) throws Exception {
    for(BatchedJobTask task:tasks) {
      // do similar thing as selectExecutors but call the specific PlanDefinition.runTask
    }
    return null;
  }


  /**
   * A task representing loading a block into the memory of a worker.
   */
  public static class BatchedJobTask implements Serializable {
    private static final long serialVersionUID = -3643377264144315329L;
    final String mJobType;
    final Map<String,String> mJobTaskArgs;

    /**
     * @param jobType
     * @param jobTaskArgs
     */
    public BatchedJobTask(String jobType, Map<String,String> jobTaskArgs) {
      this.mJobType = jobType;
      this.mJobTaskArgs = jobTaskArgs;
    }


    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("jobType", mJobType)
          .add("jobTaskArgs", mJobTaskArgs)
          .toString();
    }
  }

  @Override
  public Class<BatchedJobConfig> getJobConfigClass() {
    return BatchedJobConfig.class;
  }
}
