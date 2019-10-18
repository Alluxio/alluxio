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

import alluxio.client.job.JobGrpcClientUtils;
import alluxio.conf.ServerConfiguration;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.JobConfig;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The job definition for {@link CompositeConfig}.
 */
public final class CompositeDefinition
    extends AbstractVoidJobDefinition<CompositeConfig, ArrayList<CompositeTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(CompositeDefinition.class);

  /**
   * Constructs a new {@link CompositeDefinition}.
   */
  public CompositeDefinition() {
  }

  @Override
  public Class<CompositeConfig> getJobConfigClass() {
    return CompositeConfig.class;
  }

  @Override
  public Map<WorkerInfo, ArrayList<CompositeTask>> selectExecutors(CompositeConfig config,
      List<WorkerInfo> jobWorkers, SelectExecutorsContext context) throws Exception {
    Preconditions.checkState(!jobWorkers.isEmpty(), "No job worker");
    Map<WorkerInfo, ArrayList<CompositeTask>> assignments = Maps.newHashMap();
    if (config.isSequential()) {
      WorkerInfo worker = jobWorkers.get(new Random().nextInt(jobWorkers.size()));
      assignments.put(worker, new ArrayList<>());
      assignments.get(worker).add(new CompositeTask(config.getJobs()));
    } else {
      int jobWorkerIndex = 0;
      for (JobConfig job : config.getJobs()) {
        WorkerInfo worker = jobWorkers.get(jobWorkerIndex++);
        if (jobWorkerIndex == jobWorkers.size()) {
          jobWorkerIndex = 0;
        }
        assignments.putIfAbsent(worker, new ArrayList<>());
        assignments.get(worker).add(new CompositeTask(Lists.newArrayList(job)));
      }
    }
    return assignments;
  }

  @Override
  public SerializableVoid runTask(CompositeConfig config, ArrayList<CompositeTask> tasks,
      RunTaskContext context) throws Exception {
    for (CompositeTask task : tasks) {
      for (JobConfig job : task.getJobs()) {
        JobGrpcClientUtils.run(job, 1, ServerConfiguration.global());
      }
    }
    return null;
  }
}
