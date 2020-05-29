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

package alluxio.job.plan.io;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.util.BenchmarkJobUtils;
import alluxio.stress.BaseParameters;
import alluxio.stress.job.IOConfig;
import alluxio.util.ShellUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The definition for the UFS I/O throughput job, which generates concurrent streams to the UFS.
 *
 * {@link IOConfig} is the configuration class, each task takes a List<String> as a list of
 * command-line arguments to the benchmark command, and each task returns the string output.
 * The output will be serialized and deserialized using JSON, collected and merged.
 */
public class IODefinition implements PlanDefinition<IOConfig, ArrayList<String>, String> {
  @Override
  public Class<IOConfig> getJobConfigClass() {
    return IOConfig.class;
  }

  @Override
  public String join(IOConfig config, Map<WorkerInfo, String> taskResults) throws Exception {
    return BenchmarkJobUtils.join(taskResults);
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<String>>> selectExecutors(
          IOConfig config, List<WorkerInfo> jobWorkerInfoList,
          SelectExecutorsContext selectExecutorsContext) throws Exception {
    Set<Pair<WorkerInfo, ArrayList<String>>> result = Sets.newHashSet();

    // Randomly select N workers
    Collections.shuffle(jobWorkerInfoList);
    int cnt = config.getWorkerNum();
    for (WorkerInfo worker : jobWorkerInfoList) {
      if (cnt-- <= 0) {
        break;
      }
      result.add(new Pair<>(worker, BenchmarkJobUtils.generateIdArgs(worker)));
    }
    return result;
  }

  @Override
  public String runTask(IOConfig config, ArrayList<String> args, RunTaskContext runTaskContext)
          throws Exception {
    List<String> command = new ArrayList<>();
    command.add(ServerConfiguration.get(PropertyKey.HOME) + "/bin/alluxio");
    command.add("runUfsIOTest");
    command.add(config.getClassName());
    command.addAll(config.getArgs());
    // Run in distributed mode with job service
    command.add(BaseParameters.DISTRIBUTED_FLAG);
    command.add(BaseParameters.IN_PROCESS_FLAG);
    command.addAll(args);
    String output = ShellUtils.execCommand(command.toArray(new String[0]));
    return output;
  }
}
