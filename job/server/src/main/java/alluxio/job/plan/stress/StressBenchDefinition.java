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

package alluxio.job.plan.stress;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.job.util.BenchmarkJobUtils;
import alluxio.stress.BaseParameters;
import alluxio.stress.JsonSerializable;
import alluxio.stress.TaskResult;
import alluxio.stress.job.StressBenchConfig;
import alluxio.util.ShellUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * The definition for the stress bench job, which runs distributed benchmarks.
 *
 * {@link StressBenchConfig} is the configuration class, each task takes a List<String> as a list of
 * command-line arguments to the benchmark command, and each task returns the string output.
 */
public final class StressBenchDefinition
    implements PlanDefinition<StressBenchConfig, ArrayList<String>, String> {
  private static final Logger LOG = LoggerFactory.getLogger(StressBenchDefinition.class);

  /**
   * Constructs a new instance.
   */
  public StressBenchDefinition() {}

  @Override
  public Class<StressBenchConfig> getJobConfigClass() {
    return StressBenchConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<String>>> selectExecutors(StressBenchConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) throws Exception {
    Set<Pair<WorkerInfo, ArrayList<String>>> result = Sets.newHashSet();

    // sort copy of workers by hashcode
    List<WorkerInfo> workerList = Lists.newArrayList(jobWorkerInfoList);
    workerList.sort(Comparator.comparingInt(w -> w.getAddress().hashCode()));
    // take the first subset, according to cluster limit
    int clusterLimit = config.getClusterLimit();
    if (clusterLimit <= 0) {
      clusterLimit = workerList.size();
    }
    workerList = workerList.subList(0, clusterLimit);

    for (WorkerInfo worker : workerList) {
      result.add(new Pair<>(worker, BenchmarkJobUtils.generateIdArgs(worker)));
    }
    return result;
  }

  @Override
  public String runTask(StressBenchConfig config, ArrayList<String> args,
      RunTaskContext runTaskContext) throws Exception {
    List<String> command = new ArrayList<>(3 + config.getArgs().size());
    command.add(ServerConfiguration.get(PropertyKey.HOME) + "/bin/alluxio");
    command.add("runClass");
    command.add(config.getClassName());
    command.addAll(config.getArgs());
    // the cluster will run distributed tasks
    command.add(BaseParameters.DISTRIBUTED_FLAG);
    command.add(BaseParameters.IN_PROCESS_FLAG);

    if (config.getArgs().stream().noneMatch((s) -> s.equals(BaseParameters.START_MS_FLAG))) {
      command.add(BaseParameters.START_MS_FLAG);
      command.add(Long.toString((System.currentTimeMillis() + config.getStartDelayMs())));
    }

    command.addAll(args);
    String output = ShellUtils.execCommand(command.toArray(new String[0]));
    return output;
  }

  @Override
  public String join(StressBenchConfig config, Map<WorkerInfo, String> taskResults)
      throws Exception {
    return BenchmarkJobUtils.join(taskResults);
  }
}
