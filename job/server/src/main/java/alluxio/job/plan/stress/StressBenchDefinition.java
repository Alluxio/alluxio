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
import alluxio.resource.CloseableResource;
import alluxio.stress.BaseParameters;
import alluxio.stress.worker.UfsIOParameters;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.JsonSerializable;
import alluxio.stress.TaskResult;
import alluxio.stress.job.StressBenchConfig;
import alluxio.util.ShellUtils;
import alluxio.wire.MountPointInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  public StressBenchDefinition() {
  }

  @Override
  public Class<StressBenchConfig> getJobConfigClass() {
    return StressBenchConfig.class;
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<String>>> selectExecutors(StressBenchConfig config,
      List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext context) {
    Set<Pair<WorkerInfo, ArrayList<String>>> result = Sets.newHashSet();

    // sort copy of workers by hashcode
    List<WorkerInfo> workerList = Lists.newArrayList(jobWorkerInfoList);
    workerList.sort(Comparator.comparing(w -> w.getAddress().getHost()));
    // take the first subset, according to cluster limit
    int clusterLimit = config.getClusterLimit();
    if (clusterLimit == 0) {
      clusterLimit = workerList.size();
    }
    if (clusterLimit < 0) {
      // if negative, reverse the list
      clusterLimit = -clusterLimit;
      Collections.reverse(workerList);
    }
    workerList = workerList.subList(0, clusterLimit);

    for (WorkerInfo worker : workerList) {
      ArrayList<String> args = new ArrayList<>(2);
      // Add the worker hostname + worker id as the unique task id for each distributed task.
      // The worker id is used since there may be multiple workers on a single host.
      args.add(BaseParameters.ID_FLAG);
      args.add(worker.getAddress().getHost() + "-" + worker.getId());
      result.add(new Pair<>(worker, args));
    }
    return result;
  }

  private Map<String, String> getUfsConf(String ufsUri, RunTaskContext runTaskContext)
      throws Exception {
    Map<String, MountPointInfo> mountTable = runTaskContext.getFileSystem().getMountTable();
    for (Map.Entry<String, MountPointInfo> entry : mountTable.entrySet()) {
      if (ufsUri.startsWith(entry.getValue().getUfsUri())) {
        try (CloseableResource<UnderFileSystem> resource = runTaskContext.getUfsManager()
            .get(entry.getValue().getMountId()).acquireUfsResource()) {
          return resource.get().getConfiguration().toMap();
        }
      }
    }
    return ImmutableMap.of();
  }

  @Override
  public String runTask(StressBenchConfig config, ArrayList<String> args,
      RunTaskContext runTaskContext) throws Exception {
    List<String> command = new ArrayList<>(3 + config.getArgs().size());
    command.add(ServerConfiguration.get(PropertyKey.HOME) + "/bin/alluxio");
    command.add("runClass");
    command.add(config.getClassName());

    // the cluster will run distributed tasks
    command.add(BaseParameters.DISTRIBUTED_FLAG);
    command.add(BaseParameters.IN_PROCESS_FLAG);

    List<String> commandArgs = config.getArgs();
    List<String> newArgs = new ArrayList<>();
    if (commandArgs.stream().anyMatch(
        (s) -> s.equals(UfsIOParameters.USE_MOUNT_CONF))) {
      boolean nextElemIsPath = false;
      boolean removeNext = false;
      String ufsUri = "";
      for (String elem : commandArgs) {
        // Go through the parameters and remove any conf parameters and record the path parameter
        if (elem.equals(UfsIOParameters.CONF)) {
          removeNext = true;
        } else {
          if (!removeNext) {
            newArgs.add(elem);
          }
          removeNext = false;
        }
        if (elem.equals(UfsIOParameters.PATH)) {
          nextElemIsPath = true;
        } else {
          if (nextElemIsPath) {
            ufsUri = elem;
          }
          nextElemIsPath = false;
        }
      }
      commandArgs = newArgs;

      List<String> properties = getUfsConf(ufsUri, runTaskContext).entrySet().stream()
          .map(entry -> "--conf" + entry.getKey() + "=" + entry.getValue())
          .collect(Collectors.toList());
      commandArgs.addAll(properties);
    }

    if (config.getArgs().stream().noneMatch((s) -> s.equals(BaseParameters.START_MS_FLAG))) {
      command.add(BaseParameters.START_MS_FLAG);
      command.add(Long.toString((System.currentTimeMillis() + config.getStartDelayMs())));
    }

    command.addAll(commandArgs);
    command.addAll(args);
    String output = ShellUtils.execCommand(command.toArray(new String[0]));
    return output;
  }

  @Override
  public String join(StressBenchConfig config, Map<WorkerInfo, String> taskResults)
      throws Exception {
    if (taskResults.isEmpty()) {
      throw new IOException("No results from any workers.");
    }

    AtomicReference<IOException> error = new AtomicReference<>(null);

    List<TaskResult> results = taskResults.entrySet().stream().map(
        entry -> {
          try {
            return JsonSerializable.fromJson(entry.getValue().trim(), new TaskResult[0]);
          } catch (IOException | ClassNotFoundException e) {
            error.set(new IOException(String
                .format("Failed to parse task output from %s into result class",
                    entry.getKey().getAddress().getHost()), e));
          }
          return null;
        }).collect(Collectors.toList());

    if (error.get() != null) {
      throw error.get();
    }

    return results.get(0).aggregator().aggregate(results).toJson();
  }
}
