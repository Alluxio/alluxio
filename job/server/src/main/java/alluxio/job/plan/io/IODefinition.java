package alluxio.job.plan.io;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.stress.BaseParameters;
import alluxio.stress.JsonSerializable;
import alluxio.stress.job.IOConfig;
import alluxio.stress.worker.IOTaskResult;
import alluxio.util.ShellUtils;
import alluxio.wire.WorkerInfo;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    if (taskResults.isEmpty()) {
      throw new IOException("No results from any workers.");
    }

    AtomicReference<IOException> error = new AtomicReference<>(null);
    List<IOTaskResult> results = taskResults.entrySet().stream().map(
            entry -> {
              try {
                return JsonSerializable.fromJson(entry.getValue().trim(), new IOTaskResult[0]);
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
      ArrayList<String> args = new ArrayList<>(2);
      // Add the worker hostname + worker id as the unique task id for each distributed task.
      // The worker id is used since there may be multiple workers on a single host.
      args.add(BaseParameters.ID_FLAG);
      args.add(worker.getAddress().getHost() + "-" + worker.getId());
      result.add(new Pair<>(worker, args));
    }
    return result;
  }

  @Override
  public String runTask(IOConfig config, ArrayList<String> args, RunTaskContext runTaskContext)
          throws Exception {
    List<String> command = new ArrayList<>(3 + config.getArgs().size());
    command.add(ServerConfiguration.get(PropertyKey.HOME) + "/bin/alluxio");
    command.add("runUfsIOTest");
    command.add(config.getClassName());
    command.addAll(config.getArgs());
    // the cluster will run distributed tasks
    command.add(BaseParameters.DISTRIBUTED_FLAG);
    command.add(BaseParameters.IN_PROCESS_FLAG);

    // TODO(jiacheng): do i need this?
    if (config.getArgs().stream().noneMatch((s) -> s.equals(BaseParameters.START_MS_FLAG))) {
      command.add(BaseParameters.START_MS_FLAG);
      command.add(Long.toString((System.currentTimeMillis() + 5000)));
    }

    command.addAll(args);
    String output = ShellUtils.execCommand(command.toArray(new String[0]));
    return output;
  }
}
