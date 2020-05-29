package alluxio.job.util;

import alluxio.stress.BaseParameters;
import alluxio.stress.JsonSerializable;
import alluxio.stress.TaskResult;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class BenchmarkJobUtils {
  public static ArrayList<String> generateIdArgs(WorkerInfo worker) {
    ArrayList<String> args = new ArrayList<>(2);
    // Add the worker hostname + worker id as the unique task id for each distributed task.
    // The worker id is used since there may be multiple workers on a single host.
    args.add(BaseParameters.ID_FLAG);
    args.add(worker.getAddress().getHost() + "-" + worker.getId());
    return args;
  }

  public static String join(Map<WorkerInfo, String> taskResults) throws Exception {
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
