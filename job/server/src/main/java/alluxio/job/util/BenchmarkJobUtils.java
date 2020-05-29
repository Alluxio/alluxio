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

/**
 * Utilities used by benchmarking jobs.
 * */
public class BenchmarkJobUtils {
  /**
   * Generate the ID argument from {@link WorkerInfo}.
   *
   * @param worker worker info
   * @return args
   * */
  public static ArrayList<String> generateIdArgs(WorkerInfo worker) {
    ArrayList<String> args = new ArrayList<>(2);
    // Add the worker hostname + worker id as the unique task id for each distributed task.
    // The worker id is used since there may be multiple workers on a single host.
    args.add(BaseParameters.ID_FLAG);
    args.add(worker.getAddress().getHost() + "-" + worker.getId());
    return args;
  }

  /**
   * Deserialize and merge the task results from workers.
   *
   * @param taskResults task results from workers
   * @return serialized merged results
   * */
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
