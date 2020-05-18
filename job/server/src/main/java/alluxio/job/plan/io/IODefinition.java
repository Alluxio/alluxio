package alluxio.job.plan.io;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.stress.BaseParameters;
import alluxio.stress.JsonSerializable;
import alluxio.stress.TaskResult;
import alluxio.stress.job.IOConfig;
import alluxio.stress.worker.IOTaskResult;
import alluxio.util.ShellUtils;
import alluxio.wire.WorkerInfo;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

// TODO(jiacheng): return benchmark result?
public class IODefinition implements PlanDefinition<IOConfig, ArrayList<String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(IODefinition.class);

    @Override
    public Class<IOConfig> getJobConfigClass() {
        return IOConfig.class;
    }

    @Override
    public String join(IOConfig config, Map<WorkerInfo, String> taskResults) throws Exception {
        if (taskResults.isEmpty()) {
            throw new IOException("No results from any workers.");
        }
        LOG.info("join()");
        for(Map.Entry<WorkerInfo, String> entry : taskResults.entrySet()) {
            LOG.info("worker={}", entry.getKey());
            LOG.info("{}", entry.getValue());
        }

        AtomicReference<IOException> error = new AtomicReference<>(null);

        // TODO(jiacheng): check this join operation, use IOTaskResult instead of String?
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
            if (cnt <= 0) {
                break;
            }
            ArrayList<String> args = new ArrayList<>(2);
            // Add the worker hostname + worker id as the unique task id for each distributed task.
            // The worker id is used since there may be multiple workers on a single host.
            args.add(BaseParameters.ID_FLAG);
            args.add(worker.getAddress().getHost() + "-" + worker.getId());
            // TODO(jiacheng): args here?
            result.add(new Pair<>(worker, args));
            cnt--;
        }
        return result;
    }

    @Override
    public String runTask(IOConfig config, ArrayList<String> args, RunTaskContext runTaskContext)
            throws Exception {
        LOG.info("args={}", args);

        // TODO(jiacheng): how are the command args generated??
        List<String> command = new ArrayList<>(3 + config.getArgs().size());
        command.add(ServerConfiguration.get(PropertyKey.HOME) + "/bin/alluxio");
        command.add("runClass");
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

        LOG.info("running command: " + String.join(" ", command));
        // TODO(jiacheng): execWithOutput?
        String output = ShellUtils.execCommand(command.toArray(new String[0]));
        LOG.info("Command output: {}", output);
        return output;
    }
}
