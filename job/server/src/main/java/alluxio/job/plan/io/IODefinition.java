package alluxio.job.plan.io;

import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.PlanDefinition;
import alluxio.stress.BaseParameters;
import alluxio.stress.job.IOConfig;
import alluxio.util.ShellUtils;
import alluxio.wire.WorkerInfo;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

// TODO(jiacheng): return benchmark result?
public class IODefinition implements PlanDefinition<IOConfig, ArrayList<String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(IODefinition.class);

    @Override
    public Class<IOConfig> getJobConfigClass() {
        return null;
    }

    @Override
    public String join(IOConfig config, Map taskResults) throws Exception {

        // TODO(jiacheng): what should this be?
        return null;
    }

    @Override
    public Set<Pair<WorkerInfo, ArrayList<String>>> selectExecutors(IOConfig config, List<WorkerInfo> jobWorkerInfoList, SelectExecutorsContext selectExecutorsContext) throws Exception {
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
            result.add(new Pair<>(worker, args));
        }
        return result;
    }

    @Override
    public String runTask(IOConfig config, ArrayList<String> args, RunTaskContext runTaskContext)
            throws Exception {

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
        String output = ShellUtils.execCommand(command.toArray(new String[0]));
        return output;
    }
}
