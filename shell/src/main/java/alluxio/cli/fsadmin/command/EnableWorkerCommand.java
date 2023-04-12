package alluxio.cli.fsadmin.command;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.grpc.RemoveDecommissionedWorkerPOptions;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class EnableWorkerCommand extends AbstractFsAdminCommand {
  private static final Option ADDRESSES_OPTION =
          Option.builder("h")
              .longOpt("addresses")
              .required(true)  // Host option is mandatory.
              .hasArg(true)
              .numberOfArgs(1)
              .argName("addresses")
              .desc("One or more worker addresses separated by comma. If port is not specified, "
                  + PropertyKey.WORKER_WEB_PORT.getName() + " will be used. "
                  + "Note the addresses specify the WEB port instead of RPC port!")
              .build();

  /**
   * Constructs a new instance to decommission the given worker from Alluxio.
   * @param context the context containing all operator handles
   */
  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public EnableWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  private List<WorkerNetAddress> getWorkerAddresses(CommandLine cl) {
    String workerAddressesStr = cl.getOptionValue(ADDRESSES_OPTION.getLongOpt());
    if (workerAddressesStr.isEmpty()) {
      throw new IllegalArgumentException("Worker addresses must be specified");
    }

    List<WorkerNetAddress> result = new ArrayList<>();
    for (String part : workerAddressesStr.split(",")) {
      if (part.contains(":")) {
        String[] p = part.split(":");
        Preconditions.checkState(p.length == 2, "worker address %s cannot be recognized", part);
        // TODO(jiacheng): handle hostnames like localhost
        String port = p[1];
        WorkerNetAddress addr = new WorkerNetAddress().setHost(p[0]).setWebPort(Integer.parseInt(port));
        result.add(addr);
      } else {
        // Assume the whole string is hostname
        int port = Configuration.getInt(PropertyKey.WORKER_WEB_PORT);
        WorkerNetAddress addr = new WorkerNetAddress().setHost(part).setWebPort(port);
        result.add(addr);
      }
    }
    return result;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    List<WorkerNetAddress> addresses = getWorkerAddresses(cl);

    Set<WorkerNetAddress> failedWorkers = new HashSet<>();
    for (WorkerNetAddress workerAddress : addresses) {
      System.out.format("Re-enabling worker %s:%s%n", workerAddress.getHost(), workerAddress.getWebPort());
      try {
        mBlockClient.removeDecommissionedWorker(workerAddress.getHost());
        System.out.format("Reenabled worker %s:%s on master%n", workerAddress.getHost(), workerAddress.getWebPort());
      } catch (IOException ie) {
        System.err.format("Failed to reenable worker %s:%s%n", workerAddress.getHost(), workerAddress.getWebPort());
        ie.printStackTrace();
        failedWorkers.add(workerAddress);
      }
    }

    if (failedWorkers.size() == 0) {
      System.out.println("Successfully re-enabled all workers on the master. The workers should be able to register to the master and then serve normally." +
              "Note there is a short gap defined by " + PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL.getName() + " before the clients become aware of this worker and start to use it.");
      return 0;
    } else {
      System.out.format("%s failed to be re-enabled on the master: %s%n", failedWorkers.size(),
          failedWorkers.stream().map(w -> w.getHost() + ":" + w.getWebPort()).collect(Collectors.toList()));
      System.out.println("The admin needs to manually check and fix the problem. "
          + "Those workers are not able to register to the master and serve requests.");
      return 1;
    }
  }

  @Override
  public String getCommandName() {
    return "enableWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESSES_OPTION);
  }

  @Override
  public String getUsage() {
    return "enableWorker -h <worker host>";
  }

  @Override
  public String getDescription() {
    return "Re-enables workers to register and join the cluster. This is used in pair with the decommissionWorker command.";
  }
}
