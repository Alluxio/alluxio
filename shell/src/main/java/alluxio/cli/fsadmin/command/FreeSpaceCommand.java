package alluxio.cli.fsadmin.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.FreeSpaceRequest;
import alluxio.grpc.GrpcServerAddress;
import alluxio.security.user.UserState;

import com.google.common.net.HostAndPort;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.InetSocketAddress;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * free space command.
 */
@ThreadSafe
@PublicApi
public class FreeSpaceCommand extends AbstractFsAdminCommand {
  private static final Option PERCENT_OPTION =
      Option.builder()
          .longOpt("percent")
          .required(false)
          .hasArg(true)
          .desc("percent to free")
          .build();

  private static final Option TIER_OPTION =
      Option.builder()
          .longOpt("tier")
          .required(false)
          .hasArg(true)
          .desc("free space tier")
          .build();

  private AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public FreeSpaceCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(PERCENT_OPTION).addOption(TIER_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String workerAddress = args[0];
    HostAndPort hostAndPort = HostAndPort.fromString(workerAddress);
    InetSocketAddress socketAddress =
        new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
    GrpcServerAddress serverAddress = GrpcServerAddress.create(socketAddress);
    UserState userState = UserState.Factory.create(mConf, new Subject());

    FreeSpaceRequest.Builder freeSpaceRequestBuilder = FreeSpaceRequest.newBuilder();
    if (cl.hasOption(PERCENT_OPTION.getLongOpt())) {
      int percent = Integer.parseInt(cl.getOptionValue(PERCENT_OPTION.getLongOpt()));
      freeSpaceRequestBuilder.setPercent(percent);
    }
    if (cl.hasOption(TIER_OPTION.getLongOpt())) {
      String tierAlias = cl.getOptionValue(TIER_OPTION.getLongOpt());
      freeSpaceRequestBuilder.setTierAlias(tierAlias);
    }
    try (BlockWorkerClient blockWorkerClient =
             BlockWorkerClient.Factory.create(userState, serverAddress, mConf)) {
      blockWorkerClient.freeSpace(freeSpaceRequestBuilder.build());
    }
    return 0;
  }

  @Override
  public String getCommandName() {
    return "freeSpace";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "freeSpace [--percent <percent>] [--tier <tier>] <address>";
  }

  @Override
  public String getDescription() {
    return "Frees the space occupied by a worker in Alluxio."
        + " Specify -f to force freeing pinned files in the directory.";
  }
}
