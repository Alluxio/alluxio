package alluxio.cli.fsadmin.command;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

public class StateLockHoldersCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public StateLockHoldersCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "statelock";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    for (String stateLockHolder : mFsClient.getStateLockHolders()) {
      System.out.println(stateLockHolder);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "statelock";
  }

  @Override
  public String getDescription() {
    return "statelock returns all waiters and holders of the state lock.";
  }


}
