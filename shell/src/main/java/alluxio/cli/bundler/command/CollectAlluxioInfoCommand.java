package alluxio.cli.bundler.command;

import alluxio.cli.bundler.RunCommandUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectAlluxioInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);

  public CollectAlluxioInfoCommand(@Nullable InstancedConfiguration conf) {
    super(conf);
  }

  private static class AlluxioCommand {
    String mName;
    String mCmd;
    String mAlternative;
    AlluxioCommand(String name, String cmd, String alternative) {
      mName = name;
      mCmd = cmd;
      mAlternative = alternative;
    }
    String[] getCommand() {
      // TODO(jiacheng): where to get bin/ from?
      return String.format("bin/alluxio %s", mCmd).split(" ");
    }
    String[] getAlternativeCommand() {
      if (mAlternative == null) {
        return new String[]{};
      }
      return mAlternative.split(" ");
    }
  }

  private List<AlluxioCommand> mCommands;

  private void registerCommands() {
    mCommands = new ArrayList<>();
    mCommands.add(new AlluxioCommand("getConf", "getConf --master --source",
            null));
    mCommands.add(new AlluxioCommand("fsadmin", "fsadmin report", null));
    mCommands.add(new AlluxioCommand("mount", "fs mount", null));
    mCommands.add(new AlluxioCommand("version", "version -r", null));
    mCommands.add(new AlluxioCommand("job", "job ls", null));
    mCommands.add(new AlluxioCommand("fsadmin", "fsadmin report", null));
    mCommands.add(new AlluxioCommand("journal",
            String.format("fs ls -R %s", mConf.get(PropertyKey.MASTER_JOURNAL_FOLDER)),
            String.format("ls -al -R %s", mConf.get(PropertyKey.MASTER_JOURNAL_FOLDER))));
    // TODO(jiacheng): a command to find lost blocks
  }

  @Override
  public String getCommandName() {
    return "runAlluxioCheck";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;
    StringWriter output = new StringWriter();

    for(AlluxioCommand cmd : mCommands) {
      //TODO(jiacheng): Output with format

      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(cmd.getCommand());
      output.write(cr.getOutput());

      if (cr.getStatusCode() != 0) {
        LOG.error(String.format("Command %s returned status %s. Try alternative command.", Arrays.toString(cmd.getCommand()),
                cr.getStatusCode()));
        String[] alternativeCmd = cmd.getAlternativeCommand();
        if (alternativeCmd.length == 0) {
          LOG.info(String.format("No alternative command for %s"));
        }

        output.write("Running alternative command " + Arrays.toString(alternativeCmd));
        LOG.info(String.format("Alternative command: %s", Arrays.toString(alternativeCmd)));
        cr = RunCommandUtils.runCommandNoFail(alternativeCmd);
        output.write(cr.getOutput());

        ret |= cr.getStatusCode();
      }
    }

    return ret;
  }

  @Override
  public String getUsage() {
    return null;
  }

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public String getWorkingDirectory() {
    // TODO(jiacheng): create if not exist
    return Paths.get(super.getWorkingDirectory(), this.getCommandName()).toString();
  }
}
