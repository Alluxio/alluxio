package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectAlluxioInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);

  private static String OUTPUT_FILE_NAME = "alluxioInfo.txt";

  private List<AlluxioCommand> mCommands;

  private static final Option FORCE_OPTION =
          Option.builder("f")
                  .required(false)
                  .hasArg(false)
                  .desc("ignores existing work")
                  .build();

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(FORCE_OPTION);
  }

  public CollectAlluxioInfoCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
    registerCommands();
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
            String.format("fs ls -R %s", mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)),
            String.format("ls -al -R %s", mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER))));
    // TODO(jiacheng): a command to find lost blocks
  }

  @Override
  public String getCommandName() {
    return "runAlluxioCheck";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    String targetDir = getDestDir(cl);
    boolean force = cl.hasOption("f");

    // Skip if previous work can be reused.
    if (!force && foundPreviousWork(targetDir)) {
      LOG.info("Found previous work. Skipped.");
      return ret;
    }

    StringWriter output = new StringWriter();

    for(AlluxioCommand cmd : mCommands) {
      RunCommandUtils.CommandReturn cr = RunCommandUtils.runCommandNoFail(cmd.getCommand());
      output.write(cr.getFormattedOutput());

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
        output.write(cr.getFormattedOutput());

        // TODO(jiacheng): What status code makes sense?
        ret |= cr.getStatusCode();
      }
    }

    File outputFile = getOutputFile(targetDir, OUTPUT_FILE_NAME);
    FileUtils.writeStringToFile(outputFile, output.toString());

    return ret;
  }

  @Override
  public String getUsage() {
    return "runAlluxioCheck";
  }

  @Override
  public String getDescription() {
    return "Run a list of Alluxio commands";
  }
}
