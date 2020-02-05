package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CollectAlluxioInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);
  public static final String COMMAND_NAME="runAlluxioCheck";

  private static String OUTPUT_FILE_NAME = "alluxioInfo.txt";

  private String mAlluxioPath;
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
    mAlluxioPath = Paths.get(fsContext.getClusterConf().get(PropertyKey.WORK_DIR), "bin/alluxio")
            .toAbsolutePath().toString();
    registerCommands();
  }

  public static class AlluxioCommand extends ShellCommand {
    String mName;
    String mAlluxioPath;
    ShellCommand mAlternative;

    public AlluxioCommand(String name, String alluxioPath, String cmd, ShellCommand alternative) {
      super((alluxioPath + " " + cmd).split(" "));
      mName = name;
      mAlluxioPath = alluxioPath;
      mAlternative = alternative;
    }

    boolean hasAlternativeCommand() {
      return mAlternative != null;
    }

    ShellCommand getAlternativeCommand() {
      return mAlternative;
    }
  }

  private void registerCommands() {
    mCommands = new ArrayList<>();
    mCommands.add(new AlluxioCommand("getConf", mAlluxioPath, "getConf --master --source",
            null));
    mCommands.add(new AlluxioCommand("fsadmin", mAlluxioPath, "fsadmin report", null));
    mCommands.add(new AlluxioCommand("mount", mAlluxioPath, "fs mount", null));
    mCommands.add(new AlluxioCommand("version", mAlluxioPath, "version -r", null));
    mCommands.add(new AlluxioCommand("job", mAlluxioPath, "job ls", null));
    mCommands.add(new AlluxioCommand("fsadmin", mAlluxioPath, "fsadmin report", null));
    mCommands.add(new AlluxioCommand("journal", mAlluxioPath,
            String.format("fs ls -R %s", mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)),
            new ShellCommand(new String[]{"ls", "-al", "-R",
                            mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER)})));
    // TODO(jiacheng): a command to find lost blocks
  }

  public List<AlluxioCommand> getCommands() {
    return mCommands;
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
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
    for(AlluxioCommand cmd : getCommands()) {
      CommandReturn cr;
      String crStr = "";
      boolean cmdCompleted = false;
      try {
        cr = cmd.runWithOutput();
        if (cr.getExitCode() != 0) {
          crStr = String.format("Command %s failed: %s", cmd, cr.getFormattedOutput());
          LOG.warn(crStr);
        } else {
          // Command completed
          crStr = String.format("Command %s succeeded %s", cmd, cr.getFormattedOutput());
          LOG.info(crStr);
          cmdCompleted = true;
        }
      } catch (IOException e) {
        crStr = String.format("Command %s failed with exception %s", cmd, e.getMessage());
        LOG.warn(crStr);
        if (LOG.isDebugEnabled()) {
          e.printStackTrace();
        }
      }
      output.write(crStr);

      if (!cmdCompleted) {
        // Command failed, try alternative command
        if (cmd.hasAlternativeCommand()) {
          ShellCommand alternativeCmd = cmd.getAlternativeCommand();
          String tryAgainMsg = String.format("Try alternative command %s", alternativeCmd);
          output.write(tryAgainMsg);
          LOG.info(tryAgainMsg);

          String tryAgainRes = "";
          try {
            CommandReturn tryAgain = alternativeCmd.runWithOutput();
            if (tryAgain.getExitCode() != 0) {
              tryAgainRes = String.format("Alternative command %s failed: %s", alternativeCmd, tryAgain.getFormattedOutput());
              LOG.warn(tryAgainRes);
            } else {
              tryAgainRes = String.format("Alternative command %s succeeded: %s", alternativeCmd, tryAgain.getFormattedOutput());
              LOG.info(tryAgainRes);
            }
          } catch (IOException e) {
            tryAgainRes = String.format("Alternative command %s failed with exception: %s", alternativeCmd, e.getMessage());
            if (LOG.isDebugEnabled()) {
              e.printStackTrace();
            }
          }
          output.write(tryAgainRes);
        }
      }
    }

    File outputFile = generateOutputFile(targetDir, OUTPUT_FILE_NAME);
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
