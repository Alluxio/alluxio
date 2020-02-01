package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;
import alluxio.util.ShellUtils;
import jdk.nashorn.tools.Shell;
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
import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class CollectAlluxioInfoCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectAlluxioInfoCommand.class);

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
    String mAlternative;

    public AlluxioCommand(String name, String alluxioPath, String cmd, String alternative) {
      super((alluxioPath + " " + cmd).split(" "));
      mName = name;
      mAlluxioPath = alluxioPath;
      mAlternative = alternative;
    }

    boolean hasAlternativeCommand() {
      return mAlternative == null;
    }

    AlluxioCommand getAlternativeCommand() {
      return new AlluxioCommand(mName, mAlluxioPath, mAlternative, null);
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
            String.format("ls -al -R %s", mFsContext.getClusterConf().get(PropertyKey.MASTER_JOURNAL_FOLDER))));
    // TODO(jiacheng): a command to find lost blocks
  }

  public List<AlluxioCommand> getCommands() {
    return mCommands;
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
    for(AlluxioCommand cmd : getCommands()) {
      CommandReturn cr = cmd.runWithOutput();

      if (cr.getExitCode() != 0) {
        String crStr = String.format("Command %s failed: %s", cmd, cr.getFormattedOutput());
        LOG.warn(crStr);
        output.write(crStr);

        // Try alternative command if there is one
        if (cmd.hasAlternativeCommand()) {
          AlluxioCommand alternativeCmd = cmd.getAlternativeCommand();
          String tryAgainMsg = String.format("Try alternative command %s", alternativeCmd);
          output.write(tryAgainMsg);
          LOG.warn(tryAgainMsg);

          CommandReturn tryAgain = alternativeCmd.runWithOutput();
          String tryAgainRes;
          if (tryAgain.getExitCode() != 0) {
            tryAgainRes = String.format("Alternative command %s failed: %s", alternativeCmd, tryAgain.getFormattedOutput());
            LOG.warn(tryAgainRes);
          } else {
            tryAgainRes = String.format("Alternative command %s succeeded: %s", alternativeCmd, tryAgain.getFormattedOutput());
            LOG.info(tryAgainRes);
          }
          output.write(tryAgainRes);
        }

        continue;
      }

      // Command completed
      String crStr = String.format("Command %s succeeded %s", cmd, cr.getFormattedOutput());
      LOG.info(crStr);
      output.write(crStr);
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
