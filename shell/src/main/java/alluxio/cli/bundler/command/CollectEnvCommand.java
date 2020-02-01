package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;
import alluxio.util.ShellUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectEnvCommand extends AbstractInfoCollectorCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CollectEnvCommand.class);

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

  private Map<String, ShellCommand> mCommands;
  private Map<String, ShellCommand> mCommandsBetter;

  public CollectEnvCommand(@Nullable FileSystemContext fsContext) {
    super(fsContext);
    mCommands = new HashMap<>();
    mCommandsBetter = new HashMap<>();
    registerCommands();
  }

  private void registerCommands() {
    registerCommand("ps", "ps -ef | grep alluxio*", null);
    registerCommand("env", "env", null);
    registerCommand("top", "top -b -n 1", "atop -b -n 1");
    registerCommand("mount", "mount", null);
    registerCommand("df", "df -H", null);
    registerCommand("ulimit", "ulimit -Ha", null);
    registerCommand("uname", "uname -a", null);
    registerCommand("hostname", "hostname", null);
    registerCommand("host ip", "hostname -i", null);
    registerCommand("host fqdn", "hostname -v", null);
    registerCommand("list Alluxio home", String.format("ls -al -R %s",
            mFsContext.getClusterConf().get(PropertyKey.HOME)), null);
    registerCommand("dig", "dig $(hostname -i)", null);
    registerCommand("nslookup", "nslookup $(hostname -i)", null);
    // TODO(jiacheng): does this stop?
    registerCommand("dstat", "dstat -cdgilmnprsty", null);
  }

  private void registerCommand(String name, String cmd, String betterVersion) {
    mCommands.put(name, new ShellCommand(cmd.split(" ")));
    if (betterVersion != null && betterVersion.trim() != "") {
      mCommandsBetter.put(name, new ShellCommand(betterVersion.split(" ")));
    }
  }

  @Override
  public String getCommandName() {
    return "collectEnv";
  }

  @Override
  public boolean hasSubCommand() {
    return false;
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

    // Output buffer stream
    StringWriter outputBuffer = new StringWriter();

    for (String cmdName : mCommands.keySet()) {
      boolean complete = false;

      // if there is a better option, try it first
      if (mCommandsBetter.containsKey(cmdName)) {
        ShellCommand betterCmd = mCommandsBetter.get(cmdName);
        CommandReturn crBetter = betterCmd.runWithOutput();
        String cmdBetterMsg;
        if (crBetter.getExitCode() == 0) {
          complete = true;
          cmdBetterMsg = String.format("Better option cmd %s succeeded: %s", betterCmd.toString(),
                  crBetter.getFormattedOutput());
          LOG.warn(cmdBetterMsg);
          ret = crBetter.getExitCode();
        } else {
          cmdBetterMsg = String.format("Better option cmd %s failed: %s", betterCmd.toString(),
                  crBetter.getFormattedOutput());
          LOG.info(cmdBetterMsg);
        }
        outputBuffer.write(cmdBetterMsg);
      }

      // If the better option does not work, fall back
      if (!complete) {
        ShellCommand cmd = mCommands.get(cmdName);
        CommandReturn cr = cmd.runWithOutput();
        String cmdMsg;
        if (cr.getExitCode() == 0) {
          cmdMsg = String.format("Better option cmd %s succeeded: %s", cmd.toString(),
                  cr.getFormattedOutput());
          LOG.warn(cmdMsg);
          ret = cr.getExitCode();
        } else {
          cmdMsg = String.format("Better option cmd %s failed: %s", cmd.toString(),
                  cr.getFormattedOutput());
          LOG.info(cmdMsg);
        }
        outputBuffer.write(cmdMsg);
      }
    }

    // output the buffer
    File outputFile = getOutputFile(targetDir, String.format("%s.txt", getCommandName()));
    LOG.info(String.format("Finished all commands. Writing to output file %s", outputFile.getAbsolutePath()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return ret;
  }

  @Override
  public String getUsage() {
    return "collectEnv";
  }

  @Override
  public String getDescription() {
    return "Collect environment information by running a set of commands. ";
  }
}