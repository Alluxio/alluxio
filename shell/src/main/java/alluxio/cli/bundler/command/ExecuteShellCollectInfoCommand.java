package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Command to run a set of shell commands to get system information.
 * */
public abstract class ExecuteShellCollectInfoCommand extends AbstractCollectInfoCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ExecuteShellCollectInfoCommand.class);

  protected Map<String, ShellCommand> mCommands;
  protected Map<String, ShellCommand> mCommandsAlt;

  /**
   * Creates a new instance of {@link ExecuteShellCollectInfoCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public ExecuteShellCollectInfoCommand(FileSystemContext fsContext) {
    super(fsContext);
    mCommands = new HashMap<>();
    mCommandsAlt = new HashMap<>();
    registerCommands();
  }

  protected abstract void registerCommands();

  protected void registerCommand(String name, ShellCommand cmd, ShellCommand alternativeCmd) {
    mCommands.put(name, cmd);
    if (alternativeCmd != null) {
      mCommandsAlt.put(name, alternativeCmd);
    }
  }

  protected Pair<Integer, String> runAndFormatOutput(ShellCommand cmd) {
    String crStr;
    int cmdExitCode;
    try {
      CommandReturn cr = cmd.runWithOutput();
      cmdExitCode = cr.getExitCode();
      if (cr.getExitCode() != 0) {
        crStr = String.format("Command %s failed: %s", cmd, cr.getFormattedOutput());
        LOG.warn(crStr);
      } else {
        // Command succeeded
        crStr = String.format("Command %s succeeded %s", cmd, cr.getFormattedOutput());
        LOG.info(crStr);
      }
    } catch (IOException e) {
      cmdExitCode = 1;
      crStr = String.format("Command %s failed with exception %s", cmd, e.getMessage());
      LOG.warn(crStr);
      LOG.debug("%s", e);
    }
    return new Pair<>(cmdExitCode, crStr);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int ret = 0;

    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    // Output buffer stream
    StringWriter outputBuffer = new StringWriter();

    for (String cmdName : mCommands.keySet()) {
      int cmdExitCode = 0;

      ShellCommand cmd = mCommands.get(cmdName);
      Pair<Integer, String> cmdOutput = runAndFormatOutput(cmd);
      outputBuffer.write(cmdOutput.getSecond());

      if (cmdOutput.getFirst() == 0) {
        continue;
      } else {
        cmdExitCode = cmdOutput.getFirst();
      }

      // if there is a backup option, try it first
      if (mCommandsAlt.containsKey(cmdName)) {
        ShellCommand betterCmd = mCommandsAlt.get(cmdName);
        Pair<Integer, String> cmdAltOutput = runAndFormatOutput(betterCmd);
        outputBuffer.write(cmdAltOutput.getSecond());
        // If the backup option succeeded, count as successful
        if (cmdAltOutput.getFirst() == 0) {
          cmdExitCode = 0;
        }
      }

      // Keep only the larger return code
      if (cmdExitCode != 0 && ret == 0) {
        ret = cmdExitCode;
      }
    }

    // output the logs
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s.txt", getCommandName()));
    LOG.info(String.format("Finished all commands. Writing to output file %s",
            outputFile.getAbsolutePath()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return ret;
  }
}
