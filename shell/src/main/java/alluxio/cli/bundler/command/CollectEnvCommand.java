/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.bundler.command;

import alluxio.client.file.FileSystemContext;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
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
public class CollectEnvCommand extends AbstractInfoCollectorCommand {
  public static final String COMMAND_NAME = "collectEnv";
  private static final Logger LOG = LoggerFactory.getLogger(CollectEnvCommand.class);

  protected Map<String, ShellCommand> mCommands;
  protected Map<String, ShellCommand> mCommandsAlt;

  /**
   * Creates a new instance of {@link CollectEnvCommand}.
   *
   * @param fsContext the {@link FileSystemContext} to execute in
   * */
  public CollectEnvCommand(FileSystemContext fsContext) {
    super(fsContext);
    mCommands = new HashMap<>();
    mCommandsAlt = new HashMap<>();
    registerCommands();
  }

  private void registerCommands() {
    registerCommand("ps", new ShellCommand(new String[]{"ps", "-ef", "|grep alluxio*"}), null);
    registerCommand("env", new ShellCommand(new String[]{"env"}), null);
    registerCommand("top", new ShellCommand(new String[]{"atop", "-b", "-n", "1"}),
            new ShellCommand(new String[]{"top", "-b", "-n", "1"}));
    registerCommand("mount", new ShellCommand(new String[]{"mount"}), null);
    registerCommand("df", new ShellCommand(new String[]{"df", "-H"}), null);
    registerCommand("ulimit", new ShellCommand(new String[]{"ulimit -Ha"}), null);
    registerCommand("uname", new ShellCommand(new String[]{"uname", "-a"}), null);
    registerCommand("hostname", new ShellCommand(new String[]{"hostname"}), null);
    registerCommand("host ip", new ShellCommand(new String[]{"hostname", "-i"}), null);
    registerCommand("host fqdn", new ShellCommand(new String[]{"hostname", "-v"}), null);
    registerCommand("list Alluxio home",
            new ShellCommand(new String[]{String.format("ls -al -R %s",
                    mFsContext.getClusterConf().get(PropertyKey.HOME))}), null);
    registerCommand("dig", new ShellCommand(new String[]{"dig $(hostname -i)"}), null);
    registerCommand("nslookup", new ShellCommand(new String[]{"nslookup", "$(hostname -i)"}), null);
    // TODO(jiacheng): does this stop?
    registerCommand("dstat", new ShellCommand(new String[]{"dstat", "-cdgilmnprsty"}), null);
  }

  protected void registerCommand(String name, ShellCommand cmd, ShellCommand alternativeCmd) {
    mCommands.put(name, cmd);
    if (alternativeCmd != null) {
      mCommandsAlt.put(name, alternativeCmd);
    }
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public boolean hasSubCommand() {
    return false;
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

  @Override
  public String getUsage() {
    return "collectEnv <outputPath>";
  }

  @Override
  public String getDescription() {
    return "Collect environment information by running a set of shell commands. ";
  }
}
