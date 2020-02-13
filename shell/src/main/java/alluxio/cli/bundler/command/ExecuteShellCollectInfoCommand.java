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
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;
import alluxio.util.ShellUtils;

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
  }

  protected abstract void registerCommands();

  // TODO(jiacheng): phase 2 refactor to use a chainable ShellCommand structure
  protected void registerCommand(String name, ShellCommand cmd, ShellCommand alternativeCmd) {
    mCommands.put(name, cmd);
    if (alternativeCmd != null) {
      mCommandsAlt.put(name, alternativeCmd);
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Determine the working dir path
    mWorkingDirPath = getWorkingDirectory(cl);

    // Output buffer stream
    StringWriter outputBuffer = new StringWriter();

    for (Map.Entry<String, ShellCommand> entry : mCommands.entrySet()) {
      String cmdName = entry.getKey();
      ShellCommand cmd = entry.getValue();
      ShellCommand backupCmd = mCommandsAlt.getOrDefault(cmdName, null);
      CommandReturn cr = ShellUtils.execCmdWithBackup(cmd, backupCmd);
      outputBuffer.write(cr.getFormattedOutput());

      // If neither command works, log a warning instead of returning with error state
      // This is because a command can err due to:
      // 1. The executable does not exist. This results in an IOException.
      // 2. The command is not compatible to the system, eg. Mac.
      // 3. The command is wrong.
      // We choose to tolerate the error state due since we cannot correct state 1 or 2.
      if (cr.getExitCode() != 0) {
        LOG.warn("Command %s failed with exit code %d", cmdName, cr.getExitCode());
      }
    }

    // output the logs
    File outputFile = generateOutputFile(mWorkingDirPath,
            String.format("%s.txt", getCommandName()));
    LOG.info(String.format("Finished all commands. Writing to output file %s",
            outputFile.getAbsolutePath()));
    FileUtils.writeStringToFile(outputFile, outputBuffer.toString());

    return 0;
  }
}
