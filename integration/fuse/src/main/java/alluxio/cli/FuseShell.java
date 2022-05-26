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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Fuse shell to run Fuse special commands.
 */
public final class FuseShell {
  private static final Logger LOG = LoggerFactory.getLogger(FuseShell.class);

  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;
  private final Map<String, Command> mCommands;

  /**
   * Creates a new instance of {@link FuseShell}.
   *
   * @param fs Alluxio file system
   * @param conf Alluxio configuration
   */
  public FuseShell(FileSystem fs, AlluxioConfiguration conf) {
    mFileSystem = fs;
    mConf = conf;
    mCommands = CommandUtils.loadCommands(FuseShell.class.getPackage().getName(),
        new Class [] {FileSystem.class, AlluxioConfiguration.class},
        new Object[] {mFileSystem, mConf});
  }

  /**
   * Checks if the given uri contains Fuse special command.
   *
   * @param uri check whether the uri contains Fuse special command
   * @return true if the path is a special path, otherwise false
   */
  public boolean isSpecialCommand(AlluxioURI uri) {
    int index = uri.getPath().lastIndexOf(AlluxioURI.SEPARATOR);
    return index != -1 && uri.getPath().substring(index).startsWith(Constants
        .ALLUXIO_CLI_PATH);
  }

  /**
   * @param uri that includes command information
   * @return a mock URIStatus instance
   */
  public URIStatus runCommand(AlluxioURI uri) throws InvalidArgumentException {
    // TODO(bingzheng): extend some other operations.
    AlluxioURI path = uri.getParent();
    int index = uri.getPath().lastIndexOf(Constants.ALLUXIO_CLI_PATH);
    String cmdsInfo = uri.getPath().substring(index);
    String[] cmds = cmdsInfo.split("\\.");

    if (cmds.length <= 2) {
      logUsage();
      throw new InvalidArgumentException("Command is needed in Fuse shell");
    }

    FuseCommand command = (FuseCommand) mCommands.get(cmds[2]);
    if (command == null) {
      logUsage();
      throw new InvalidArgumentException(String.format("%s is an unknown command.", cmds[2]));
    }
    try {
      String [] currArgs = Arrays.copyOfRange(cmds, 2, cmds.length);
      while (command.hasSubCommand()) {
        if (currArgs.length < 2) {
          throw new InvalidArgumentException("No sub-command is specified");
        }
        if (!command.getSubCommands().containsKey(currArgs[1])) {
          throw new InvalidArgumentException("Unknown sub-command: " + currArgs[1]);
        }
        command = (FuseCommand) command.getSubCommands().get(currArgs[1]);
        currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);
      }
      command.validateArgs(Arrays.copyOfRange(currArgs, 1, currArgs.length));
      return command.run(path, Arrays.copyOfRange(currArgs, 1, currArgs.length));
    } catch (InvalidArgumentException e) {
      LOG.info(command.getDescription());
      LOG.info("Usage: " + command.getUsage());
      throw new InvalidArgumentException(String.format("Invalid arguments for command %s, "
              + "For detailed usage please see the log formation above.",
          command.getCommandName()), e);
    }
  }

  private void logUsage() {
    // TODO(lu) better log fuse shell description
    LOG.info(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED.getDescription());
  }
}
