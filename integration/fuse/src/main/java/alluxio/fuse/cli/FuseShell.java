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

package alluxio.fuse.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Fuse shell to run Fuse special commands.
 */
public final class FuseShell {
  private static final Logger LOG = LoggerFactory.getLogger(FuseShell.class);
  public static final String ALLUXIO_CLI_PATH = "/.alluxiocli";

  private final AlluxioConfiguration mConf;
  private final FileSystem mFileSystem;
  private final Map<String, FuseCommand> mCommands;

  /**
   * Creates a new instance of {@link FuseShell}.
   * @param fs Alluxio file system
   * @param conf Alluxio configuration
   */
  public FuseShell(FileSystem fs, AlluxioConfiguration conf) {
    mFileSystem = fs;
    mConf = conf;
    mCommands = loadCommands();
  }

  private Map<String, FuseCommand> loadCommands() {
    Map<String, FuseCommand> commandsMap = new HashMap<>();
    Reflections reflections = new Reflections(FuseCommand.class.getPackage().getName());
    for (Class<? extends FuseCommand> cls : reflections.getSubTypesOf(FuseCommand.class)) {
      // Add commands from <pkgName>.command.*
      if (cls.getPackage().getName().equals(FuseShell.class.getPackage().getName() + ".command")
          && !Modifier.isAbstract(cls.getModifiers())) {
        // Only instantiate a concrete class
        FuseCommand cmd = CommonUtils.createNewClassInstance(cls,
            new Class [] {FileSystem.class, AlluxioConfiguration.class},
            new Object[] {mFileSystem, mConf});
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
  }

  /**
   * Checks if the given uri contains Fuse special command.
   *
   * @param uri check whether the uri contains Fuse special command
   * @return true of if the path is a special path, otherwise false
   */
  public boolean isFuseSpecialCommand(AlluxioURI uri) {
    int index = uri.getPath().lastIndexOf(AlluxioURI.SEPARATOR);
    return index != -1 && uri.getPath().substring(index).startsWith(ALLUXIO_CLI_PATH);
  }

  /**
   * @param uri that include command information
   * @return a mock URIStatus instance
   */
  public URIStatus runCommand(AlluxioURI uri) throws InvalidArgumentException {
    // TODO(bingzheng): extend some other operations.
    AlluxioURI path = uri.getParent();
    int index = uri.getPath().lastIndexOf(ALLUXIO_CLI_PATH);
    String cmdsInfo = uri.getPath().substring(index);
    String [] cmds = cmdsInfo.split("\\.");

    if (cmds.length <= 2) {
      logUsage();
      throw new InvalidArgumentException("Command is needed in Fuse shell");
    }

    FuseCommand command = mCommands.get(cmds[2]);
    if (command == null) {
      logUsage();
      throw new InvalidArgumentException(String.format("%s is an unknown command.", cmds[2]));
    }
    String [] currArgs = Arrays.copyOfRange(cmds, 2, cmds.length);
    try {
      while (command.hasSubCommand()) {
        if (currArgs.length < 2) {
          throw new InvalidArgumentException("No sub-command is specified");
        }
        if (!command.getSubCommands().containsKey(currArgs[1])) {
          throw new InvalidArgumentException("Unknown sub-command: " + currArgs[1]);
        }
        command = command.getSubCommands().get(currArgs[1]);
        currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);
      }
      command.validateArgs(Arrays.copyOfRange(currArgs, 1, currArgs.length));
    } catch (Exception e) {
      LOG.info(e.getMessage());
      LOG.info(command.getDescription());
      LOG.info("Usage: " + command.getUsage());
      throw new InvalidArgumentException(String.format("Invalid arguments for command %s",
          command.getCommandName()));
    }
    return command.run(path, Arrays.copyOfRange(currArgs, 1, cmds.length));
  }

  private void logUsage() {
    // TODO(lu) better log fuse shell description
    LOG.info(PropertyKey.FUSE_SPECIAL_COMMAND_ENABLED.getDescription());
  }
}
