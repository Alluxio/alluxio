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
import alluxio.client.file.URIStatus;
import alluxio.exception.status.InvalidArgumentException;

import java.util.HashMap;
import java.util.Map;

public interface FuseCommand {
  /**
   * Gets the command name as input from the shell.
   *
   * @return the command name
   */
  String getCommandName();

  /**
   * If a command has sub-commands, the first argument should be the sub-command's name,
   * all arguments and options will be parsed for the sub-command.
   *
   * @return whether this command has sub-commands
   */
  default boolean hasSubCommand() {
    return false;
  }

  /**
   * @return a map from sub-command names to sub-command instances
   */
  default Map<String, FuseCommand> getSubCommands() {
    return new HashMap<>();
  }

  /**
   * Checks if the arguments are valid or throw InvalidArgumentException.
   * @param argv args need to be validated
   * @throws InvalidArgumentException when arguments are not valid
   */
  default void validateArgs(String[] argv) throws InvalidArgumentException {}

  /**
   * Runs the command.
   *
   * @param path the path uri from fuse command
   * @param argv args from fuse command
   * @return the result of running the command
   */
  default URIStatus run(AlluxioURI path, String[] argv) throws InvalidArgumentException {
    return null;
  }

  /**
   * @return the usage information of the command
   */
  String getUsage();

  /**
   * @return the description information of the command
   */
  String getDescription();
}
