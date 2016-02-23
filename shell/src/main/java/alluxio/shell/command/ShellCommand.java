/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.shell.AlluxioShell;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * An interface for all the commands that can be run from {@link AlluxioShell}.
 */
public interface ShellCommand {

  /**
   * Gets the command name as input from the shell.
   *
   * @return the command name
   */
  String getCommandName();

  /**
   * Parses and validates the arguments.
   *
   * @param args the arguments for the command, excluding the command name
   * @return the parsed command line object. If the arguments are invalid, return null
   */
  CommandLine parseAndValidateArgs(String... args);

  /**
   * Runs the command.
   *
   * @param cl the parsed command line for the arguments
   * @throws IOException when the command fails
   */
  void run(CommandLine cl) throws IOException;

  /**
   * @return the usage information of the command
   */
  String getUsage();

  /**
   * @return the description information of the command
   */
  String getDescription();
}
