/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.shell.command;

import java.io.IOException;

/**
 * An interface for all the commands that can be run from {@link tachyon.shell.TfsShell}.
 */
public interface TfsShellCommand {

  /**
   * Gets the command name as input from the shell.
   *
   * @return the command name
   */
  String getCommandName();

  /**
   * Checks if the arguments are valid.
   *
   * @param args the arguments for the command, excluding the command name
   * @return whether the args are valid
   */
  boolean validateArgs(String... args);

  /**
   * Runs the command.
   *
   * @param args the arguments for the command, excluding the command name
   * @throws IOException when the command fails
   */
  void run(String... args) throws IOException;

  /**
   * @return the usage information of the command
   */
  String getUsage();

  /**
   * @return the description information of the command
   */
  String getDescription();
}
