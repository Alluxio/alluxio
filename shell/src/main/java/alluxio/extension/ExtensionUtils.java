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

package alluxio.extension;

import alluxio.cli.ExtensionShell;
import alluxio.cli.CommandUtils;
import alluxio.extension.command.ExtensionCommand;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by instances of {@link ExtensionShell}.
 */
@ThreadSafe
public final class ExtensionUtils {

  private ExtensionUtils() {} // prevent instantiation

  /**
   * Gets all supported {@link ExtensionCommand} classes instances and load them into a map.
   *
   * @return a mapping from command name to command instance
   */
  public static Map<String, ExtensionCommand> loadCommands() {
    return CommandUtils.loadCommands(ExtensionCommand.class, null, null);
  }
}