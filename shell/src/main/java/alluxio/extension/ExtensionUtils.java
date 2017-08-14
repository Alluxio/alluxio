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

import alluxio.cli.Extension;
import alluxio.cli.command.Command;
import alluxio.cli.command.CommandUtils;
import alluxio.extension.command.ExtensionCommand;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by {@link Extension}.
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
    Map<String, ExtensionCommand> extensionCommandsMap = new HashMap<>();
    Map<String, Command> commandsMap =
        CommandUtils.loadCommands(ExtensionCommand.class, null, null);
    for (Map.Entry<String, Command> cmdPair : commandsMap.entrySet()) {
      if (cmdPair.getValue() instanceof ExtensionCommand) {
        extensionCommandsMap.put(cmdPair.getKey(), (ExtensionCommand) cmdPair.getValue());
      }
    }
    return extensionCommandsMap;
  }
}
