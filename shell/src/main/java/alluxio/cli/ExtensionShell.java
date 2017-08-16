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

import alluxio.extension.ExtensionUtils;
import alluxio.extension.command.ExtensionCommand;

import java.util.Map;

/**
 * Utility for managing Alluxio extensions.
 */
public final class ExtensionShell extends AbstractShell {
  /**
   * Manage Alluxio extensions.
   *
   * @param args [] Array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    ExtensionShell extensionShell = new ExtensionShell();
    System.exit(extensionShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "extension";
  }

  @Override
  protected Map<String, ExtensionCommand> loadCommands() {
    return ExtensionUtils.loadCommands();
  }
}