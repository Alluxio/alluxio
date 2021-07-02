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

package alluxio.cli.extensions;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;

import java.util.Map;

/**
 * Utility for managing Alluxio extensions.
 */
public final class ExtensionsShell extends AbstractShell {
  /**
   * Construct a new instance of {@link ExtensionsShell}.
   *
   * @param conf the Alluxio configuration to use when instantiating the shell
   */
  ExtensionsShell(InstancedConfiguration conf) {
    super(null, null, conf);
  }

  /**
   * Manage Alluxio extensions.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    ExtensionsShell extensionShell =
        new ExtensionsShell(ServerConfiguration.global());
    System.exit(extensionShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "extensions";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(ExtensionsShell.class.getPackage().getName(), null, null);
  }
}
