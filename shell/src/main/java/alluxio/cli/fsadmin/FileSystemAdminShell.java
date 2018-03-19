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

package alluxio.cli.fsadmin;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.util.ConfigurationUtils;

import java.util.Map;

/**
 * Shell for admin to manage file system.
 */
public final class FileSystemAdminShell extends AbstractShell {
  /**
   * Construct a new instance of {@link FileSystemAdminShell}.
   */
  public FileSystemAdminShell() {
    super(null);
  }

  /**
   * Manage Alluxio file system.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println("Cannot run fsadmin shell as master hostname is not configured.");
      System.exit(1);
    }
    FileSystemAdminShell fsAdminShell = new FileSystemAdminShell();
    System.exit(fsAdminShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "fsadmin";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    return CommandUtils.loadCommands(FileSystemAdminShell.class.getPackage().getName(),
        new Class[] {}, new Object[] {});
  }
}
