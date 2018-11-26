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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.conf.Source;
import alluxio.master.MasterClientConfig;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Shell for admin to manage file system.
 */
public final class FileSystemAdminShell extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemAdminShell.class);

  /**
   * Context shared with fsadmin commands.
   */
  private Context mContext;

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
    if (!ConfigurationUtils.masterHostConfigured() && args.length > 0) {
      System.out.println("Cannot run alluxio fsadmin shell as master hostname is not configured.");
      System.exit(1);
    }
    // Reduce the RPC retry max duration to fall earlier for CLIs
    Configuration.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    FileSystemAdminShell fsAdminShell = new FileSystemAdminShell();
    System.exit(fsAdminShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "fsadmin";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    Context context = new Context(
        new RetryHandlingFileSystemMasterClient(MasterClientConfig.defaults()),
        new RetryHandlingBlockMasterClient(MasterClientConfig.defaults()),
        new RetryHandlingMetaMasterClient(MasterClientConfig.defaults()),
        System.out
    );
    return CommandUtils.loadCommands(FileSystemAdminShell.class.getPackage().getName(),
        new Class[] {Context.class}, new Object[] {context});
  }
}
