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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.conf.Source;
import alluxio.master.MasterClientContext;
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
   *
   * @param alluxioConf Alluxio configuration
   */
  public FileSystemAdminShell(InstancedConfiguration alluxioConf) {
    super(null, alluxioConf);
  }

  /**
   * Manage Alluxio file system.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    if (!ConfigurationUtils.masterHostConfigured(conf) && args.length > 0) {
      System.out.println("Cannot run alluxio fsadmin shell as master hostname is not configured.");
      System.exit(1);
    }
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    FileSystemAdminShell fsAdminShell = new FileSystemAdminShell(conf);
    System.exit(fsAdminShell.run(args));
  }

  @Override
  protected String getShellName() {
    return "fsadmin";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    ClientContext ctx = ClientContext.create(mConfiguration);
    MasterClientContext masterConfig = MasterClientContext.newBuilder(ctx).build();
    Context context = new Context(
        new RetryHandlingFileSystemMasterClient(masterConfig),
        new RetryHandlingBlockMasterClient(masterConfig),
        new RetryHandlingMetaMasterClient(masterConfig),
        System.out
    );
    return CommandUtils.loadCommands(FileSystemAdminShell.class.getPackage().getName(),
        new Class[] {Context.class, AlluxioConfiguration.class}, new Object[] {context,
            mConfiguration});
  }
}
