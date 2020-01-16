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
import alluxio.client.job.RetryHandlingJobMasterClient;
import alluxio.client.journal.RetryHandlingJournalMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.conf.Source;
import alluxio.master.MasterClientContext;
import alluxio.util.ConfigurationUtils;
import alluxio.worker.job.JobMasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Shell for admin to manage file system.
 */
public final class FileSystemAdminShell extends AbstractShell {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemAdminShell.class);

  /**
   * Construct a new instance of {@link FileSystemAdminShell}.
   *
   * @param alluxioConf Alluxio configuration
   */
  public FileSystemAdminShell(InstancedConfiguration alluxioConf) {
    super(null, null, alluxioConf);
  }

  /**
   * Manage Alluxio file system.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) throws IOException {
    int ret;
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    if (!ConfigurationUtils.masterHostConfigured(conf) && args.length > 0) {
      System.out.println(ConfigurationUtils
          .getMasterHostNotConfiguredMessage("Alluxio fsadmin shell"));
      System.exit(1);
    }
    // Reduce the RPC retry max duration to fall earlier for CLIs
    conf.set(PropertyKey.USER_RPC_RETRY_MAX_DURATION, "5s", Source.DEFAULT);
    try (FileSystemAdminShell fsAdminShell = new FileSystemAdminShell(conf)) {
      ret = fsAdminShell.run(args);
    }
    System.exit(ret);
  }

  @Override
  protected String getShellName() {
    return "fsadmin";
  }

  @Override
  protected Map<String, Command> loadCommands() {
    ClientContext ctx = ClientContext.create(mConfiguration);
    MasterClientContext masterConfig = MasterClientContext.newBuilder(ctx).build();
    JobMasterClientContext jobMasterConfig = JobMasterClientContext.newBuilder(ctx).build();
    Context adminContext = new Context(
        new RetryHandlingFileSystemMasterClient(masterConfig),
        new RetryHandlingBlockMasterClient(masterConfig),
        new RetryHandlingMetaMasterClient(masterConfig),
        new RetryHandlingMetaMasterConfigClient(masterConfig),
        new RetryHandlingMetricsMasterClient(masterConfig),
        new RetryHandlingJournalMasterClient(masterConfig),
        new RetryHandlingJournalMasterClient(jobMasterConfig),
        new RetryHandlingJobMasterClient(jobMasterConfig),
        System.out
    );
    return CommandUtils.loadCommands(FileSystemAdminShell.class.getPackage().getName(),
        new Class[] {Context.class, AlluxioConfiguration.class},
        new Object[] {mCloser.register(adminContext), mConfiguration});
  }
}
