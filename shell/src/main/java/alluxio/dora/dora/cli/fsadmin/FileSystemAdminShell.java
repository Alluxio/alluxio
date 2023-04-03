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

package alluxio.dora.dora.cli.fsadmin;

import alluxio.dora.dora.ClientContext;
import alluxio.dora.dora.cli.AbstractShell;
import alluxio.dora.dora.cli.Command;
import alluxio.dora.dora.cli.CommandUtils;
import alluxio.dora.dora.cli.fsadmin.command.Context;
import alluxio.dora.dora.client.block.RetryHandlingBlockMasterClient;
import alluxio.dora.dora.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.dora.dora.client.job.RetryHandlingJobMasterClient;
import alluxio.dora.dora.client.journal.RetryHandlingJournalMasterClient;
import alluxio.dora.dora.client.meta.RetryHandlingMetaMasterClient;
import alluxio.dora.dora.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.dora.dora.client.metrics.RetryHandlingMetricsMasterClient;
import alluxio.dora.dora.conf.AlluxioConfiguration;
import alluxio.dora.dora.conf.Configuration;
import alluxio.dora.dora.conf.InstancedConfiguration;
import alluxio.dora.dora.conf.PropertyKey;
import alluxio.dora.dora.conf.Source;
import alluxio.dora.dora.util.ConfigurationUtils;
import alluxio.dora.dora.worker.job.JobMasterClientContext;
import alluxio.dora.dora.master.MasterClientContext;

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
  public FileSystemAdminShell(AlluxioConfiguration alluxioConf) {
    super(null, null, alluxioConf);
  }

  /**
   * Manage Alluxio file system.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) throws IOException {
    int ret;
    InstancedConfiguration conf = Configuration.modifiableGlobal();
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
