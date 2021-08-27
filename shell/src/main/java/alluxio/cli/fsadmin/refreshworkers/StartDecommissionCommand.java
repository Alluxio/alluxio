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

package alluxio.cli.fsadmin.refreshworkers;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Start worker decommission process.
 */
public class StartDecommissionCommand extends AbstractFsAdminCommand {
  private static final Logger LOG = LoggerFactory.getLogger(StartDecommissionCommand.class);

  private static final String HELP_OPTION_NAME = "h";
  private static final Option HELP_OPTION =
      Option.builder(HELP_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();
  private static final Option EXCLUDED_HOSTS_OPTION =
      Option.builder()
          .longOpt("excluded-hosts")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-hosts")
          .desc("A list of excluded worker hosts separated by comma.")
          .build();
  private static final Option EXCLUDED_WORKER_HOST =
      Option.builder()
          .longOpt("excluded-worker")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-worker")
          .desc("A specific excluded worker host.")
          .build();
  private final AlluxioConfiguration mAlluxioConf;

  private void readItemsFromOptionString(Set<String> localityIds,
      String argOption) {
    for (String locality : StringUtils.split(argOption, ",")) {
      locality = locality.trim().toUpperCase();
      if (!locality.isEmpty()) {
        localityIds.add(locality);
      }
    }
  }

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public StartDecommissionCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Set<String> excludedWorkerSet = new HashSet<>();
    if (cl.hasOption(HELP_OPTION_NAME)) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    } else if (cl.hasOption(EXCLUDED_HOSTS_OPTION.getLongOpt())) {
      String hosts = cl.getOptionValue(EXCLUDED_HOSTS_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(excludedWorkerSet, hosts);
    } else if (cl.hasOption(EXCLUDED_WORKER_HOST.getLongOpt())) {
      String excludedWorker = cl.getOptionValue(EXCLUDED_WORKER_HOST.getLongOpt()).trim();
      excludedWorkerSet.add(excludedWorker);
    }
    if (excludedWorkerSet.isEmpty()) {
      excludedWorkerSet = ConfigurationUtils.getExWorkerHostnames(mAlluxioConf);
    }
    FileSystemAdminShellUtils.checkMasterClientService(mAlluxioConf);
    boolean success;
    try {
      success = mBlockClient.startDecommission(excludedWorkerSet);
      if (!success) {
        System.out.println("Worker decommission failed!");
        return -1;
      }
    } catch (IOException e) {
      return -1;
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(HELP_OPTION).addOption(EXCLUDED_HOSTS_OPTION)
        .addOption(EXCLUDED_WORKER_HOST);
  }

  @Override
  public String getCommandName() {
    return "start";
  }

  @Override
  public String getUsage() {
    return "start [-h] [--excluded-hosts <hostFilePath>] "
        + "[--excluded-worker <workerHostName>]";
  }

  @Override
  public String getDescription() {
    return "Decommission workers specified in the exclude host file. This command is "
        + "useful fsAdmin command to remove workers on our own initial.";
  }
}
