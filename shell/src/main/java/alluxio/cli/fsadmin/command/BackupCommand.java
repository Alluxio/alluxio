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

package alluxio.cli.fsadmin.command;

import alluxio.Constants;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPOptions.Builder;
import alluxio.util.CommonUtils;
import alluxio.wire.BackupResponse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Command for backing up Alluxio master metadata.
 */
public class BackupCommand extends AbstractFsAdminCommand {

  private static final Option UFS_OPTION =
      Option.builder()
          .longOpt("ufs")
          .required(false)
          .hasArg(false)
          .desc("whether to write the backup to the root UFS "
              + "instead of the local filesystem of leader master")
          .build();
  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public BackupCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "backup";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(UFS_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    Builder opts = BackupPOptions.newBuilder();
    if (args.length >= 1) {
      opts.setTargetDirectory(args[0]);
    }
    opts.setUseRootUnderFileSystem(cl.hasOption(UFS_OPTION.getLongOpt()));
    // Create progress thread for showing progress while backup is in progress.
    Thread progressThread = CommonUtils.createProgressThread(5 * Constants.SECOND_MS, System.out);
    progressThread.start();
    BackupResponse resp;
    try {
      resp = mMetaClient.backup(opts.build());
    } finally {
      progressThread.interrupt();
    }
    if (opts.getUseRootUnderFileSystem()) {
      mPrintStream.printf("Successfully backed up journal to %s with %d entries%n",
          resp.getBackupUri(), resp.getEntryCount());
    } else {
      mPrintStream.printf("Successfully backed up journal to %s on master %s with %d entries%n",
          resp.getBackupUri(), resp.getHostname(), resp.getEntryCount());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "backup [directory] [--ufs]";
  }

  @Override
  public String getDescription() {
    return "backup backs up all Alluxio metadata to the backup directory configured on master. The"
        + " directory to back up to can be overridden by specifying a directory here."
        + " By default, backup backs up to the local disk of the primary master,"
        + " use --ufs to backup to a directory path relative to the UFS. Backing up metadata"
        + " requires a pause in master metadata changes, so use this command sparingly to"
        + " avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
