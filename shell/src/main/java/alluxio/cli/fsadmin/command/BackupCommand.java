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

import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPOptions.Builder;
import alluxio.wire.BackupResponse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Command for backing up Alluxio master metadata.
 */
public class BackupCommand extends AbstractFsAdminCommand {

  private static final Option LOCAL_OPTION =
      Option.builder()
          .longOpt("local")
          .required(false)
          .hasArg(false)
          .desc("whether to write the backup to the master's local filesystem instead of the root"
              + " UFS")
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
    return new Options().addOption(LOCAL_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    Builder opts = BackupPOptions.newBuilder();
    if (args.length >= 1) {
      opts.setTargetDirectory(args[0]);
    }
    opts.setLocalFileSystem(cl.hasOption(LOCAL_OPTION.getLongOpt()));
    BackupResponse resp = mMetaClient.backup(opts.build());
    if (opts.getLocalFileSystem()) {
      mPrintStream.printf("Successfully backed up journal to %s on master %s%n",
          resp.getBackupUri(), resp.getHostname());
    } else {
      mPrintStream.printf("Successfully backed up journal to %s%n", resp.getBackupUri());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "backup [directory] [--local]";
  }

  @Override
  public String getDescription() {
    return "backup backs up all Alluxio metadata to the backup directory configured on master. The"
        + " directory to back up to can be overridden by specifying a directory here. The directory"
        + " path is relative to the root UFS. To write the backup to the local disk of the primary"
        + " master, use --local and specify a filesystem path. Backing up metadata"
        + " requires a pause in master metadata changes, so use this command sparingly to"
        + " avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }
}
