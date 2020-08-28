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

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.BackupPRequest;
import alluxio.grpc.BackupState;
import alluxio.wire.BackupStatus;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.UUID;

/**
 * Command for backing up Alluxio master metadata.
 */
@PublicApi
public class BackupCommand extends AbstractFsAdminCommand {

  private static final Option LOCAL_OPTION =
      Option.builder()
          .longOpt("local")
          .required(false)
          .hasArg(false)
          .desc("whether to write the backup to the master's local filesystem"
              + " instead of the root UFS")
          .build();
  private static final Option ALLOW_LEADER_OPTION =
      Option.builder()
          .longOpt("allow-leader")
          .required(false)
          .hasArg(false)
          .desc("whether to allow leader to take the backup when"
              + " HA cluster has no stand-by master.")
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
    return new Options()
        .addOption(LOCAL_OPTION)
        .addOption(ALLOW_LEADER_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    BackupPRequest.Builder opts = BackupPRequest.newBuilder();
    if (args.length >= 1) {
      opts.setTargetDirectory(args[0]);
    }
    opts.setOptions(
        BackupPOptions.newBuilder()
            .setRunAsync(true)
            .setLocalFileSystem(cl.hasOption(LOCAL_OPTION.getLongOpt()))
            .setAllowLeader(cl.hasOption(ALLOW_LEADER_OPTION.getLongOpt())));
    // Take backup in async mode.
    BackupStatus status = mMetaClient.backup(opts.build());
    UUID backupId = status.getBackupId();
    do {
      clearProgressLine();
      // Backup could be after a fail-over.
      if (status.getState() == BackupState.None) {
        mPrintStream.printf("Backup lost. Please check Alluxio logs.%n");
        return -1;
      }
      if (status.getState() == BackupState.Completed) {
        break;
      } else if (status.getState() == BackupState.Failed) {
        throw AlluxioStatusException.fromAlluxioException(status.getError());
      } else {
        // Generate progress line that will be replaced until backup is finished.
        String progressMessage = String.format(" Backup state: %s", status.getState());
        // Start showing entry count once backup started running.
        if (status.getState() == BackupState.Running) {
          progressMessage += String.format(" | Entries processed: %d", status.getEntryCount());
        }
        mPrintStream.write(progressMessage.getBytes());
        mPrintStream.write("\r".getBytes());
        mPrintStream.flush();
      }
      // Sleep half a sec before querying status again.
      try {
        Thread.sleep(500);
        status = mMetaClient.getBackupStatus(backupId);
      } catch (InterruptedException ie) {
        throw new RuntimeException("Interrupted while waiting for backup completion.");
      } finally {
        // In case exception is thrown.
        clearProgressLine();
      }
    } while (true);
    clearProgressLine();
    // Print final state.
    mPrintStream.printf("Backup Host        : %s%n", status.getHostname());
    mPrintStream.printf("Backup URI         : %s%n", status.getBackupUri());
    mPrintStream.printf("Backup Entry Count : %d%n", status.getEntryCount());
    return 0;
  }

  private void clearProgressLine() throws IOException {
    final int progressLineLength = 75;
    // Clear progress line.
    mPrintStream.write(StringUtils.repeat(" ", progressLineLength).getBytes());
    mPrintStream.write("\r".getBytes());
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
        + " will be delegated to stand-by masters in HA cluster. Use --allow-leader for"
        + " leader to take the backup when there are no stand-by masters.(This will pause"
        + " metadata changes during the backup.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 2);
  }
}
