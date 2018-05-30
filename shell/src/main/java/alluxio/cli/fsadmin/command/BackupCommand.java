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
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.BackupResponse;

import com.google.common.base.Preconditions;
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
   */
  public BackupCommand(Context context) {
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
    Preconditions.checkState(args.length == 1);
    String dir = args[0];
    boolean local = cl.hasOption(LOCAL_OPTION.getLongOpt());
    BackupResponse resp = mMetaClient.backup(dir, local);
    if (local) {
      mPrintStream.printf("Successfully backed up journal to %s on master %s%n",
          resp.getBackupUri(), resp.getHostname());
    } else {
      mPrintStream.printf("Successfully backed up journal to %s%n", resp.getBackupUri());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "backup directoryUri";
  }

  @Override
  public String getDescription() {
    return "backup backs up all Alluxio metadata to the given directory. The directory can be any"
        + " URI that Alluxio has permissions to write to. Backing up metadata"
        + " requires a pause in master metadata changes, so use this command sparingly to"
        + " avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
