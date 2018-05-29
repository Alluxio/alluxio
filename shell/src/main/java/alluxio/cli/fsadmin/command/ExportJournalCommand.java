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
import alluxio.wire.ExportJournalResponse;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

/**
 * Command for exporting a backup of the Alluxio master journal.
 */
public class ExportJournalCommand extends AbstractFsAdminCommand {

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
  public ExportJournalCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "exportJournal";
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
    ExportJournalResponse resp = mMetaClient.exportJournal(dir, local);
    String locationMessage = local ? " on master " + resp.getHostname() : "";
    mPrintStream.printf("Successfully exported journal to %s%s%n", resp.getBackupUri(),
        locationMessage);
    return 0;
  }

  @Override
  public String getUsage() {
    return "exportJournal directoryUri";
  }

  @Override
  public String getDescription() {
    return "exportJournal exports the journal to the given directory. The directory can be any URI"
        + " that Alluxio has permissions to write to. Exporting the journal"
        + " requires a pause in master metadata changes, so use journal export sparingly to"
        + " avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
