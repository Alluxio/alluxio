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

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

public class ExportJournalCommand extends AbstractFsAdminCommand {
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
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    Preconditions.checkState(args.length == 1);
    AlluxioURI uri = new AlluxioURI(args[0]);
    mMetaClient.exportJournal(uri);
    mPrintStream.printf("Successfully exported journal to %s\n", uri.toString());
    return 0;
  }

  @Override
  public String getUsage() {
    return "exportJournal uri";
  }

  @Override
  public String getDescription() {
    return "exportJournal exports the journal to the given URI. Exporting the journal"
        + " requires a pause in master metadata changes, so use journal export sparingly to"
        + " avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
