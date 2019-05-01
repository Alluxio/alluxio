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
import alluxio.grpc.SnapshotPResponse;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Command for triggering a snapshot in the primary master journal system.
 */
public class SnapshotCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public SnapshotCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "snapshot";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    SnapshotPResponse response = mMetaClient.snapshot();
    if (response.hasSucceed() && response.getSucceed()) {
      mPrintStream.println("Successfully take a snapshot in the primary master journal system");
    } else if (response.hasTriggered() && !response.getTriggered()) {
      mPrintStream.printf("Failed to trigger a snapshot: "
          + response.getMessage());
    } else { // Triggered but not succeed
      mPrintStream.printf("Failed to finish snapshot with exception: %n" + response.getMessage());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "snapshot";
  }

  @Override
  public String getDescription() {
    return "triggers a snapshot in the primary master journal system. This command is mainly used "
        + "for debugging and to avoid master journal logs from growing unbounded. Snapshotting "
        + "requires a pause in master metadata changes, so use this command sparingly to "
        + "avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }
}
