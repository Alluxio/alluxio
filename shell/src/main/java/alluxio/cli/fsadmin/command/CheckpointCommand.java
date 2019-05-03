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

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Command for triggering a checkpoint in the primary master journal system.
 */
public class CheckpointCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public CheckpointCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "checkpoint";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
        @Override
        public void run() {
          mPrintStream.println("Checkpointing may take a while.");
        }
    }, Constants.MINUTE_MS);
    String masterHostname = mMetaClient.checkpoint();
    timer.cancel();
    mPrintStream.println("Successfully checkpointed in " + masterHostname);
    return 0;
  }

  @Override
  public String getUsage() {
    return "checkpoint";
  }

  @Override
  public String getDescription() {
    return "triggers a checkpoint in the primary master journal system. This command "
        + "is mainly used for debugging and to avoid master journal logs "
        + "from growing unbounded. Checkpointing requires a pause in master metadata changes, "
        + "so use this command sparingly to avoid interfering with other users of the system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }
}
