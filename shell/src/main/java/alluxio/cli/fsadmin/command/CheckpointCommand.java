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
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

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
  public int run(CommandLine cl) throws IOException {
    Thread thread = CommonUtils.createProgressThread(System.out);
    thread.start();
    try {
      String masterHostname = mMetaClient.checkpoint();
      mPrintStream.printf("Successfully took a checkpoint on master %s%n", masterHostname);
    } finally {
      thread.interrupt();
    }
    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }
}
