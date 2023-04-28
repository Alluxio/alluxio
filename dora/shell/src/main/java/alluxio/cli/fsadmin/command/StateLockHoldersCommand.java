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

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * CI Command to get state lock holder thread identifiers.
 */
public class StateLockHoldersCommand extends AbstractFsAdminCommand {

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public StateLockHoldersCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "statelock";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    for (String stateLockHolder : mFsClient.getStateLockHolders()) {
      System.out.println(stateLockHolder);
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "statelock";
  }

  @Override
  public String getDescription() {
    return "statelock returns all waiters and holders of the state lock.";
  }
}
