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

package alluxio.cli.job.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobContext;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current leader master host name.
 */
@ThreadSafe
@PublicApi
public final class LeaderCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(LeaderCommand.class);

  /**
   * creates the job leader command.
   *
   * @param fsContext the Alluxio filesystem client
   */
  public LeaderCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "leader";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) {
    try {
      InetSocketAddress address = JobContext
          .create(mFsContext.getClusterConf(), mFsContext.getClientContext().getUserState())
          .getJobMasterAddress();
      System.out.println(address.getHostName());
    } catch (Exception e) {
      LOG.error("Failed to get the primary job master", e);
      System.out.println("Failed to get the primary job master.");
      return -1;
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "leader";
  }

  @Override
  public String getDescription() {
    return "Prints the hostname of the job master service leader.";
  }
}
