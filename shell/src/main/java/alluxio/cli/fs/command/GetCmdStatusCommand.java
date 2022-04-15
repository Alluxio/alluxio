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

package alluxio.cli.fs.command;

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.wire.CmdStatusBlock;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Get Command status specified by args.
 */
@ThreadSafe
@PublicApi
public class GetCmdStatusCommand extends AbstractDistributedJobCommand {
  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public GetCmdStatusCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "getCmdStatus";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "getCmdStatus <jobControlId>";
  }

  @Override
  public String getDescription() {
    return "Get the status information for a distributed command.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    long jobControlId = Long.parseLong(args[0]);

    CmdStatusBlock cmdStatus = mClient.getCmdStatusDetailed(jobControlId);
    if (cmdStatus.getJobStatusBlock().isEmpty()) {
      System.out.format("Unable to get command status for jobControlId=%s, please retry"
          + " or use `fs ls` command to check if files are already loaded in Alluxio.%n",
              jobControlId);
    } else {
      System.out.format("Get command status information below: %n");
      cmdStatus.getJobStatusBlock().forEach(block -> {
        System.out.format("Job Id = %s, Status = %s, filePath = %s %n",
                block.getJobId(), block.getStatus(), block.getFilePath());
      });
    }
    return 0;
  }
}
