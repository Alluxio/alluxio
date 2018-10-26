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

import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.file.FileSystem;
import alluxio.client.job.JobContext;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lists the job ids in the history.
 */
@ThreadSafe
public final class ListCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ListCommand.class);

  /**
   * Creates the job list command.
   *
   * @param fs the Alluxio filesystem client
   */
  public ListCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "ls";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    try (CloseableResource<JobMasterClient> client =
        JobContext.INSTANCE.acquireMasterClientResource()) {
      List<Long> ids = client.get().list();
      for (long id : ids) {
        System.out.println(id);
      }
    } catch (Exception e) {
      LOG.error("Failed to list the jobs ", e);
      System.out.println("Failed to list the jobs");
      return -1;
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "ls";
  }

  @Override
  public String getDescription() {
    return "Prints the IDs of the most recent jobs, running and finished,"
        + " in the history up to the capacity set in alluxio.job.master.job.capacity";
  }
}
