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
import alluxio.client.job.JobMasterClient;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Cancels a job asynchronously.
 */
@ThreadSafe
@PublicApi
public class CancelCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CancelCommand.class);

  /**
   * creates the cancel command.
   *
   * @param fsContext the Alluxio filesystem client
   */
  public CancelCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "cancel";
  }

  @Override
  public String getUsage() {
    return "cancel <id>";
  }

  @Override
  public String getDescription() {
    return "Cancels a job asynchronously.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
    String arg = cl.getArgs()[0];
    try {
      Long.parseLong(arg);
    } catch (Exception e) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_ARG_TYPE.getMessage(arg, "long"));
    }
  }

  @Override
  public int run(CommandLine cl) {
    long id = Long.parseLong(cl.getArgs()[0]);
    try (CloseableResource<JobMasterClient> client =
         JobContext.create(mFsContext.getClusterConf(),
             mFsContext.getClientContext().getUserState()).acquireMasterClientResource()) {
      client.get().cancel(id);
    } catch (Exception e) {
      LOG.error("Failed to cancel the job", e);
      System.out.println("Failed to cancel the job " + id);
      return -1;
    }
    return 0;
  }
}
