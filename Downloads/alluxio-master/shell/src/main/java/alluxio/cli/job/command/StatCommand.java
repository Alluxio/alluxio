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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.TaskInfo;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the status of the job.
 */
@ThreadSafe
public final class StatCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(StatCommand.class);
  private static final Option VERBOSE_OPTION =
      Option.builder("v")
          .required(false)
          .hasArg(false)
          .desc("show the status of every task")
          .build();

  /**
   * Creates the job stat command.
   *
   * @param fs the Alluxio filesystem client
   */
  public StatCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "stat";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(VERBOSE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    long id = Long.parseLong(cl.getArgs()[0]);
    try (CloseableResource<JobMasterClient> client =
        JobContext.INSTANCE.acquireMasterClientResource()) {
      JobInfo info = client.get().getStatus(id);
      System.out.print(formatOutput(cl, info));
    } catch (Exception e) {
      LOG.error("Failed to get status of the job", e);
      System.out.println("Failed to get status of the job " + id);
      return -1;
    }
    return 0;
  }

  private String formatOutput(CommandLine cl, JobInfo info) {
    StringBuilder output = new StringBuilder();
    output.append("ID: ").append(info.getJobId()).append("\n");
    if (info.getJobConfig() != null) {
      output.append("Config: ").append(info.getJobConfig()).append("\n");
    }
    output.append("Status: ").append(info.getStatus()).append("\n");
    if (info.getErrorMessage() != null && !info.getErrorMessage().isEmpty()) {
      output.append("Error: ").append(info.getErrorMessage()).append("\n");
    }
    if (info.getResult() != null && !info.getResult().isEmpty()) {
      output.append("Result: ").append(info.getResult()).append("\n");
    }

    if (cl.hasOption("v")) {
      for (TaskInfo taskInfo : info.getTaskInfoList()) {
        output.append("Task ").append(taskInfo.getTaskId()).append("\n");
        output.append("\t").append("Status: ").append(taskInfo.getStatus()).append("\n");
        if (taskInfo.getErrorMessage() != null && !taskInfo.getErrorMessage().isEmpty()) {
          output.append("\t").append("Error: ").append(taskInfo.getErrorMessage()).append("\n");
        }
        if (taskInfo.getResult() != null) {
          output.append("\t").append("Result: ").append(taskInfo.getResult()).append("\n");
        }
      }
    }
    return output.toString();
  }

  @Override
  public String getUsage() {
    return "stat [-v] <id>";
  }

  @Override
  public String getDescription() {
    return "Displays the status info for the specific job. Use -v flag to display the status of "
        + "every task";
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
}
