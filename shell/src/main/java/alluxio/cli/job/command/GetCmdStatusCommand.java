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
import alluxio.cli.util.DistributedCommandUtil;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobContext;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.resource.CloseableResource;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Get Command status specified by args.
 */
@ThreadSafe
@PublicApi
public class GetCmdStatusCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(GetCmdStatusCommand.class);

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
    Set<String> failedFiles = Sets.newHashSet();
    List<String> completedFiles = Lists.newArrayList();

    try (CloseableResource<JobMasterClient> client =
                 JobContext.create(mFsContext.getClusterConf(),
                                 mFsContext.getClientContext().getUserState())
                         .acquireMasterClientResource()) {
      DistributedCommandUtil.getDetailedCmdStatus(
              jobControlId, client.get(), failedFiles, completedFiles);
      if (!failedFiles.isEmpty()) {
        System.out.println("Failed files are:");
        failedFiles.forEach(System.out::println);
      }
    } catch (Exception e) {
      LOG.error("Failed to get detailed status of the command", e);
      System.out.println(String.format("Unable to get detailed information for command %s."
                      + " Please retry using `getCmdStatus` to check command detailed status,",
              jobControlId));
      return -1;
    }
    return 0;
  }
}
