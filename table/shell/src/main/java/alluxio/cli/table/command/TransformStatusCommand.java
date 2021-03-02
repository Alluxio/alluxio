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

package alluxio.cli.table.command;

import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.table.TransformJobInfo;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Check transformation status.
 */
@ThreadSafe
public final class TransformStatusCommand extends AbstractTableCommand {
  private static final Logger LOG = LoggerFactory.getLogger(TransformStatusCommand.class);

  private static final String COMMAND_NAME = "transformStatus";

  /**
   * creates the command.
   *
   * @param conf alluxio configuration
   * @param client the table master client used to make RPCs
   * @param fsContext the filesystem of Alluxio
   */
  public TransformStatusCommand(AlluxioConfiguration conf, TableMasterClient client,
      FileSystemContext fsContext) {
    super(conf, client, fsContext);
  }

  @Override
  public String getCommandName() {
    return COMMAND_NAME;
  }

  @Override
  public String getUsage() {
    return COMMAND_NAME + " [<job ID>]";
  }

  @Override
  public String getDescription() {
    return "Check status of transformations.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }

  private String toString(TransformJobInfo info) {
    StringBuilder sb = new StringBuilder();
    sb.append("database: ").append(info.getDbName()).append("\n");
    sb.append("table: ").append(info.getTableName()).append("\n");
    sb.append("transformation: ").append(info.getDefinition()).append("\n");
    sb.append("job ID: ").append(info.getJobId()).append("\n");
    sb.append("job status: ").append(info.getJobStatus()).append("\n");
    if (!info.getJobError().isEmpty()) {
      sb.append("job error: ").append(info.getJobError());
    }
    return sb.toString();
  }

  @Override
  public int run(CommandLine cl) throws IOException, AlluxioException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      for (TransformJobInfo info : mClient.getAllTransformJobInfo()) {
        System.out.println(toString(info));
        System.out.println();
      }
    } else {
      long jobId = Long.parseLong(args[0]);
      TransformJobInfo info = mClient.getTransformJobInfo(jobId);
      System.out.println(toString(info));
    }
    return 0;
  }
}
