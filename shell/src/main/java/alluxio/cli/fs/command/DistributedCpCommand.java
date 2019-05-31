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

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobGrpcClientUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.migrate.MigrateConfig;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or directory specified by args.
 */
@ThreadSafe
public final class DistributedCpCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedCpCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedCp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    Thread thread = CommonUtils.createProgressThread(2 * Constants.SECOND_MS, System.out);
    thread.start();
    try {
      JobGrpcClientUtils.run(new MigrateConfig(srcPath.getPath(), dstPath.getPath(), null, true,
          false), 3, mFsContext.getPathConf(dstPath));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    } finally {
      thread.interrupt();
    }
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "distributedCp <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or directory in parallel at file level.";
  }
}
