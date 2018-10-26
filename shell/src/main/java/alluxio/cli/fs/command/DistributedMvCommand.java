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
import alluxio.client.file.FileSystem;
import alluxio.client.job.JobThriftClientUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.move.MoveConfig;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Renames a file or directory specified by args. Will fail if the new path name already exists.
 */
@ThreadSafe
public final class DistributedMvCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public DistributedMvCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "distributedMv";
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
    if (mFileSystem.exists(dstPath)) {
      throw new RuntimeException(dstPath + " already exists");
    }
    Thread thread = JobThriftClientUtils.createProgressThread(2 * Constants.SECOND_MS, System.out);
    thread.start();
    try {
      JobThriftClientUtils.run(new MoveConfig(srcPath.getPath(), dstPath.getPath(), null, true), 3);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    } finally {
      thread.interrupt();
    }
    System.out.println("Moved " + srcPath + " to " + dstPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "distributedMv <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Moves a file or directory.";
  }
}
