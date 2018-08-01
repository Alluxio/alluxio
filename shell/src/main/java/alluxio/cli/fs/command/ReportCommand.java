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
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.lineage.LineageContext;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Reports to the master that a file is lost.
 */
@ThreadSafe
public final class ReportCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public ReportCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "report";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    LineageFileSystem.get(FileSystemContext.get(), LineageContext.INSTANCE).reportLostFile(path);
    System.out.println(path + " has been reported as lost.");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "report <path>";
  }

  @Override
  public String getDescription() {
    return "Reports to the master that a file is lost.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
