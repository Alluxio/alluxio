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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.lineage.LineageContext;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Reports to the master that a file is lost.
 */
@ThreadSafe
public final class ReportCommand extends WithWildCardPathCommand {

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
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    LineageFileSystem.get(FileSystemContext.INSTANCE, LineageContext.INSTANCE).reportLostFile(path);
    System.out.println(path + " has been reported as lost.");
  }

  @Override
  public String getUsage() {
    return "report <path>";
  }

  @Override
  public String getDescription() {
    return "Reports to the master that a file is lost.";
  }
}
