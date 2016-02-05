/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;

/**
 * Reports to the master that a file is lost.
 */
@ThreadSafe
public final class ReportCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public ReportCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "report";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    try {
      LineageFileSystem.get().reportLostFile(path);
      System.out.println(path + " has reported been reported as lost.");
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
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
