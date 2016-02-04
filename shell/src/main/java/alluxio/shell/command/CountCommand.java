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
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;

import alluxio.TachyonURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.TachyonConf;
import alluxio.exception.TachyonException;

/**
 * Displays the number of folders and files matching the specified prefix in args.
 */
@ThreadSafe
public final class CountCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param fs the filesystem of Tachyon
   */
  public CountCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "count";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    TachyonURI inputPath = new TachyonURI(args[0]);

    long[] values = countHelper(inputPath);
    String format = "%-25s%-25s%-15s%n";
    System.out.format(format, "File Count", "Folder Count", "Total Bytes");
    System.out.format(format, values[0], values[1], values[2]);
  }

  private long[] countHelper(TachyonURI path) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }

    if (!status.isFolder()) {
      return new long[] { 1L, 0L, status.getLength() };
    }

    long[] rtn = new long[] { 0L, 1L, 0L };

    List<URIStatus> statuses;
    try {
      statuses = mFileSystem.listStatus(path);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    for (URIStatus uriStatus : statuses) {
      long[] toAdd = countHelper(new TachyonURI(uriStatus.getPath()));
      rtn[0] += toAdd[0];
      rtn[1] += toAdd[1];
      rtn[2] += toAdd[2];
    }
    return rtn;
  }

  @Override
  public String getUsage() {
    return "count <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the number of files and directories matching the specified prefix.";
  }
}
