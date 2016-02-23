/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the number of folders and files matching the specified prefix in args.
 */
@ThreadSafe
public final class CountCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public CountCommand(Configuration conf, FileSystem fs) {
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
    AlluxioURI inputPath = new AlluxioURI(args[0]);

    long[] values = countHelper(inputPath);
    String format = "%-25s%-25s%-15s%n";
    System.out.format(format, "File Count", "Folder Count", "Total Bytes");
    System.out.format(format, values[0], values[1], values[2]);
  }

  private long[] countHelper(AlluxioURI path) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    if (!status.isFolder()) {
      return new long[] { 1L, 0L, status.getLength() };
    }

    long[] rtn = new long[] { 0L, 1L, 0L };

    List<URIStatus> statuses;
    try {
      statuses = mFileSystem.listStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
    for (URIStatus uriStatus : statuses) {
      long[] toAdd = countHelper(new AlluxioURI(uriStatus.getPath()));
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
