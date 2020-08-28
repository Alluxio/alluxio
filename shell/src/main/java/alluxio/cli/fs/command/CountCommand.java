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
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.FormatUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the number of folders and files matching the specified prefix in args.
 */
@ThreadSafe
@PublicApi
public final class CountCommand extends AbstractFileSystemCommand {

  private static final String READABLE_OPTION_NAME = "h";

  private static final Option READABLE_OPTION =
          Option.builder(READABLE_OPTION_NAME)
                  .required(false)
                  .hasArg(false)
                  .desc("print sizes in human readable format (e.g. 1KB 234MB 2GB)")
                  .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CountCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "count";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);

    long[] values = countHelper(inputPath);
    printInfo(cl.hasOption(READABLE_OPTION_NAME), values[0], values[1], values[2]);
    return 0;
  }

  /**
   * Prints the count messages.
   *
   * @param readable whether to print info of human readable format
   * @param fileCount the file count message to print
   * @param folderCount the folder count message to print
   * @param folderSize the folder size message to print
   */
  private void printInfo(boolean readable, long fileCount, long folderCount, long folderSize) {
    String formatFolderSize = readable ? FormatUtils.getSizeFromBytes(folderSize)
            : String.valueOf(folderSize);
    String format = "%-25s%-25s%-15s%n";
    System.out.format(format, "File Count", "Folder Count", "Folder Size");
    System.out.format(format, fileCount, folderCount, formatFolderSize);
  }

  private long[] countHelper(AlluxioURI path) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);

    if (!status.isFolder()) {
      return new long[]{ 1L, 0L, status.getLength() };
    }

    long[] rtn = new long[]{ 0L, 1L, 0L };

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
    return "count [-h] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the number of files and directories matching the specified prefix.";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(READABLE_OPTION);
  }
}
