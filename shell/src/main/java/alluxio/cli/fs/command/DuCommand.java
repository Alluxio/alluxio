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
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FormatUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
public final class DuCommand extends AbstractFileSystemCommand {
  private static final String LONG_INFO_FORMAT = "%-13s %-16s %-16s %s";
  private static final String SHORT_INFO_FORMAT = "%-13s %-16s %s";
  private static final String VALUE_AND_PERCENT_FORMAT = "%s (%d%%)";

  private static final String MEMORY_OPTION_NAME = "memory";
  private static final String READABLE_OPTION_NAME = "h";
  private static final String SUMMARIZE_OPTION_NAME = "s";

  private static final Option MEMORY_OPTION =
      Option.builder()
          .longOpt(MEMORY_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("display the in memory size and in memory percentage")
          .build();

  private static final Option READABLE_OPTION =
      Option.builder(READABLE_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("print sizes in human readable format (e.g., 1KB 234MB 2GB)")
          .build();

  private static final Option SUMMARIZE_OPTION =
      Option.builder(SUMMARIZE_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("display the aggregate summary of file lengths being displayed")
          .build();

  /**
   * @param fs the filesystem of Alluxio
   */
  public DuCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "du";
  }

  @Override
  protected void processHeader(CommandLine cl) {
    printInfo("File Size", "In Alluxio",
        cl.hasOption(MEMORY_OPTION_NAME) ? "In Memory" : "", "Path");
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {

    ListStatusPOptions listOptions =
        FileSystemClientOptions.getListStatusOptions().toBuilder().setRecursive(true).build();
    List<URIStatus> statuses = mFileSystem.listStatus(path, listOptions);
    if (statuses == null || statuses.size() == 0) {
      return;
    }

    getSizeInfo(path, statuses, cl.hasOption(READABLE_OPTION_NAME),
        cl.hasOption(SUMMARIZE_OPTION_NAME), cl.hasOption(MEMORY_OPTION_NAME));
  }

  /**
   * Gets and prints the size information of the input path according to options.
   *
   * @param path the path to get size info of
   * @param statuses the statuses of files and folders
   * @param readable whether to print info of human readable format
   * @param summarize whether to display the aggregate summary lengths
   * @param addMemory whether to display the memory size and percentage information
   */
  private void getSizeInfo(AlluxioURI path, List<URIStatus> statuses,
      boolean readable, boolean summarize, boolean addMemory) {
    if (summarize) {
      long totalSize = 0;
      long sizeInAlluxio = 0;
      long sizeInMem = 0;
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long size = status.getLength();
          totalSize += size;
          sizeInMem += size * status.getInMemoryPercentage();
          sizeInAlluxio += size * status.getInMemoryPercentage();
        }
      }
      String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize)
          : String.valueOf(totalSize);
      String inAlluxioMessage = getFormattedValues(readable, sizeInAlluxio / 100, totalSize);
      String inMemMessage = addMemory
          ? getFormattedValues(readable, sizeInMem / 100, totalSize) : "";
      printInfo(sizeMessage, inAlluxioMessage, inMemMessage, path.toString());
    } else {
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long totalSize = status.getLength();
          String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize)
              : String.valueOf(totalSize);
          String inAlluxioMessage = getFormattedValues(readable,
              status.getInAlluxioPercentage() * totalSize / 100, totalSize);
          String inMemMessage = addMemory ? getFormattedValues(readable,
              status.getInMemoryPercentage() * totalSize / 100, totalSize) : "";
          printInfo(sizeMessage, inAlluxioMessage, inMemMessage, status.getPath());
        }
      }
    }
  }

  /**
   * Gets the size and its percentage information, if readable option is provided,
   * get the size in human readable format.
   *
   * @param readable whether to print info of human readable format
   * @param size the size to get information from
   * @param totalSize the total size to calculate percentage information
   * @return the formatted value and percentage information
   */
  private String getFormattedValues(boolean readable, long size, long totalSize) {
    // If size is 1, total size is 5, and readable is true, it will
    // return a string as "1B (20%)"
    int percent = totalSize == 0 ? 0 : (int) (size * 100 / totalSize);
    String subSizeMessage = readable ? FormatUtils.getSizeFromBytes(size)
        : String.valueOf(size);
    return String.format(VALUE_AND_PERCENT_FORMAT, subSizeMessage, percent);
  }

  /**
   * Prints the size messages.
   *
   * @param sizeMessage the total size message to print
   * @param inAlluxioMessage the in Alluxio size message to print
   * @param inMemMessage the in memory size message to print
   * @param path the path to print
   */
  private void printInfo(String sizeMessage,
      String inAlluxioMessage, String inMemMessage, String path) {
    System.out.println(inMemMessage.isEmpty()
        ? String.format(SHORT_INFO_FORMAT, sizeMessage, inAlluxioMessage, path)
        : String.format(LONG_INFO_FORMAT, sizeMessage, inAlluxioMessage, inMemMessage, path));
  }

  @Override
  public String getUsage() {
    return "du [-h|-s|--memory] <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the total size and the in Alluxio size of the specified file or directory.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(MEMORY_OPTION)
        .addOption(READABLE_OPTION).addOption(SUMMARIZE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }
}
