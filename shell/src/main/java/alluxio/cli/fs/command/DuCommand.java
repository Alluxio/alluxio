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
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.ListStatusOptions;
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
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
public final class DuCommand extends AbstractFileSystemCommand {
  private static final String LONG_INFO_FORMAT = "%-13s %-16s %-16s %s";
  private static final String SHORT_INFO_FORMAT = "%-13s %-16s %s";
  private static final String VALUE_AND_PERCENT_FORMAT = "%s (%d%%)";

  private static final String HEADER_OPTION_NAME = "header";
  private static final String MEMORY_OPTION_NAME = "memory";
  private static final String READABLE_OPTION_NAME = "h";
  private static final String SUMMARIZE_OPTION_NAME = "s";

  private static final Option HEADER_OPTION =
      Option.builder()
          .longOpt(HEADER_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("display the header")
          .build();

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
          .desc("print sizes in human readable format (e.g., 1k 234M 2G)")
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
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {

    ListStatusOptions listOptions = ListStatusOptions.defaults().setRecursive(true);
    List<URIStatus> statuses = mFileSystem.listStatus(path, listOptions);
    if (statuses == null || statuses.size() == 0) {
      return;
    }

    if (cl.hasOption(HEADER_OPTION_NAME)) {
      printInfo("File Size", "In Alluxio",
          cl.hasOption(MEMORY_OPTION_NAME) ? "In Memory" : "", "Path");
    }

    getSizeInfo(path, statuses, cl.hasOption(READABLE_OPTION_NAME),
        cl.hasOption(SUMMARIZE_OPTION_NAME), cl.hasOption(MEMORY_OPTION_NAME));
  }

  /**
   * Gets and prints the size information of the input path according to options.
   *
   * @param path the path to get size info of
   * @param statuses the statuses of files and folders
   * @param readable whether to print info of human-readable format
   * @param summarize whether to display the aggregate summary lengths
   * @param addMemory whether to display the memory information
   */
  private void getSizeInfo(AlluxioURI path, List<URIStatus> statuses,
      boolean readable, boolean summarize, boolean addMemory) {
    if (summarize) {
      long sizeInBytes = 0;
      long sizeInAlluxio = 0;
      long sizeInMem = 0;
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long size = status.getLength();
          sizeInBytes += size;
          sizeInMem += size * status.getInMemoryPercentage() / 100;
          sizeInAlluxio += size * status.getInMemoryPercentage() / 100;
        }
      }
      String sizeMessage = readable ? FormatUtils.getSizeFromBytes(sizeInBytes)
          : String.valueOf(sizeInBytes);
      String inAlluxioMessage = getFormattedValues(readable, sizeInAlluxio, sizeInBytes);
      String inMemMessage = addMemory ? getFormattedValues(readable, sizeInMem, sizeInBytes) : "";
      printInfo(sizeMessage, inAlluxioMessage, inMemMessage, path.toString());
    } else {
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long sizeInBytes = status.getLength();
          String sizeMessage = readable ? FormatUtils.getSizeFromBytes(sizeInBytes)
              : String.valueOf(sizeInBytes);
          String inAlluxioMessage = getFormattedValues(readable,
              status.getInAlluxioPercentage() * sizeInBytes / 100, sizeInBytes);
          String inMemMessage = addMemory ? getFormattedValues(readable,
              status.getInMemoryPercentage() * sizeInBytes / 100, sizeInBytes) : "";
          printInfo(sizeMessage, inAlluxioMessage, inMemMessage, status.getPath());
        }
      }
    }
  }

  /**
   * Formats the size information to string message.
   *
   * @param readable whether to print info of human-readable format
   * @param subSize the sub size to calculate percentage information
   * @param totalSize the total size to calculate percentage information
   * @return the formatted value and percentage information
   */
  private String getFormattedValues(boolean readable, long subSize, long totalSize) {
    int percent = totalSize == 0 ? 0 : (int) (subSize * 100 / totalSize);
    String subSizeMessage = readable ? FormatUtils.getSizeFromBytes(subSize)
        : String.valueOf(subSize);
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
    System.out.println(inMemMessage.equals("")
        ? String.format(SHORT_INFO_FORMAT, sizeMessage, inAlluxioMessage, path)
        : String.format(LONG_INFO_FORMAT, sizeMessage, inAlluxioMessage, inMemMessage, path));
  }

  @Override
  public String getUsage() {
    return "du [-h|-s|--header|--memory] <path>";
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
    return new Options().addOption(HEADER_OPTION).addOption(MEMORY_OPTION)
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
