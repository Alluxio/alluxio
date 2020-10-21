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
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FormatUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
@PublicApi
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
   * @param fsContext the filesystem of Alluxio
   */
  public DuCommand(FileSystemContext fsContext) {
    super(fsContext);
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
    ListStatusPOptions listOptions = ListStatusPOptions.newBuilder().setRecursive(true).build();
    // whether to print info of human readable format
    boolean readable = cl.hasOption(READABLE_OPTION_NAME);
    // whether to display the memory size and percentage information
    boolean addMemory = cl.hasOption(MEMORY_OPTION_NAME);
    if (cl.hasOption(SUMMARIZE_OPTION_NAME)) {
      AtomicLong totalSize = new AtomicLong();
      AtomicLong sizeInAlluxio = new AtomicLong();
      AtomicLong sizeInMem = new AtomicLong();
      mFileSystem.iterateStatus(path, listOptions, status -> {
        if (!status.isFolder()) {
          long size = status.getLength();
          totalSize.addAndGet(size);
          sizeInMem.addAndGet(size * status.getInMemoryPercentage());
          sizeInAlluxio.addAndGet(size * status.getInAlluxioPercentage());
        }
      });
      String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize.get())
          : String.valueOf(totalSize);
      String inAlluxioMessage = getFormattedValues(readable, sizeInAlluxio.get() / 100,
          totalSize.get());
      String inMemMessage = addMemory
          ? getFormattedValues(readable, sizeInMem.get() / 100, totalSize.get()) : "";
      printInfo(sizeMessage, inAlluxioMessage, inMemMessage, path.toString());
    } else {
      List<URIStatus> statuses = mFileSystem.listStatus(path, listOptions);
      if (statuses == null || statuses.size() == 0) {
        return;
      }
      statuses.sort(Comparator.comparing(URIStatus::getPath));
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
  private static String getFormattedValues(boolean readable, long size, long totalSize) {
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
  private static void printInfo(String sizeMessage,
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
