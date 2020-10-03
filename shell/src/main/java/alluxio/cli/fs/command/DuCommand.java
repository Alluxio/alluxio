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
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FormatUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
@PublicApi
public final class DuCommand extends AbstractFileSystemCommand {
  private static final String LONG_INFO_FORMAT = "%-13s %-16s %-16s %-25s %s";
  private static final String MID_1_INFO_FORMAT = "%-13s %-16s %-16s %s";
  private static final String MID_2_INFO_FORMAT = "%-13s %-16s %-25s %s";
  private static final String SHORT_INFO_FORMAT = "%-13s %-16s %s";
  private static final String VALUE_AND_PERCENT_FORMAT = "%s (%d%%)";

  private static final String MEMORY_OPTION_NAME = "memory";
  private static final String READABLE_OPTION_NAME = "h";
  private static final String SUMMARIZE_OPTION_NAME = "s";
  private static final String GROUP_BY_WORKER_OPTION_NAME = "g";

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

  private static final Option GROUP_BY_WORKER_OPTION =
          Option.builder(GROUP_BY_WORKER_OPTION_NAME)
                  .required(false)
                  .hasArg(false)
                  .desc("displays information for In-Alluxio data size under the path, "
                          + "grouped by worker.")
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
        cl.hasOption(MEMORY_OPTION_NAME) ? Optional.of("In Memory") : Optional.empty(),
            "Path", cl.hasOption(GROUP_BY_WORKER_OPTION_NAME) ? Optional.of("Worker Host Name")
                    : Optional.empty());
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {

    ListStatusPOptions listOptions = ListStatusPOptions.newBuilder().setRecursive(true).build();
    List<URIStatus> statuses = mFileSystem.listStatus(path, listOptions);
    if (statuses == null || statuses.size() == 0) {
      return;
    }

    statuses.sort(Comparator.comparing(URIStatus::getPath));
    getSizeInfo(path, statuses, cl.hasOption(READABLE_OPTION_NAME),
        cl.hasOption(SUMMARIZE_OPTION_NAME), cl.hasOption(GROUP_BY_WORKER_OPTION_NAME),
        cl.hasOption(MEMORY_OPTION_NAME));
  }

  /**
   * Gets and prints the size information of the input path according to options.
   *
   * @param path the path to get size info of
   * @param statuses the statuses of files and folders
   * @param readable whether to print info of human readable format
   * @param summarize whether to display the aggregate summary lengths
   * @param groupByWorker whether to display In-Alluxio groupByWorker
   * @param addMemory whether to display the memory size and percentage information
   */
  protected static void getSizeInfo(AlluxioURI path, List<URIStatus> statuses,
      boolean readable, boolean summarize, boolean groupByWorker, boolean addMemory) {
    Optional<String> workerHostName = groupByWorker ? Optional.of("total") : Optional.empty();
    if (summarize) {
      long totalSize = 0;
      long sizeInAlluxio = 0;
      long sizeInMem = 0;
      Map<String, Long> distributionMap = new HashMap<>();
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long size = status.getLength();
          totalSize += size;
          sizeInMem += size * status.getInMemoryPercentage();
          sizeInAlluxio += size * status.getInAlluxioPercentage();
        }
        if (groupByWorker) {
          for (FileBlockInfo fileBlockInfo : status.getFileBlockInfos()) {
            long length = fileBlockInfo.getBlockInfo().getLength();
            for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
              distributionMap.put(blockLocation.getWorkerAddress().getHost(),
                      distributionMap.getOrDefault(
                              blockLocation.getWorkerAddress().getHost(), 0L) + length);
            }
          }
        }
      }
      String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize)
          : String.valueOf(totalSize);
      String inAlluxioMessage = getFormattedValues(readable, sizeInAlluxio / 100, totalSize);
      Optional<String> inMemMessage = addMemory
          ? Optional.of(getFormattedValues(readable, sizeInMem / 100, totalSize))
              : Optional.empty();

      printInfo(sizeMessage, inAlluxioMessage, inMemMessage, path.toString(), workerHostName);

      Optional<String> inMem = inMemMessage.isPresent() ? Optional.of("") : inMemMessage;
      distributionMap.forEach((hostName, size) -> {
        String inAlluxioMessageThisWorker = readable ? FormatUtils.getSizeFromBytes(size)
                : String.valueOf(size);
        printInfo("", inAlluxioMessageThisWorker, inMem, "", Optional.of(hostName));
      });
    } else {
      for (URIStatus status : statuses) {
        if (!status.isFolder()) {
          long totalSize = status.getLength();
          String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize)
              : String.valueOf(totalSize);
          String inAlluxioMessage = getFormattedValues(readable,
              status.getInAlluxioPercentage() * totalSize / 100, totalSize);
          Optional<String> inMemMessage = addMemory ? Optional.of(getFormattedValues(readable,
              status.getInMemoryPercentage() * totalSize / 100, totalSize)) : Optional.empty();

          Map<String, Long> distributionMap = new HashMap<>();
          if (groupByWorker) {
            for (FileBlockInfo fileBlockInfo : status.getFileBlockInfos()) {
              long length = fileBlockInfo.getBlockInfo().getLength();
              for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
                distributionMap.put(blockLocation.getWorkerAddress().getHost(),
                        distributionMap.getOrDefault(
                                blockLocation.getWorkerAddress().getHost(), 0L) + length);
              }
            }
          }
          Optional<String> inMem = inMemMessage.isPresent() ? Optional.of("") : inMemMessage;
          printInfo(sizeMessage, inAlluxioMessage, inMemMessage, status.getPath(), workerHostName);
          distributionMap.forEach((hostName, size) -> {
            String inAlluxioMessageThisWorker = readable ? FormatUtils.getSizeFromBytes(size)
                    : String.valueOf(size);
            printInfo("", inAlluxioMessageThisWorker, inMem, "", Optional.of(hostName));
          });
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
   * @param workerHostName the worker host name to print
   */
  private static void printInfo(String sizeMessage, String inAlluxioMessage,
      Optional<String> inMemMessage, String path, Optional<String> workerHostName) {
    String message;
    if (inMemMessage.isPresent() && workerHostName.isPresent()) {
      message = String.format(LONG_INFO_FORMAT, sizeMessage, inAlluxioMessage,
              inMemMessage.get(), workerHostName.get(), path);
    } else if (inMemMessage.isPresent()) {
      message = String.format(MID_1_INFO_FORMAT, sizeMessage, inAlluxioMessage,
              inMemMessage.get(), path);
    } else if (workerHostName.isPresent()) {
      message = String.format(MID_2_INFO_FORMAT, sizeMessage, inAlluxioMessage,
              workerHostName.get(), path);
    } else {
      message = String.format(SHORT_INFO_FORMAT, sizeMessage, inAlluxioMessage, path);
    }
    System.out.println(message);
  }

  @Override
  public String getUsage() {
    return "du [-h|-s|-g|--memory] <path>";
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
    return new Options()
        .addOption(MEMORY_OPTION)
        .addOption(READABLE_OPTION)
        .addOption(SUMMARIZE_OPTION)
        .addOption(GROUP_BY_WORKER_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }
}
