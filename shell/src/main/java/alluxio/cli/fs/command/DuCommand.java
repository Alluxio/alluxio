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
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the size of a file or a directory specified by argv.
 */
@ThreadSafe
@PublicApi
public final class DuCommand extends AbstractFileSystemCommand {
  // File Size     In Alluxio       In Memory        Worker Host Name          Path
  private static final String GROUPED_MEMORY_OPTION_FORMAT = "%-13s %-16s %-16s %-25s %s";
  // File Size     In Alluxio       In Memory        Path
  private static final String MEMORY_OPTION_FORMAT = "%-13s %-16s %-16s %s";
  // File Size     In Alluxio       Worker Host Name          Path
  private static final String GROUPED_OPTION_FORMAT = "%-13s %-16s %-25s %s";
  // File Size     In Alluxio       Path
  private static final String SHORT_INFO_FORMAT = "%-13s %-16s %s";
  private static final String VALUE_AND_PERCENT_FORMAT = "%s (%d%%)";

  private static final String MEMORY_OPTION_NAME = "m";
  private static final String MEMORY_OPTION_LONG_NAME = "memory";
  private static final String READABLE_OPTION_NAME = "h";
  private static final String SUMMARIZE_OPTION_NAME = "s";
  private static final String GROUP_BY_WORKER_OPTION_NAME = "g";

  private static final Option MEMORY_OPTION =
      Option.builder(MEMORY_OPTION_NAME)
          .longOpt(MEMORY_OPTION_LONG_NAME)
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
          .desc("display information for In-Alluxio data size under the path, grouped by worker.")
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
    printInfo("File Size", "In Alluxio", "Path",
        cl.hasOption(MEMORY_OPTION_NAME) ? Optional.of("In Memory") : Optional.empty(),
        cl.hasOption(GROUP_BY_WORKER_OPTION_NAME) ? Optional.of("Worker Host Name")
            : Optional.empty());
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    ListStatusPOptions listOptions = ListStatusPOptions.newBuilder().setRecursive(true).build();
    // whether to print info of human readable format
    boolean readable = cl.hasOption(READABLE_OPTION_NAME);
    // whether to group by worker
    boolean groupByWorker = cl.hasOption(GROUP_BY_WORKER_OPTION_NAME);
    // whether to display the memory size and percentage information
    boolean addMemory = cl.hasOption(MEMORY_OPTION_NAME);
    Optional<String> workerHostName = groupByWorker ? Optional.of("total") : Optional.empty();
    if (cl.hasOption(SUMMARIZE_OPTION_NAME)) {
      AtomicLong totalSize = new AtomicLong();
      AtomicLong sizeInAlluxio = new AtomicLong();
      AtomicLong sizeInMem = new AtomicLong();
      Map<String, Long> distributionMap = new HashMap<>();
      mFileSystem.iterateStatus(path, listOptions, status -> {
        if (!status.isFolder()) {
          long size = status.getLength();
          totalSize.addAndGet(size);
          sizeInMem.addAndGet(size * status.getInMemoryPercentage());
          sizeInAlluxio.addAndGet(size * status.getInAlluxioPercentage());
        }
        if (groupByWorker) {
          fillDistributionMap(distributionMap, status);
        }
      });
      String sizeMessage = readable ? FormatUtils.getSizeFromBytes(totalSize.get())
          : String.valueOf(totalSize);
      String inAlluxioMessage = getFormattedValues(readable, sizeInAlluxio.get() / 100,
          totalSize.get());
      Optional<String> inMemMessage = addMemory
          ? Optional.of(getFormattedValues(readable, sizeInMem.get() / 100, totalSize.get()))
              : Optional.empty();

      printInfo(sizeMessage, inAlluxioMessage, path.toString(), inMemMessage, workerHostName);

      // If workerHostName and inMemMessage is present, the "In Memory" columns
      // need an empty string as placeholders.
      // Otherwise we use an empty Optional.
      // e.g. inMemMessage is present, inMem should be ""
      // File Size     In Alluxio       In Memory        Worker Host Name          Path
      // 2             2                2                total                     /
      //               2                                 node1
      // e.g. inMemMessage is not present, inMem should be an empty Optional
      // File Size     In Alluxio       Worker Host Name          Path
      // 2             2                total                     /
      //               2                node1
      Optional<String> inMem = inMemMessage.isPresent() ? Optional.of("") : inMemMessage;
      getSizeInfoGroupByWorker(distributionMap, readable, inMem);
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
          Optional<String> inMemMessage = addMemory ? Optional.of(getFormattedValues(readable,
              status.getInMemoryPercentage() * totalSize / 100, totalSize)) : Optional.empty();

          Map<String, Long> distributionMap = new HashMap<>();
          if (groupByWorker) {
            fillDistributionMap(distributionMap, status);
          }
          Optional<String> inMem = inMemMessage.isPresent() ? Optional.of("") : inMemMessage;
          printInfo(sizeMessage, inAlluxioMessage, status.getPath(), inMemMessage, workerHostName);
          getSizeInfoGroupByWorker(distributionMap, readable, inMem);
        }
      }
    }
  }

  /**
   * Gets each block info under the url status, then accumulates block sizes
   * grouped by the worker host name, finally records info into the distribution map.
   *
   * @param distributionMap map of workers to their total block size
   * @param status whether to print info in human readable format
   */
  private static void fillDistributionMap(Map<String, Long> distributionMap, URIStatus status) {
    for (FileBlockInfo fileBlockInfo : status.getFileBlockInfos()) {
      long length = fileBlockInfo.getBlockInfo().getLength();
      for (BlockLocation blockLocation : fileBlockInfo.getBlockInfo().getLocations()) {
        distributionMap.compute(blockLocation.getWorkerAddress().getHost(),
            (hostName, totalLength) -> totalLength == null ? length : totalLength + length
        );
      }
    }
  }

  /**
   * Gets and prints the In-Alluxio size information grouped by worker in the distributionMap.
   *
   * @param distributionMap map of workers and their total block size
   * @param readable url status to be statistics
   * @param inMem in memory size information to print
   */
  private static void getSizeInfoGroupByWorker(Map<String, Long> distributionMap, boolean readable,
      Optional<String> inMem) {
    distributionMap.forEach((hostName, size) -> {
      String inAlluxioMessageThisWorker = readable ? FormatUtils.getSizeFromBytes(size)
              : String.valueOf(size);
      printInfo("", inAlluxioMessageThisWorker, "", inMem, Optional.of(hostName));
    });
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
   * @param path the path to print
   * @param inMemMessage the in memory size message to print
   * @param workerHostName the worker host name to print
   */
  private static void printInfo(String sizeMessage, String inAlluxioMessage,
      String path, Optional<String> inMemMessage, Optional<String> workerHostName) {
    String message;
    if (inMemMessage.isPresent() && workerHostName.isPresent()) {
      message = String.format(GROUPED_MEMORY_OPTION_FORMAT, sizeMessage, inAlluxioMessage,
              inMemMessage.get(), workerHostName.get(), path);
    } else if (inMemMessage.isPresent()) {
      message = String.format(MEMORY_OPTION_FORMAT, sizeMessage, inAlluxioMessage,
              inMemMessage.get(), path);
    } else if (workerHostName.isPresent()) {
      message = String.format(GROUPED_OPTION_FORMAT, sizeMessage, inAlluxioMessage,
              workerHostName.get(), path);
    } else {
      message = String.format(SHORT_INFO_FORMAT, sizeMessage, inAlluxioMessage, path);
    }
    System.out.println(message);
  }

  @Override
  public String getUsage() {
    return "du [-h|-s|-g|-m] <path>";
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
        .addOption(GROUP_BY_WORKER_OPTION)
        .addOption(MEMORY_OPTION)
        .addOption(READABLE_OPTION)
        .addOption(SUMMARIZE_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }
}
