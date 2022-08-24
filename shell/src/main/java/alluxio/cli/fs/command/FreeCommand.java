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
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerInfoField;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerRange;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.FreeMode;
import alluxio.grpc.FreePOptions;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Frees the given file or folder from Alluxio storage (recursively freeing all children if a
 * folder).
 */
@ThreadSafe
@PublicApi
public final class FreeCommand extends AbstractFileSystemCommand {

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("force to free files even pinned")
          .build();

  private static final Option EXCLUDE_WORKER_LIST_OPTION =
      Option.builder()
          .longOpt("exclude")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .valueSeparator(',')
          .desc("Specifies a list of worker hosts separated by comma "
              + "which shouldn’t free target data.")
          .build();

  private static final Option INCLUDE_WORKER_LIST_OPTION =
      Option.builder()
          .longOpt("include")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .valueSeparator(',')
          .desc("Specifies a list of worker hosts separated by comma to free target data.")
          .build();

  /**
   * Constructs a new instance to free the given file or folder from Alluxio.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public FreeCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(FORCE_OPTION)
        .addOption(EXCLUDE_WORKER_LIST_OPTION)
        .addOption(INCLUDE_WORKER_LIST_OPTION);
  }

  @Override
  public String getCommandName() {
    return "free";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    int interval =
        Math.toIntExact(mFsContext.getPathConf(path)
            .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS));
    List<Long> workerIds = new ArrayList<>();
    FreeMode freeMode = FreeMode.ALL;
    if (cl.hasOption(INCLUDE_WORKER_LIST_OPTION.getLongOpt())) {
      freeMode = FreeMode.INCLUDE;
      workerIds = parseWorkerIdList(cl, INCLUDE_WORKER_LIST_OPTION.getLongOpt());
    } else if (cl.hasOption(EXCLUDE_WORKER_LIST_OPTION.getLongOpt())) {
      freeMode = FreeMode.EXCLUDE;
      workerIds = parseWorkerIdList(cl, EXCLUDE_WORKER_LIST_OPTION.getLongOpt());
    }
    FreePOptions options = FreePOptions.newBuilder()
        .setRecursive(true)
        .setForced(cl.hasOption("f"))
        .setFreeMode(freeMode)
        .addAllWorkerIds(workerIds)
        .build();
    mFileSystem.free(path, options);
    // TODO(qian0817): need to check if free mode is not all
    if (freeMode == FreeMode.ALL) {
      try {
        CommonUtils.waitFor("file to be freed. Another user may be loading it.", () -> {
          try {
            URIStatus fileStatus = mFileSystem.getStatus(path);
            if (fileStatus.getLength() == 0 && !fileStatus.isFolder()) {
              // `getInAlluxioPercentage()` will always return 100,
              // but 'free' on an empty file should be a no-op
              return true;
            }
            if (fileStatus.getInAlluxioPercentage() > 0) {
              mFileSystem.free(path, options);
              return fileStatus.getInAlluxioPercentage() == 0;
            } else {
              return true;
            }
          } catch (Exception e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
          }
        }, WaitForOptions.defaults().setTimeoutMs(10 * Math.toIntExact(interval))
            .setInterval(interval));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println(path + " was successfully freed from Alluxio space.");
  }

  private List<Long> parseWorkerIdList(CommandLine cl, String option) throws IOException {
    String[] workerHostnames = StringUtils.split(
        cl.getOptionValue(option), ",");
    List<Long> workerIds = new ArrayList<>();
    try (BlockMasterClient blockMasterClient =
             BlockMasterClient.Factory.create(mFsContext.getMasterClientContext())) {
      GetWorkerReportOptions options = GetWorkerReportOptions.defaults()
          .setWorkerRange(WorkerRange.ALL)
          .setFieldRange(Sets.newHashSet(WorkerInfoField.ADDRESS, WorkerInfoField.ID));
      List<WorkerInfo> workerReport = blockMasterClient.getWorkerReport(options);
      Map<String, List<WorkerInfo>> workerAddressMap = workerReport
          .stream()
          .collect(Collectors.groupingBy(
              worker -> worker.getAddress().getHost() + ":" + worker.getAddress().getRpcPort()));
      for (String workerHostname : workerHostnames) {
        List<WorkerInfo> workerList = workerAddressMap.get(workerHostname);
        if (workerList == null) {
          continue;
        }
        for (WorkerInfo workerInfo : workerList) {
          workerIds.add(workerInfo.getId());
        }
      }
    }
    return workerIds;
  }

  @Override
  public String getUsage() {
    return "free [-f] [--include <address1>,<address2>...<addressN>] "
        + "[--exclude <address1>,<address2>...<addressN>] <path>";
  }

  @Override
  public String getDescription() {
    return "Frees the space occupied by a file or a directory in Alluxio."
        + " Specify -f to force freeing pinned files in the directory."
        + " Use --exclude or --include to specify worker to free.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
