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
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.LoadProgressReportFormat;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in Alluxio.
 */
@ThreadSafe
@PublicApi
public final class LoadCommand extends AbstractFileSystemCommand {
  private static final Option LOCAL_OPTION =
      Option.builder()
          .longOpt("local")
          .required(false)
          .hasArg(false)
          .desc("load the file to local worker.")
          .build();
  private static final Option SUBMIT_OPTION = Option.builder()
      .longOpt("submit")
      .required(false)
      .hasArg(false)
      .desc("Submit load job to Alluxio master, update job options if already exists.")
      .build();

  private static final Option STOP_OPTION = Option.builder()
      .longOpt("stop")
      .required(false)
      .hasArg(false)
      .desc("Stop a load job if it's still running.")
      .build();

  private static final Option PROGRESS_OPTION = Option.builder()
      .longOpt("progress")
      .required(false)
      .hasArg(false)
      .desc("Get progress report of a load job.")
      .build();

  private static final Option PARTIAL_LISTING_OPTION = Option.builder()
      .longOpt("partial-listing")
      .required(false)
      .hasArg(false)
      .desc("Use partial directory listing. This limits the memory usage "
          + "and starts load sooner for larger directory. But progress "
          + "report cannot report on the total number of files because the "
          + "whole directory is not listed yet.")
      .build();

  private static final Option VERIFY_OPTION = Option.builder()
      .longOpt("verify")
      .required(false)
      .hasArg(false)
      .desc("Run verification when load finish and load new files if any.")
      .build();

  private static final Option BANDWIDTH_OPTION = Option.builder()
      .longOpt("bandwidth")
      .required(false)
      .hasArg(true)
      .desc("Run verification when load finish and load new files if any.")
      .build();

  private static final Option PROGRESS_FORMAT = Option.builder()
      .longOpt("format")
      .required(false)
      .hasArg(true)
      .desc("Format of the progress report, supports TEXT and JSON. If not "
          + "set, TEXT is used.")
      .build();

  private static final Option PROGRESS_VERBOSE = Option.builder()
      .longOpt("verbose")
      .required(false)
      .hasArg(false)
      .desc("Whether to return a verbose progress report with detailed errors")
      .build();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LoadCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(BANDWIDTH_OPTION)
        .addOption(PARTIAL_LISTING_OPTION)
        .addOption(VERIFY_OPTION)
        .addOption(SUBMIT_OPTION)
        .addOption(STOP_OPTION)
        .addOption(PROGRESS_OPTION)
        .addOption(PROGRESS_FORMAT)
        .addOption(PROGRESS_VERBOSE)
        .addOption(LOCAL_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    if (isOldFormat(cl)) {
      runWildCardCmd(path, cl);
      return 0;
    }

    if (path.containsWildcard()) {
      throw new UnsupportedOperationException("Load does not support wildcard path");
    }

    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      OptionalLong bandwidth = OptionalLong.empty();
      if (cl.hasOption(BANDWIDTH_OPTION.getLongOpt())) {
        bandwidth = OptionalLong.of(FormatUtils.parseSpaceSize(
            cl.getOptionValue(BANDWIDTH_OPTION.getLongOpt())));
      }
      return submitLoad(
          path,
          bandwidth,
          cl.hasOption(PARTIAL_LISTING_OPTION.getLongOpt()),
          cl.hasOption(VERIFY_OPTION.getLongOpt()));
    }

    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      return stopLoad(path);
    }

    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      Optional<LoadProgressReportFormat> format = Optional.empty();
      if (cl.hasOption(PROGRESS_FORMAT.getLongOpt())) {
        format = Optional.of(LoadProgressReportFormat.valueOf(
            cl.getOptionValue(PROGRESS_FORMAT.getLongOpt())));
      }
      return getProgress(path, format, cl.hasOption(PROGRESS_VERBOSE.getLongOpt()));
    }

    return 0;
  }

  @Override
  public String getUsage() {
    return "For backward compatibility: load [--local] <path>\n"
        + "For distributed load:\n"
        + "\tload <path> --submit [--bandwidth N] [--verify] [--partial-listing]\n"
        + "\tload <path> --stop\n"
        + "\tload <path> --progress [--format TEXT|JSON] [--verbose]\n";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in Alluxio.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
    if (!isOldFormat(cl)) {
      int commands = 0;
      if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
        commands++;
      }
      if (cl.hasOption(STOP_OPTION.getLongOpt())) {
        commands++;
      }
      if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
        commands++;
      }
      if (commands != 1) {
        throw new InvalidArgumentException("Must have one of submit / stop / progress");
      }
    }
  }

  private int submitLoad(AlluxioURI path, OptionalLong bandwidth,
      boolean usePartialListing, boolean verify) {
    try {
      if (mFileSystem.submitLoad(path, bandwidth, usePartialListing, verify)) {
        System.out.printf("Load '%s' is successfully submitted.%n", path);
      } else {
        System.out.printf("Load already running for path '%s', updated the job with "
                + "new bandwidth: %s, verify: %s%n",
            path,
            bandwidth.isPresent() ? String.valueOf(bandwidth.getAsLong()) : "unlimited",
            verify);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println("Failed to submit load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  private int stopLoad(AlluxioURI path) {
    try {
      if (mFileSystem.stopLoad(path)) {
        System.out.printf("Load '%s' is successfully stopped.%n", path);
      }
      else {
        System.out.printf("Cannot find load job for path %s, it might have already been "
            + "stopped or finished%n", path);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println("Failed to stop load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  private int getProgress(AlluxioURI path, Optional<LoadProgressReportFormat> format,
      boolean verbose) {
    try {
      System.out.println("Progress for loading path '" + path + "':");
      System.out.println(mFileSystem.getLoadProgress(path, format, verbose));
      return 0;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        System.out.println("Load for path '" + path + "' cannot be found.");
        return -2;
      }
      System.out.println("Failed to get progress for load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  private boolean isOldFormat(CommandLine cl) {
    return cl.getOptions().length == 0
        || (cl.getOptions().length == 1 && cl.hasOption(LOCAL_OPTION.getLongOpt()));
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    Preconditions.checkState(
        isOldFormat(cl),
        "The new load command should not hit this code path");
    oldLoad(plainPath, cl.hasOption(LOCAL_OPTION.getLongOpt()));
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in Alluxio.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio
   * @param local whether to load data to local worker even when the data is already loaded remotely
   */
  private void oldLoad(AlluxioURI filePath, boolean local)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        oldLoad(newPath, local);
      }
    } else {
      if (local) {
        if (!mFsContext.hasNodeLocalWorker()) {
          System.out.println(
              "When local option is specified, there must be a local worker available");
          return;
        }
      } else if (status.getInAlluxioPercentage() == 100) {
        // The file has already been fully loaded into Alluxio.
        System.out.println(filePath + " already in Alluxio fully");
        return;
      }
      runLoadTask(filePath, status, local);
    }
    System.out.println(filePath + " loaded");
  }

  private void runLoadTask(AlluxioURI filePath, URIStatus status, boolean local)
      throws IOException {
    AlluxioConfiguration conf = mFsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptionsUtils.openFileDefaults(conf);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.getClass(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        "UFS read location policy Required when loading files");
    WorkerNetAddress dataSource;
    List<Long> blockIds = status.getBlockIds();
    for (long blockId : blockIds) {
      if (local) {
        dataSource = mFsContext.getNodeLocalWorker();
      } else { // send request to data source
        BlockStoreClient blockStore = BlockStoreClient.create(mFsContext);
        Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> dataSourceAndType = blockStore
            .getDataSourceAndType(status.getBlockInfo(blockId), status, policy, ImmutableMap.of());
        dataSource = dataSourceAndType.getFirst();
      }
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          new InStreamOptions(status, options, conf, mFsContext).getOpenUfsBlockOptions(blockId);
      BlockInfo info = status.getBlockInfo(blockId);
      long blockLength = info.getLength();
      String host = dataSource.getHost();
      // issues#11172: If the worker is in a container, use the container hostname
      // to establish the connection.
      if (!dataSource.getContainerHost().equals("")) {
        host = dataSource.getContainerHost();
      }
      CacheRequest request = CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
          .setOpenUfsBlockOptions(openUfsBlockOptions).setSourceHost(host)
          .setSourcePort(dataSource.getDataPort()).build();
      try (CloseableResource<BlockWorkerClient> blockWorker =
          mFsContext.acquireBlockWorkerClient(dataSource)) {
        blockWorker.get().cache(request);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to complete cache request from %s for "
            + "block %d of file %s: %s", dataSource, blockId, status.getPath(), e), e);
      }
    }
  }
}
