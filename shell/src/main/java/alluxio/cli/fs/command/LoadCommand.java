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
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.block.AlluxioBlockStore;
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
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.CacheResponse;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptions;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in Alluxio.
 */
@ThreadSafe
@PublicApi
public final class LoadCommand extends AbstractFileSystemCommand {
  // todo(jianjian) add option for timeout and parallelism(compatibility?)
  private static final int DEFAULT_PARALLELISM = 4;

  private static final Option LOCAL_OPTION = Option.builder().longOpt("local").required(false)
      .hasArg(false).desc("load the file to local worker.").build();

  private static final long DEFAULT_TIMEOUT = 20 * Constants.MINUTE_MS;

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
    return new Options().addOption(LOCAL_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    ExecutorService service = Executors.newFixedThreadPool(DEFAULT_PARALLELISM,
        ThreadFactoryUtils.build("load-cli-%d", true));

    List<LoadTask> tasks = new ArrayList<>();

    load(plainPath, cl.hasOption(LOCAL_OPTION.getLongOpt()), tasks);
    try {
      List<Future<Boolean>> futures =
          service.invokeAll(tasks, DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
      for (Future<Boolean> future : futures) {
        if (!future.isCancelled()) {
          try {
            boolean result = future.get();
            // TODO(jianjian): post process failure blocks
          } catch (ExecutionException e) {
            System.out.println("Fatal error: " + e);
          }
        }
      }
    } catch (InterruptedException e) {
      System.out.println("Load interrupted, exiting.");
    } finally {
      service.shutdownNow();
    }
    System.out.println(plainPath + " loaded");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in Alluxio.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio
   * @param local whether to load data to local worker even when the data is already loaded remotely
   */
  private void load(AlluxioURI filePath, boolean local, List<LoadTask> tasks)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);

    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath, local, tasks);
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

      queueLoadTasks(filePath, status, local, tasks);
    }
  }

  private void queueLoadTasks(AlluxioURI filePath, URIStatus status, boolean local,
      List<LoadTask> tasks) throws IOException {
    AlluxioConfiguration conf = mFsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptions.openFileDefaults(conf);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);

    WorkerNetAddress dataSource;
    List<Long> blockIds = status.getBlockIds();
    for (long blockId : blockIds) {
      if (local) {
        dataSource = mFsContext.getNodeLocalWorker();
      } else { // send request to data source
        AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);

        Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> dataSourceAndType =
            blockStore.getDataSourceAndType(status.getBlockInfo(blockId), status, policy);
        dataSource = dataSourceAndType.getFirst();
      }
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          new InStreamOptions(status, options, conf).getOpenUfsBlockOptions(blockId);
      tasks.add(new LoadTask(blockId, dataSource, local, status, openUfsBlockOptions));
    }
  }

  @Override
  public String getUsage() {
    return "load [--local] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in Alluxio.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  private class LoadTask implements Callable<Boolean> {
    private final long mBlockId;
    private final WorkerNetAddress mDataSource;
    private final boolean mLocal;
    private final URIStatus mStatus;
    private final Protocol.OpenUfsBlockOptions mOptions;

    LoadTask(long blockId, WorkerNetAddress dataSource, boolean local, URIStatus status,
        Protocol.OpenUfsBlockOptions options) {
      mBlockId = blockId;
      mDataSource = dataSource;
      mLocal = local;
      mStatus = status;
      mOptions = options;
    }

    @Override
    public Boolean call() throws Exception {
      BlockInfo info = mStatus.getBlockInfo(mBlockId);
      long blockLength = info.getLength();
      CacheRequest request = CacheRequest.newBuilder().setBlockId(mBlockId).setLength(blockLength)
          .setOpenUfsBlockOptions(mOptions)
          .setSourceHost(mDataSource.getHost()).setSourcePort(mDataSource.getDataPort()).build();
      // TODO(jianjian) support FUSE processLocalWorker?
      WorkerNetAddress worker;
      try {
        if (mLocal) {
          // send request to local worker
          worker = mFsContext.getNodeLocalWorker();
        } else { // send request to data source
          worker = mDataSource;
        }
        try (CloseableResource<BlockWorkerClient> blockWorker =
            mFsContext.acquireBlockWorkerClient(worker)) {
          CacheResponse response = blockWorker.get().cache(request);
          return response.getSuccess();
        }
      } catch (Exception e) {
        System.out.printf("Failed to complete cache request for block %d of file %s: %s", mBlockId,
            mStatus.getPath(), e.toString());
      }
      return false;
    }
  }
}