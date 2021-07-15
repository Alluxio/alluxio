/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.ReadType;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.util.BlockLocationUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.CacheResponse;
import alluxio.grpc.OpenFilePOptions;
import alluxio.master.block.BlockId;
import alluxio.network.TieredIdentityFactory;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
              "When local option is specified," + " there must be a local worker available");
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
    OpenFilePOptions options = OpenFilePOptions.newBuilder().build();
    AlluxioConfiguration conf = mFsContext.getPathConf(filePath);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        PreconditionMessage.UFS_READ_LOCATION_POLICY_UNSPECIFIED);

    WorkerNetAddress worker;
    List<Long> blockIds = status.getBlockIds();
    for (long blockId : blockIds) {
      if (local) {
        worker = mFsContext.getNodeLocalWorker();
      } else { // send request to data source
        worker = getDataSouce(blockId, status, policy);
      }
      tasks.add(new LoadTask(blockId, worker, local, status, options));
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
    private final OpenFilePOptions mOptions;

    LoadTask(long blockId, WorkerNetAddress dataSource, boolean local, URIStatus status,
        OpenFilePOptions options) {
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
          .setOpenUfsBlockOptions(getOpenUfsBlockOptions(mBlockId, mStatus, mOptions))
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

  // todo(jianjian) consider refactoring with AlluxioBlockStore
  private WorkerNetAddress getDataSouce(long blockId, URIStatus status, BlockLocationPolicy policy)
      throws IOException {
    BlockInfo info = status.getBlockInfo(blockId);
    List<BlockLocation> locations = info.getLocations();
    List<BlockWorkerInfo> blockWorkerInfo = Collections.EMPTY_LIST;
    TieredIdentity tieredIdentity =
        TieredIdentityFactory.localIdentity(mFsContext.getClusterConf());
    // Initial target workers to read the block given the block locations.
    Set<WorkerNetAddress> workers;
    // Note that, it is possible that the blocks have been written as UFS blocks
    if (status.isPersisted() || status.getPersistenceState().equals("TO_BE_PERSISTED")) {
      blockWorkerInfo = mFsContext.getCachedWorkers();
      if (blockWorkerInfo.isEmpty()) {
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      workers = blockWorkerInfo.stream().map(BlockWorkerInfo::getNetAddress).collect(toSet());
    } else {
      if (locations.isEmpty()) {
        blockWorkerInfo = mFsContext.getCachedWorkers();
        if (blockWorkerInfo.isEmpty()) {
          throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
        }
        throw new UnavailableException(
            ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(info.getBlockId()));
      }
      workers = locations.stream().map(BlockLocation::getWorkerAddress).collect(toSet());
    }
    // Workers to read the block, after considering failed workers.
    WorkerNetAddress dataSource = null;
    locations = locations.stream().filter(location -> workers.contains(location.getWorkerAddress()))
        .collect(toList());
    // First try to read data from Alluxio
    if (!locations.isEmpty()) {
      // TODO(calvin): Get location via a policy
      List<WorkerNetAddress> tieredLocations =
          locations.stream().map(location -> location.getWorkerAddress()).collect(toList());
      Collections.shuffle(tieredLocations);
      Optional<Pair<WorkerNetAddress, Boolean>> nearest =
          BlockLocationUtils.nearest(tieredIdentity, tieredLocations, mFsContext.getClusterConf());
      if (nearest.isPresent()) {
        dataSource = nearest.get().getFirst();
      }
    }

    // Can't get data from Alluxio, get it from the UFS instead
    if (dataSource == null) {
      blockWorkerInfo = blockWorkerInfo.stream()
          .filter(workerInfo -> workers.contains(workerInfo.getNetAddress())).collect(toList());
      GetWorkerOptions getWorkerOptions = GetWorkerOptions
          .defaults().setBlockInfo(new BlockInfo().setBlockId(info.getBlockId())
              .setLength(info.getLength()).setLocations(locations))
          .setBlockWorkerInfos(blockWorkerInfo);
      dataSource = policy.getWorker(getWorkerOptions);
    }
    if (dataSource == null) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    return dataSource;
  }

  // todo(jianjian) refactoring with InStreamOptions
  private Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions(long blockId, URIStatus status,
      OpenFilePOptions options) {
    Preconditions.checkArgument(status.getBlockIds().contains(blockId), "blockId");
    boolean readFromUfs = status.isPersisted();
    // In case it is possible to fallback to read UFS blocks, also fill in the options.
    boolean storedAsUfsBlock = status.getPersistenceState().equals("TO_BE_PERSISTED");
    readFromUfs = readFromUfs || storedAsUfsBlock;
    if (!readFromUfs) {
      return Protocol.OpenUfsBlockOptions.getDefaultInstance();
    }
    BlockInfo info = status.getBlockInfo(blockId);
    long blockStart = BlockId.getSequenceNumber(blockId) * status.getBlockSizeBytes();
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = Protocol.OpenUfsBlockOptions.newBuilder()
        .setUfsPath(status.getUfsPath()).setOffsetInFile(blockStart).setBlockSize(info.getLength())
        .setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency())
        .setNoCache(!ReadType.fromProto(options.getReadType()).isCache())
        .setMountId(status.getMountId()).build();
    if (storedAsUfsBlock) {
      // On client-side, we do not have enough mount information to fill in the UFS file path.
      // Instead, we unset the ufsPath field and fill in a flag ufsBlock to indicate the UFS file
      // path can be derived from mount id and the block ID. Also because the entire file is only
      // one block, we set the offset in file to be zero.
      openUfsBlockOptions = openUfsBlockOptions.toBuilder().clearUfsPath().setBlockInUfsTier(true)
          .setOffsetInFile(0).build();
    }
    return openUfsBlockOptions;
  }
}

