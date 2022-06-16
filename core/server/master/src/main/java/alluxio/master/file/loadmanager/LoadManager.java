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

package alluxio.master.file.loadmanager;

import static alluxio.master.file.loadmanager.load.LoadInfo.LoadOptions;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.job.meta.JobIdGenerator;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.loadmanager.load.LoadInfo;
import alluxio.master.file.loadmanager.load.BlockBatch;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The Load manager which controls load operations.
 */
public final class LoadManager {
  private static final Logger LOG = LoggerFactory.getLogger(LoadManager.class);
  private final FileSystemMaster mFileSystemMaster;
  private final FileSystemContext mContext;
  private final Map<String, LoadInfo> mLoadPathToInfo = Maps.newHashMap();
  private final JobIdGenerator mJobIdGenerator = new JobIdGenerator();
  private final Map<Long, Load> mLoads = Maps.newHashMap();
  private final Scheduler mScheduler;

  /**
   * Constructor.
   * @param fileSystemMaster fileSystemMaster
   * @param context fileSystemContext
   */
  public LoadManager(FileSystemMaster fileSystemMaster, FileSystemContext context) {
    mFileSystemMaster = fileSystemMaster;
    mContext = context;
    mScheduler = new Scheduler(mFileSystemMaster, mContext);
  }

  /**
   * Validate the load information.
   * @param loadInfo load information
   * @return boolean value on whether the load is validated for scheduling or not
   */
  public boolean validate(LoadInfo loadInfo) {
    LoadOptions options = loadInfo.getLoadOptions();
    String path = loadInfo.getPath();
    if (mLoadPathToInfo.containsKey(path)) {
      boolean isNewOption = mLoadPathToInfo.get(path).getLoadOptions().equals(options);
      if (isNewOption) {
        LoadInfo info = mLoadPathToInfo.get(path);
        long loadId = info.getId();
        Load load = mLoads.get(loadId);
        load.updateOptions(options);
        /*Only update bandwidth here.*/
        long newBandWidth = options.getBandwidth();
        info.getLoadOptions().setBandwidth(newBandWidth);
      }
      return false;
    } else {
      return true;
    }
  }

  /**
   * Schedule a load to run.
   * @param loadInfo load meta information
   * @throws ResourceExhaustedException throw ResourceExhaustedException
   * @throws InterruptedException throw InterruptedException
   */
  public void schedule(LoadInfo loadInfo)
          throws ResourceExhaustedException, InterruptedException {
    long loadId = loadInfo.getId();
    Load load = new Load(loadInfo.getId(),
            loadInfo.getPath(), loadInfo.getLoadOptions());
    mScheduler.schedule(load);
    mLoads.put(loadId, load);
  }

  static class Scheduler {
    private static final int CAPACITY = 100;
    private static final long TIMEOUT = 100;
    private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Load> mLoadQueue = new LinkedBlockingQueue<>(CAPACITY);
    private final AtomicInteger mCurrentSize = new AtomicInteger(); // scheduler capacity indicator
    private final AtomicLong mIdGenerator = new AtomicLong();
    private final FileSystemMaster mFileSystemMaster;
    private final FileSystemContext mFileSystemContext;

    public Scheduler(FileSystemMaster fileSystemMaster, FileSystemContext fileSystemContext) {
      mFileSystemMaster = fileSystemMaster;
      mFileSystemContext = fileSystemContext;
    }

    void start() {
      mExecutorService.submit(this::runLoad);
    }

    void schedule(Load load)
            throws InterruptedException, ResourceExhaustedException {
      if (mCurrentSize.get() >= CAPACITY) {
        throw new ResourceExhaustedException(
                "Insufficient capacity to enqueue load tasks!");
      }

      boolean offered = mLoadQueue.offer(load, TIMEOUT, TimeUnit.MILLISECONDS);
      if (offered) {
        mCurrentSize.incrementAndGet();
      } else {
        throw new AlluxioRuntimeException(
                "Cannot enqueue load to the queue to schedule, please retry");
      }
    }

    void runLoad() {
      while (!Thread.interrupted()) {
        try {
          Load load = mLoadQueue.take();
          BlockBuffer blockBuffer = new BlockBuffer(load.getPath());
          List<WorkerNetAddress> addresses = getWorkerAddresses();

          addresses.forEach(address -> {
            try (CloseableResource<BlockWorkerClient> blockWorker
                         = mFileSystemContext.acquireBlockWorkerClient(address)) {
              loadBlockBatch(blockWorker.get(), blockBuffer, load.getBandWidth());
            } catch (IOException e) {
              // handle IOException
            }
          });
          mCurrentSize.decrementAndGet();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private void loadBlockBatch(BlockWorkerClient client,
                                BlockBuffer blockBuffer,
                                long bandWidth) {
      BlockBatch blockBatch = blockBuffer.getNextBatchBlocks();
      if (blockBatch.getBlockIds().isEmpty()) {
        return;
      }

      LoadRequest loadRequest = LoadRequest
              .newBuilder()
              .addFileBlocks(blockBatch.toProto())
              .setBandwidth(bandWidth)
              .build();
      ListenableFuture<LoadResponse> listenableFuture = client.load(loadRequest);
      Futures.addCallback(listenableFuture, new FutureCallback<LoadResponse>() {
        @Override
        public void onSuccess(LoadResponse r) {
          if (r.hasStatus()) {
            TaskStatus s = r.getStatus();
            if (s == TaskStatus.PARTIAL_FAILURE) {
              List<BlockStatus> blockStatusList = r.getBlockStatusList();
              Stream<Long> retryableBlocks =
                      blockStatusList.stream()
                              .filter(BlockStatus::getRetryable)
                              .map(BlockStatus::getBlockId);

              Stream<Long> failedBlocks =
                      blockStatusList.stream()
                              .filter(blockStatus -> !blockStatus.getRetryable())
                              .map(BlockStatus::getBlockId);
              blockBuffer.addToRetry(retryableBlocks.collect(Collectors.toList()));
              blockBuffer.addFailedBlocks(failedBlocks.collect(Collectors.toList()));
            } else if (s == TaskStatus.FAILURE) {
              //
            }
          } else {
            blockBuffer.addFailedBlocks(blockBatch.getBlockIds());
          }
          loadBlockBatch(client, blockBuffer, bandWidth);
        }

        @Override
        public void onFailure(Throwable t) {
        }
      }, MoreExecutors.directExecutor());
    }

    private long getNextBatchId() {
      return mIdGenerator.incrementAndGet();
    }

    private List<WorkerNetAddress> getWorkerAddresses() {
      try {
        return mFileSystemMaster.getWorkerInfoList().stream()
                .map(WorkerInfo::getAddress).collect(Collectors.toList());
      } catch (UnavailableException e) {
        return Collections.EMPTY_LIST;
      }
    }
  }

  static class Load {
    private final long mLoadId;
    private final String mPath;
    private final LoadOptions mOptions;

    public Load(long loadId, String path, LoadOptions options) {
      mLoadId = loadId;
      mPath = path;
      mOptions = options;
    }

    public long getLoadId() {
      return mLoadId;
    }

    public long getBandWidth() {
      return mOptions.getBandwidth();
    }

    /*
     * Only update bandwidth.
     */
    public void updateOptions(LoadOptions options) {
      mOptions.setBandwidth(options.getBandwidth());
    }

    public String getPath() {
      return mPath;
    }

    public String getDetailedInfo() {
      return "";
    }
  }

  static class BlockBuffer {
    /**
     * Constructor to create a BlockBuffer.
     * @param filePath file path
     */
    public BlockBuffer(String filePath) {
    }

    public BlockBatch getNextBatchBlocks() throws AlluxioRuntimeException {
      return null;
    }

    public boolean addToRetry(List<Long> blockId) {
      return true;
    }

    public boolean addFailedBlocks(List<Long> blockId) {
      return true;
    }
  }
}
