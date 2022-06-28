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

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.job.meta.JobIdGenerator;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.loadmanager.load.LoadInfo;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
    private static final int MAX_RETRY_COUNT = 5;
    private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Load> mLoadQueue = new LinkedBlockingQueue<>(CAPACITY);
    /*scheduler capacity indicator, number of current running and non-running load tasks*/
    private final AtomicInteger mCurrentSize = new AtomicInteger();
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

      mLoadQueue.put(load);
      mCurrentSize.incrementAndGet();
    }

    void runLoad() {
      RetryPolicy retryPolicy = new CountingRetry(MAX_RETRY_COUNT);

      while (!Thread.interrupted()) {
        try {
          Load load = mLoadQueue.take();
          List<FileInfo> listFileInfos = listFileInfos(load);
          if (listFileInfos == null || listFileInfos.isEmpty()) {
            LOG.warn(String.format("Get an empty file info list, skip this task %s",
                    load.mLoadId));
            mCurrentSize.decrementAndGet();
            continue;
          }

          BlockBuffer blockBuffer = BlockBuffer.create(listFileInfos);
          List<WorkerNetAddress> addresses = getWorkerAddresses();

          addresses.forEach(address -> {
            while (retryPolicy.attempt()) {
              try (CloseableResource<BlockWorkerClient> blockWorker
                           = mFileSystemContext.acquireBlockWorkerClient(address)) {
                // register worker and set status
                load.setWorkerLoadFinished(address, false);
                loadBlockBatch(blockWorker.get(), address, blockBuffer, load);
              } catch (IOException e) {
                LOG.warn(String.format("Cannot acquire worker client for %s" + ","
                        + " after %s tries", address.getHost(), retryPolicy.getAttemptCount()));

                if (retryPolicy.getAttemptCount() == MAX_RETRY_COUNT) {
                  addresses.remove(address);
                  LOG.warn("Remove this worker from the current"
                          + " worker list after reaching maximum retry count = " + MAX_RETRY_COUNT);
                }
              }
            }
          });

          /*If no workers can be acquired, fail the entire load task immediately.*/
          if (addresses.isEmpty()) {
            load.addUfsLoadStatus(load.getPath(), TaskStatus.FAILURE);
          }

          mCurrentSize.decrementAndGet();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    void loadBlockBatch(BlockWorkerClient client,
                                WorkerNetAddress address,
                                BlockBuffer blockBuffer,
                                Load load) {

      List<Block> blockBatch = blockBuffer.hasNext() ? blockBuffer.getNextFileBlockBatch() : null;
      if (blockBatch == null) {
        load.setWorkerLoadFinished(address, true);
        return;
      }

      LoadRequest loadRequest = buildRequest(load, blockBatch);
      ListenableFuture<LoadResponse> listenableFuture = client.load(loadRequest);
      Futures.addCallback(listenableFuture, new FutureCallback<LoadResponse>() {
        @Override
        public void onSuccess(LoadResponse r) {
          TaskStatus s = r.getStatus();

          /*If successful, just schedule a next run on loadBlockBatch;
           only handles for TaskStatus not successful.*/
          if (s != TaskStatus.SUCCESS) {
            List<BlockStatus> blockStatusList = r.getBlockStatusList();
            blockStatusList.forEach(blockStatus -> {
              Block block = blockStatus.getBlock();
              if (blockStatus.getCode() != Status.Code.OK.value()) {
                if (blockStatus.getRetryable()) {
                  blockBuffer.addToRetry(block);
                } else { //if blocks failed, handle this condition
                  blockBuffer.addFailedBlock(block);
                  load.addFailedBlock(block.getBlockId(), blockStatus.getMessage());
                  /*The corresponding ufs must be FAILURE status*/
                  load.addUfsLoadStatus(block.getUfsPath(), TaskStatus.FAILURE);
                }
              }
            });
          }
          loadBlockBatch(client, address, blockBuffer, load);
        }

        @Override
        public void onFailure(Throwable t) {
          /* Handle failure for workers to execute and get a response,
           will be retried by other workers.*/
          load.setWorkerLoadFinished(address, true);
          LOG.error("Failure for workers to execute and get a response for worker "
                  + address.getHost());
          LOG.error(t.getMessage());
        }
      }, MoreExecutors.directExecutor());
    }

    public static LoadRequest buildRequest(Load load, List<Block> blockBatch) {
      return LoadRequest
              .newBuilder()
              .setBandwidth(load.getBandWidth())
              .addAllBlocks(blockBatch)
              .build();
    }

    protected List<FileInfo> listFileInfos(Load load) {
      ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
      String path = load.getPath();
      try {
        return mFileSystemMaster.listStatus(new AlluxioURI(path),
                ListStatusContext.create(options.toBuilder()));
      } catch (AccessControlException e) {
        LOG.error("Invalid access to file path " + path + ", " + e.getMessage());
        load.addListFilePathError(e.getMessage());
      } catch (FileDoesNotExistException | InvalidPathException e) {
        LOG.error("Invalid path or file does not exist for " + path);
        load.addListFilePathError(e.getMessage() + ", " + e.getMessage());
      } catch (Exception e) {
        LOG.error("Exception when listing file infos: " + path);
        load.addListFilePathError(e.getMessage() + ", " + e.getMessage());
      }
      return null;
    }

    protected List<WorkerNetAddress> getWorkerAddresses() {
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

    private final Map<String, TaskStatus> mUfsLoadTaskStatusMap = Maps.newHashMap();
    private final Map<Long, Set<String>> mFailedBlockMap = Maps.newHashMap();
    private final Set<String> mListFilePathErrors = Sets.newHashSet();
    /*Used to check worker registration and whether the load by the worker is finished or not.*/
    private final Map<WorkerNetAddress, Boolean> mWorkerLoadFinished = Maps.newHashMap();
    private final Lock mLoadLock = new ReentrantLock();

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

    /**
     * Add list file path errors.
     * @param error error msg
     */
    public void addListFilePathError(String error) {
      mListFilePathErrors.add(error);
    }

    /**
     * Add task status for usf path.
     * @param ufs ufs path
     * @param status task status
     */
    public void addUfsLoadStatus(String ufs, TaskStatus status) {
      try (LockResource lr = new LockResource(mLoadLock)) {
        mUfsLoadTaskStatusMap.put(ufs, status);
      }
    }

    /**
     * Record failed block information.
     * @param blockId block id
     * @param error error code and messages
     */
    public void addFailedBlock(long blockId, String error) {
      try (LockResource lr = new LockResource(mLoadLock)) {
        if (!mFailedBlockMap.containsKey(blockId)) {
          mFailedBlockMap.put(blockId, Sets.newHashSet());
        }
        mFailedBlockMap.get(blockId).add(error);
      }
    }

    /**
     * Set worker load finish status.
     * @param address worker address
     * @param s worker load status
     */
    public void setWorkerLoadFinished(WorkerNetAddress address, Boolean s) {
      try (LockResource lr = new LockResource(mLoadLock)) {
        mWorkerLoadFinished.put(address, s);
      }
    }
  }

  static class BlockBuffer {
    static final int BATCH_SIZE = 50;
    private final Lock mLock = new ReentrantLock();
    private final List<FileInfo> mFileInfoList;
    private final List<Block> mBufferedFileBlocks = Lists.newArrayList();
    private List<Block> mRetriedFileBlocks = Lists.newArrayList();
    private List<Block> mFailedFileBlocks = Lists.newArrayList();
    private final Iterator<FileInfo> mFileInfoIterator;
    private Iterator<Long> mBlockIdIterator;
    private FileInfo mFileInfo = null;

    /**
     * Constructor to create a BlockBuffer.
     * @param fileInfoList FileInfo list
     */
    private BlockBuffer(List<FileInfo> fileInfoList) {
      mFileInfoList = fileInfoList;
      mFileInfoIterator = mFileInfoList.iterator();
    }

    /**
     * Create a BlockBuffer.
     * @param fileInfoList fileInfoList param to use
     * @return a BlockBuffer instance
     */
    public static  BlockBuffer create(List<FileInfo> fileInfoList) {
      return new BlockBuffer(fileInfoList);
    }

    /**
     *  Get a next batch of blocks for loading.
     * @return batch list of blocks
     * @throws AlluxioRuntimeException AlluxioRuntimeException
     */
    public List<Block> getNextFileBlockBatch() throws AlluxioRuntimeException {
      try (LockResource l = new LockResource(mLock)) {
        mBufferedFileBlocks.clear();
        int i = 0;

        while (i < BATCH_SIZE) {
          if (mBlockIdIterator != null && mBlockIdIterator.hasNext()) {
            long blockId = mBlockIdIterator.next();
            Block block = buildBlock(mFileInfo, blockId);
            mBufferedFileBlocks.add(block);
            i++;
          } else if (mFileInfoIterator.hasNext()) {
            mFileInfo = mFileInfoIterator.next();
            mBlockIdIterator = mFileInfo.getBlockIds().iterator();
          } else {
            break;
          }
        }

        if (i < BATCH_SIZE && !mRetriedFileBlocks.isEmpty()) {
          int length = Math.min(BATCH_SIZE - mBufferedFileBlocks.size(), mRetriedFileBlocks.size());
          List<Block> retried = mRetriedFileBlocks.subList(0, length);
          mBufferedFileBlocks.addAll(retried);
          mRetriedFileBlocks.removeAll(retried);
        }

        return mBufferedFileBlocks;
      }
    }

    /**
     * Whether the BlockBuffer has next blocks.
     * @return boolean flag
     */
    public boolean hasNext() {
      try (LockResource l = new LockResource(mLock)) {
        while (mBlockIdIterator == null || !mBlockIdIterator.hasNext()) {
          if (mFileInfoIterator != null && mFileInfoIterator.hasNext()) {
            mFileInfo = mFileInfoIterator.next();
            mBlockIdIterator = mFileInfo.getBlockIds().iterator();
          } else {
            break;
          }
        }

        return mBlockIdIterator != null && mBlockIdIterator.hasNext()
                || !mRetriedFileBlocks.isEmpty();
      }
    }

    public static Block buildBlock(FileInfo fileInfo, long blockId) {
      return Block.newBuilder().setBlockId(blockId)
              .setBlockSize(fileInfo.getBlockSizeBytes())
              .setUfsPath(fileInfo.getUfsPath())
              .setMountId(fileInfo.getMountId())
              .build();
    }

    public void addToRetry(Block block) {
      try (LockResource l = new LockResource(mLock)) {
        mRetriedFileBlocks.add(block);
      }
    }

    public void addFailedBlock(Block block) {
      try (LockResource l = new LockResource(mLock)) {
        mFailedFileBlocks.add(block);
      }
    }
  }
}
