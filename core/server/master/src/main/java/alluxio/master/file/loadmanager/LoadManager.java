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

import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.grpc.LoadResponse;
import alluxio.job.meta.JobIdGenerator;
import alluxio.master.file.loadmanager.load.LoadInfo;
import alluxio.master.file.loadmanager.load.BlockBatch;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Load manager which controls load operations.
 */
public final class LoadManager implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(LoadManager.class);
  private final Map<String, LoadInfo>
          mLoadPathToInfo = Maps.newHashMap();
  private final JobIdGenerator mJobIdGenerator = new JobIdGenerator();
  private final Map<Long, Load> mLoads = Maps.newHashMap();
  private final Scheduler mScheduler = new Scheduler();

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

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(mLoadPathToInfo.keySet().stream()
            .map(loadInfo -> Journal.JournalEntry.newBuilder().setLoadDirectory(
                            File.LoadDirectory.newBuilder()
                                    .setSrcFilePath(loadInfo)
                                    .setLoadId(0)
                    )
                    .build())
            .iterator());
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (entry.hasLoadDirectory()) {
      File.LoadDirectory loadDirectory = entry.getLoadDirectory();
      mLoadPathToInfo.put(loadDirectory.getSrcFilePath(),
              new LoadInfo(mJobIdGenerator.getNewJobId(), loadDirectory.getSrcFilePath(),
                      loadDirectory.getOptions().getBandWidth()));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void resetState() {
    mLoadPathToInfo.clear();
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.LOAD_DIRECTORY;
  }

  static class Scheduler {
    private static final int CAPACITY = 100;
    private static final long TIMEOUT = 100;
    private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();
    private final BlockingQueue<Load> mLoadQueue = new LinkedBlockingQueue<>(CAPACITY);
    private final AtomicInteger mCurrentSize = new AtomicInteger();
    private final AtomicLong mIdGenerator = new AtomicLong();

    void schedule(Load load)
            throws ResourceExhaustedException, InterruptedException {
      if (mCurrentSize.get() == CAPACITY) {
        throw new ResourceExhaustedException(
                "Insufficient capacity to enqueue load tasks!");
      }

      boolean offered = mLoadQueue.offer(load, TIMEOUT, TimeUnit.MILLISECONDS);
      if (offered) {
        mCurrentSize.incrementAndGet();
        mExecutorService.submit(() -> {
          try {
            runLoad(load);
          } catch (AlluxioRuntimeException e) {
            handleErrorOnStatuses(); // handle based on status
          } catch (TimeoutException e) {
            // add retry and handle timeout caused by checking available workers
          }
        });
      } else {
        LOG.warn("Cannot enqueue load to the queue, may lose track on this load!"
                + load.getDetailedInfo());
      }
    }

    void runLoad(Load load) throws AlluxioRuntimeException, TimeoutException {
      BlockIterator<Long> blockIterator = new BlockIterator<>(load.getPath());

      while (blockIterator.hasNextBatch()) {
        // Get a worker to handle the load task.
        ExecutionWorkerInfo worker = getNextAvailableWorker();
        if (worker == null) { // if no workers available, continue
          continue;
        }

        List<Long> blockIds = blockIterator.getNextBatchBlocks();
        BlockBatch blockBatch = new BlockBatch(blockIds, getNextBatchId());

        CompletableFuture<LoadResponse> completableFuture =
                CompletableFuture.supplyAsync(() -> worker.execute(blockBatch));
        completableFuture.thenAccept(load::setLoadResponse);
      }
    }

    void handleErrorOnStatuses() {
    }

    private long getNextBatchId() {
      return mIdGenerator.incrementAndGet();
    }

    private static ExecutionWorkerInfo getNextAvailableWorker() throws TimeoutException {
      // update currently available workers and get a next available worker.
      return null;
    }
  }

  static class Load {
    private final long mLoadId;
    private final String mPath;
    private List<LoadResponse> mLoadResponse;
    private final LoadOptions mOptions;

    public Load(long loadId, String path, LoadOptions options) {
      mLoadId = loadId;
      mPath = path;
      mOptions = options;
    }

    /*
     * Only update bandwidth.
     */
    public void updateOptions(LoadOptions options) {
      mOptions.setBandwidth(options.getBandwidth());
    }

    public void setLoadResponse(LoadResponse response) {
      mLoadResponse.add(response);
    }

    public String getPath() {
      return mPath;
    }

    public String getDetailedInfo() {
      return "";
    }
  }

  static class BlockIterator<T> {
    /**
     * Constructor to create a BlockIterator.
     * @param filePath file path
     */
    public BlockIterator(String filePath) {
    }

    public List<T> getNextBatchBlocks() throws AlluxioRuntimeException {
      return null;
    }

    /**
     * Whether the iterator has a next complete or partial batch.
     * @return boolean
     */
    public boolean hasNextBatch() {
      return true;
    }
  }
}
