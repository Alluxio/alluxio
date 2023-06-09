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

package alluxio.master.file.mdsync;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsLoadResult;
import alluxio.util.RateLimiter;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * This is the task for handling the loading of a path from the UFS.
 * It will consist of at least 1 load request.
 */
public class PathLoaderTask {
  private static final Logger LOG = LoggerFactory.getLogger(PathLoaderTask.class);

  public static final Counter PROCESS_FAIL_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_PROCESSING_FAILED.getName());
  public static final Counter LOAD_FAIL_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_LOADS_FAILED.getName());

  /**
   * All load requests that are ready, but have not yet started executing.
   * This must be concurrent safe as other threads will poll it to get the
   * next load request.
   */
  private final PriorityBlockingQueue<LoadRequest> mNextLoad;
  /**
   * True when the task is completed, must be volatile, as other threads
   * will access it to check if they should stop polling {@link PathLoaderTask#mNextLoad}.
   */
  private volatile boolean mCompleted = false;
  /**
   * These are all running (or ready to be run) load requests.
   */
  private final HashMap<Long, LoadRequest> mRunningLoads = new HashMap<>();
  /**
   * The load id that starts each load (where a load is a set of multiple load batches until
   * a batch is not truncated) is stored here until the request that truncates this load
   * is completed.
   */
  private final HashSet<Long> mTruncatedLoads = new HashSet<>();
  private final TaskInfo mTaskInfo;
  private long mNxtLoadId = 0;
  private Runnable mRunOnPendingLoad;
  private final RateLimiter mRateLimiter;

  private final Function<AlluxioURI, CloseableResource<UfsClient>> mClientSupplier;

  private DescendantType computeDescendantType() {
    if (mTaskInfo.getDescendantType() == DescendantType.ALL
        && mTaskInfo.getLoadByDirectory() != DirectoryLoadType.SINGLE_LISTING) {
      return DescendantType.ONE;
    }
    return mTaskInfo.getDescendantType();
  }

  /**
   * Create a new PathLoaderTask.
   * @param taskInfo task info
   * @param continuationToken token
   * @param clientSupplier the client supplier
   */
  public PathLoaderTask(
      TaskInfo taskInfo, @Nullable String continuationToken,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier) {
    mTaskInfo = taskInfo;
    final long loadId = mNxtLoadId++;
    // the first load request will get a GetStatus check on the path
    // the following loads will be listings
    LoadRequest firstRequest = new LoadRequest(loadId, loadId, mTaskInfo, mTaskInfo.getBasePath(),
        continuationToken, null, computeDescendantType(), true);
    mNextLoad = new PriorityBlockingQueue<>();
    addLoadRequest(firstRequest, true);
    mClientSupplier = clientSupplier;
    try (CloseableResource<UfsClient> client = mClientSupplier.apply(mTaskInfo.getBasePath())) {
      mRateLimiter = client.get().getRateLimiter();
    }
  }

  RateLimiter getRateLimiter() {
    return mRateLimiter;
  }

  boolean isComplete() {
    return mCompleted;
  }

  TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  CloseableResource<UfsClient> getClient() {
    return mClientSupplier.apply(mTaskInfo.getBasePath());
  }

  synchronized void runOnPendingLoad(Runnable toRun) {
    mRunOnPendingLoad = toRun;
  }

  synchronized Optional<LoadResult> createLoadResult(
      long requestId, UfsLoadResult ufsLoadResult) {
    if (mCompleted) {
      return Optional.empty();
    }
    LoadRequest originalRequest = mRunningLoads.get(requestId);
    if (originalRequest == null) {
      LOG.debug("Received a load result for task {} for a load that was already"
              + "removed with id {}",
          mTaskInfo, requestId);
      return Optional.empty();
    }
    TaskStats stats = mTaskInfo.getStats();
    stats.gotBatch(ufsLoadResult.getItemsCount());
    if (originalRequest.isFirstLoad() && ufsLoadResult.isFirstFile()) {
      stats.setFirstLoadFile();
    }
    // If truncated, need to submit a new task for the next set of items
    // unless descendant type is none
    boolean shouldLoadMore = originalRequest.getDescendantType() != DescendantType.NONE
          && ufsLoadResult.isTruncated();
    if (shouldLoadMore) {
      final long loadId = mNxtLoadId++;
      addLoadRequest(new LoadRequest(loadId, originalRequest.getBatchSetId(), mTaskInfo,
              originalRequest.getLoadPath(), ufsLoadResult.getContinuationToken(),
              ufsLoadResult.getLastItem().orElse(null),
              computeDescendantType(), false),
          false);
    }
    return Optional.of(new LoadResult(originalRequest, originalRequest.getLoadPath(),
        mTaskInfo, originalRequest.getPreviousLoadLast().orElse(null),
        ufsLoadResult, originalRequest.isFirstLoad()));
  }

  void loadNestedDirectory(AlluxioURI path) {
    // If we are loading by directory, then we must create a new load task on each
    // directory traversed
    synchronized (this) {
      final long loadId = mNxtLoadId++;
      addLoadRequest(new LoadRequest(loadId, loadId, mTaskInfo, path,
          null, null, computeDescendantType(), false), true);
    }
  }

  private void addLoadRequest(LoadRequest loadRequest, boolean isFirstForPath) {
    mRunningLoads.put(loadRequest.getLoadRequestId(), loadRequest);
    mNextLoad.add(loadRequest);
    if (isFirstForPath) {
      mTruncatedLoads.add(loadRequest.getBatchSetId());
    }
    if (mRunOnPendingLoad != null) {
      mRunOnPendingLoad.run();
    }
  }

  /**
   * This should be called when a load request task with id is finished
   * processing by the metadata sync.
   * @param loadRequestId the id of the finished task
   */
  void onProcessComplete(long loadRequestId, SyncProcessResult result) {
    mTaskInfo.getMdSync().onEachResult(mTaskInfo.getId(), result);
    boolean completed = false;
    synchronized (this) {
      LoadRequest request = mRunningLoads.remove(loadRequestId);
      if (request != null && !result.isTruncated()) {
        Preconditions.checkState(mTruncatedLoads.remove(request.getBatchSetId()),
            "load request %s finished, without finding the load %s that started the batch loading",
            loadRequestId, request.getBatchSetId());
      }
      if (mTruncatedLoads.size() == 0 && mRunningLoads.size() == 0) {
        // all sets of loads have finished
        completed = true;
        mCompleted = true;
      }
    }
    if (completed) {
      mTaskInfo.getMdSync().onPathLoadComplete(mTaskInfo.getId(),
          result.rootPathIsFile());
    }
  }

  synchronized void onProcessError(Throwable t) {
    PROCESS_FAIL_COUNT.inc();
    // If there is a processing error then we fail the entire task
    mTaskInfo.getStats().setProcessFailed();
    mCompleted = true;
    mTaskInfo.getMdSync().onFailed(mTaskInfo.getId(), t);
  }

  synchronized void onLoadRequestError(long id, Throwable t) {
    LOAD_FAIL_COUNT.inc();
    mTaskInfo.getStats().gotLoadError();
    if (mCompleted) {
      LOG.debug("Received a load error for task {} wit id {} after the task was completed",
          mTaskInfo, id);
      return;
    }
    LoadRequest load = mRunningLoads.get(id);
    if (load == null) {
      LOG.debug("Received a load error for task {} for a load that was already"
              + "removed with id {}",
          mTaskInfo, id);
      return;
    }
    if (load.attempt()) {
      LOG.debug("Rescheduling retry of load on path {}, with id {}, with continuation token {}"
              + "after error {}",
          mTaskInfo, load.getLoadRequestId(), load.getContinuationToken(), t);
      addLoadRequest(load, false);
    } else {
      LOG.warn("Path loader task failed of load on path {},"
              + "with id {} with continuation token {} after error {}",
          mTaskInfo, load.getLoadRequestId(), load.getContinuationToken(), t);
      mCompleted = true;
      mTaskInfo.getStats().setLoadFailed();
      mTaskInfo.getMdSync().onFailed(mTaskInfo.getId(), t);
    }
  }

  synchronized void cancel() {
    LOG.debug("Canceling load task on path {}", mTaskInfo);
    mCompleted = true;
  }

  Optional<LoadRequest> getNext() {
    return Optional.ofNullable(mNextLoad.poll());
  }

  int getPendingLoadCount() {
    return mNextLoad.size();
  }
}
