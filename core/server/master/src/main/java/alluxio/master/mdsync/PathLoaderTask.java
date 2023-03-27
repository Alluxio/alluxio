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

package alluxio.master.mdsync;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;
import alluxio.file.options.DescendantType;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import javax.annotation.Nullable;

/**
 * This is the task for handling the loading of a path from the UFS.
 * It will consist of at least 1 load request.
 */
public class PathLoaderTask {
  private static final Logger LOG = LoggerFactory.getLogger(PathLoaderTask.class);

  private final TaskInfo mTaskInfo;
  /**
   * All load requests that are ready, but have not yet started executing.
   */
  private final ConcurrentLinkedDeque<LoadRequest> mNextLoad;
  /**
   * These are all running (or ready to be run) load requests.
   */
  private final ConcurrentHashMap<Long, LoadRequest> mRunningLoads = new ConcurrentHashMap<>();
  /**
   * The load id that starts each load (where a load is a set of multiple load batches until
   * a batch is not truncated) is stored here until the request that truncates this load
   * is completed.
   */
  private final ConcurrentHashSet<Long> mTruncatedLoads = new ConcurrentHashSet<>();
  private long mNxtLoadId = 0;
  private boolean mCompleted = false;
  private Runnable mRunOnPendingLoad;

  private final UfsClient mClient;

  private DescendantType computeDescendantType() {
    if (mTaskInfo.getDescendantType() == DescendantType.ALL
        && mTaskInfo.getLoadByDirectory() != DirectoryLoadType.NONE) {
      return DescendantType.ONE;
    }
    return mTaskInfo.getDescendantType();
  }

  /**
   * Create a new PathLoaderTask.
   * @param taskInfo
   * @param continuationToken
   */
  public PathLoaderTask(
      TaskInfo taskInfo, @Nullable String continuationToken) {
    mTaskInfo = taskInfo;
    final long loadId = mNxtLoadId++;
    LoadRequest firstRequest = new LoadRequest(loadId, loadId, mTaskInfo, mTaskInfo.getBasePath(),
        continuationToken, computeDescendantType());
    if (mTaskInfo.getDescendantType() == DescendantType.NONE) {
      mNextLoad = new ConcurrentLinkedDeque<>();
    } else {
      mNextLoad = new ConcurrentLinkedDeque<>();
    }
    addLoadRequest(firstRequest, true);
    mClient = mTaskInfo.getMdSync().getClient(taskInfo.getBasePath());
  }

  boolean isComplete() {
    return mCompleted;
  }

  TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  UfsClient getClient() {
    return mClient;
  }

  synchronized void runOnPendingLoad(Runnable toRun) {
    mRunOnPendingLoad = toRun;
  }

  synchronized Optional<LoadResult> createLoadResult(
      long requestId, UfsLoadResult ufsLoadResult) {
    mTaskInfo.getStats().gotBatch(ufsLoadResult.getItemsCount());
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
    if (ufsLoadResult.isTruncated()) {
      // If truncated, need to submit a new task for the next set of items
      final long loadId = mNxtLoadId++;
      addLoadRequest(new LoadRequest(loadId, originalRequest.getBatchSetId(), mTaskInfo,
          originalRequest.getLoadPath(), ufsLoadResult.getContinuationToken(),
          computeDescendantType()), false);
    }
    return Optional.of(new LoadResult(requestId, originalRequest.getLoadPath(),
        mTaskInfo, ufsLoadResult));
  }

  void loadNestedDirectory(AlluxioURI path) {
    // If we are loading by directory, then we must create a new load task on each
    // directory traversed
    synchronized (this) {
      final long loadId = mNxtLoadId++;
      addLoadRequest(new LoadRequest(loadId, loadId, mTaskInfo, path,
          null, computeDescendantType()), true);
    }
  }

  private void addLoadRequest(LoadRequest loadRequest, boolean isFirstForPath) {
    mRunningLoads.put(loadRequest.getLoadRequestId(), loadRequest);
    if (mTaskInfo.getLoadByDirectory() == DirectoryLoadType.BFS) {
      mNextLoad.addLast(loadRequest);
    } else {
      mNextLoad.addFirst(loadRequest);
    }
    if (isFirstForPath) {
      mTruncatedLoads.add(loadRequest.getLoadRequestId());
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
    synchronized (this) {
      LoadRequest request = mRunningLoads.remove(loadRequestId);
      if (request != null && !result.isTruncated()) {
        Preconditions.checkState(mTruncatedLoads.remove(request.getBatchSetId()),
            "load request %s finished, without finding the load %s that started the batch loading",
            loadRequestId, request.getBatchSetId());
      }
      if (mTruncatedLoads.size() == 0 && mRunningLoads.size() == 0) {
        // all sets of loads have finished
        mCompleted = true;
        mTaskInfo.getMdSync().onPathLoadComplete(mTaskInfo.getId(), result.rootPathIsFile());
      }
    }
  }

  synchronized void onProcessError(Throwable t) {
    // If there is a processing error then we fail the entire task
    mTaskInfo.getStats().setProcessFailed();
    mCompleted = true;
    mTaskInfo.getMdSync().onFailed(mTaskInfo.getId(), t);
  }

  void onLoadRequestError(long id, Throwable t) {
    mTaskInfo.getStats().gotLoadError();
    synchronized (this) {
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
        return;
      } else {
        LOG.warn("Path loader task failed of load on path {},"
                + "with id {} with continuation token {} after error {}",
            mTaskInfo, load.getLoadRequestId(), load.getContinuationToken(), t);
        mCompleted = true;
      }
    }
    mTaskInfo.getStats().setLoadFailed();
    mTaskInfo.getMdSync().onFailed(mTaskInfo.getId(), t);
  }

  synchronized void cancel() {
    LOG.debug("Canceling load task on path {}", mTaskInfo);
    mCompleted = true;
  }

  Optional<LoadRequest> getNext() {
    return Optional.ofNullable(mNextLoad.poll());
  }
}
