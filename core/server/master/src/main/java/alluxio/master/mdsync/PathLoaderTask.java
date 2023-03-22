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

import alluxio.file.options.DescendantType;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.UfsStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * This is the task for handling the loading of a path from the UFS.
 * It will consist of at least 1 load request.
 */
public class PathLoaderTask {
  private static final Logger LOG = LoggerFactory.getLogger(PathLoaderTask.class);

  private final TaskInfo mTaskInfo;
  private final RetryPolicy mRetryPolicy = new CountingRetry(2);
  private final ArrayDeque<LoadRequest> mNextLoad;
  private final Consumer<Throwable> mOnFailed;
  private final ConcurrentHashMap<Long, LoadRequest> mRunningLoads = new ConcurrentHashMap<>();
  private long mNxtLoadId = 0;
  private boolean mCompleted = false;
  private final Consumer<SyncProcessResult> mOnEachResult;
  private final Consumer<Boolean> mOnComplete;
  private Runnable mRunOnPendingLoad;

  private final UfsClient mClient;

  private boolean hasDirLoadTasks() {
    return mTaskInfo.getDescendantType() == DescendantType.ALL
        && mTaskInfo.getLoadByDirectory() != DirectoryLoadType.NONE;
  }

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
   * @param onEachResult
   * @param onComplete
   * @param onFailed
   */
  public PathLoaderTask(
      TaskInfo taskInfo, @Nullable String continuationToken,
      Consumer<SyncProcessResult> onEachResult, Consumer<Boolean> onComplete,
      Consumer<Throwable> onFailed) {
    mTaskInfo = taskInfo;
    final long loadId = mNxtLoadId++;
    LoadRequest firstRequest = new LoadRequest(loadId, mTaskInfo, mTaskInfo.getBasePath(),
        continuationToken, computeDescendantType(),
        result -> createLoadResult(loadId, result), t -> onLoadError(loadId, t));
    if (mTaskInfo.getDescendantType() == DescendantType.NONE) {
      mNextLoad = new ArrayDeque<>(1);
    } else {
      mNextLoad = new ArrayDeque<>();
    }
    addLoadRequest(firstRequest);
    mClient = UfsClient.getClientByPath(taskInfo.getBasePath());
    mOnEachResult = onEachResult;
    mOnComplete = onComplete;
    mOnFailed = onFailed;
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

  private synchronized Optional<LoadResult> createLoadResult(
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
    if (hasDirLoadTasks()) {
      // loading of children directories will take place as different tasks
      Stream<UfsStatus> items = ufsLoadResult.getItems().peek(nxt -> {
        if (nxt.isDirectory()) {
          synchronized (this) {
            final long loadId = mNxtLoadId++;
            addLoadRequest(new LoadRequest(loadId, mTaskInfo,
                originalRequest.getLoadPath().join(nxt.getName()),
                 null, computeDescendantType(),
                result -> createLoadResult(loadId, result), t -> onLoadError(loadId, t)));
          }
        }
      });
      ufsLoadResult = new UfsLoadResult(items, ufsLoadResult.getContinuationToken(),
          ufsLoadResult.isTruncated());
    }
    if (ufsLoadResult.isTruncated()) {
      // If truncated, need to submit a new task for the next set of items
      final long loadId = mNxtLoadId++;
      addLoadRequest(new LoadRequest(loadId, mTaskInfo, originalRequest.getLoadPath(),
          ufsLoadResult.getContinuationToken(), computeDescendantType(),
          result -> createLoadResult(loadId, result), t -> onLoadError(loadId, t)));
    }
    return Optional.of(new LoadResult(originalRequest.getLoadPath(), mTaskInfo, ufsLoadResult,
        syncProcessResult -> onProcessComplete(requestId, syncProcessResult),
        this::onProcessError));
  }

  private void addLoadRequest(LoadRequest loadRequest) {
    if (mTaskInfo.getLoadByDirectory() == DirectoryLoadType.BFS) {
      mNextLoad.addLast(loadRequest);
    } else {
      mNextLoad.addFirst(loadRequest);
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
  synchronized void onProcessComplete(long loadRequestId, SyncProcessResult result) {
    mOnEachResult.accept(result);
    if (!result.isTruncated()) {
      mRunningLoads.remove(loadRequestId);
      if (mRunningLoads.size() == 0) {
        mCompleted = true;
        mOnComplete.accept(result.rootPathIsFile());
      }
    }
  }

  private synchronized void onProcessError(Throwable t) {
    // If there is a processing error then we fail the entire task
    mCompleted = true;
    mOnFailed.accept(t);
  }

  void onLoadError(long id, Throwable t) {
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
      if (mRetryPolicy.attempt()) {
        LOG.debug("Rescheduling retry of load on path {}, with id {}, with continuation token {}"
                + "after error {}",
            mTaskInfo, load.getId(), load.getContinuationToken(), t);
        mNextLoad.addFirst(load);
        return;
      } else {
        LOG.warn("Path loader task failed of load on path {},"
                + "with id {} with continuation token {} after error {}",
            mTaskInfo, load.getId(), load.getContinuationToken(), t);
        mCompleted = true;
      }
    }
    mOnFailed.accept(t);
  }

  synchronized void cancel() {
    LOG.debug("Canceling load task on path {}", mTaskInfo);
    mCompleted = true;
  }

  synchronized Optional<LoadRequest> getNext() {
    return Optional.ofNullable(mNextLoad.poll());
  }
}
