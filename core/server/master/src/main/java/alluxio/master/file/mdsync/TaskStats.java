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

import alluxio.collections.Pair;

import com.google.common.base.MoreObjects;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * The metadata sync task stats.
 */
public class TaskStats {
  private final AtomicInteger mBatches = new AtomicInteger();
  private final AtomicInteger mStatuses = new AtomicInteger();
  private final AtomicInteger mLoadErrors = new AtomicInteger();
  private final AtomicInteger mLoadRequests = new AtomicInteger();
  final AtomicInteger mProcessStarted = new AtomicInteger();
  final AtomicInteger mProcessCompleted = new AtomicInteger();
  private final AtomicLong[] mSuccessOperationCount;
  private final Map<Long, SyncFailure> mSyncFailReasons =
      new ConcurrentHashMap<>();
  private volatile boolean mLoadFailed;
  private volatile boolean mProcessFailed;
  private volatile boolean mFirstLoadFile;
  private volatile boolean mSyncFailed = false;

  /**
   * Creates a new task stats.
   */
  public TaskStats() {
    mSuccessOperationCount = new AtomicLong[SyncOperation.values().length];
    for (int i = 0; i < mSuccessOperationCount.length; ++i) {
      mSuccessOperationCount[i] = new AtomicLong();
    }
  }

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
        .add("Success op count", getSuccessOperationCountString().getSecond())
        .add("# of batches", mBatches.get())
        .add("# of objects loaded from UFS", mStatuses.get())
        .add("# of load requests", mLoadRequests.get())
        .add("# of load errors", mLoadErrors.get())
        .add("Load failed", mLoadFailed)
        .add("Process failed", mProcessFailed)
        .add("First load was file", mFirstLoadFile)
        .add("Failed load requests", mSyncFailReasons);
    return helper.toString();
  }

  /**
   * @return a formatted string that is used to display as the cli command output
   */
  public Pair<Long, String> toReportString() {
    Pair<Long, String> successOps = getSuccessOperationCountString();
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.add("Success op count", successOps.getSecond())
        .add("# of batches", mBatches.get())
        .add("# of objects loaded from UFS", mStatuses.get())
        .add("# of load requests", mLoadRequests.get())
        .add("# of load errors", mLoadErrors.get());
    if (mSyncFailReasons.size() > 0) {
      helper.add("Failed load requests", mSyncFailReasons);
    }
    return new Pair<>(successOps.getFirst(), helper.toString());
  }

  /**
   * @return if the first load was file
   */
  boolean firstLoadWasFile() {
    return mFirstLoadFile;
  }

  /**
   * @return if the load is failed
   */
  boolean isLoadFailed() {
    return mLoadFailed;
  }

  /**
   * @return if the processing is failed
   */
  boolean isProcessFailed() {
    return mProcessFailed;
  }

  int getLoadRequestCount() {
    return mLoadRequests.get();
  }

  int getBatchCount() {
    return mBatches.get();
  }

  /**
   * @return the status count
   */
  int getStatusCount() {
    return mStatuses.get();
  }

  int getLoadErrors() {
    return mLoadErrors.get();
  }

  void gotBatch(int size) {
    mBatches.incrementAndGet();
    mStatuses.addAndGet(size);
  }

  void gotLoadRequest() {
    mLoadRequests.incrementAndGet();
  }

  void gotLoadError() {
    mLoadErrors.incrementAndGet();
  }

  void setLoadFailed() {
    mLoadFailed = true;
  }

  void setProcessFailed() {
    mProcessFailed = true;
  }

  void setFirstLoadFile() {
    mFirstLoadFile = true;
  }

  /**
   * @return success operation count map
   */
  public AtomicLong[] getSuccessOperationCount() {
    return mSuccessOperationCount;
  }

  private Pair<Long, String> getSuccessOperationCountString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    long total = 0;
    for (int i = 0; i < mSuccessOperationCount.length; ++i) {
      long value = mSuccessOperationCount[i].get();
      total += value;
      if (value != 0) {
        sb.append("[")
            .append(SyncOperation.fromInteger(i))
            .append(":")
            .append(value)
            .append("]");
      }
    }
    sb.append("}");
    return new Pair<>(total, sb.toString());
  }

  /**
   * reports the completion of a successful sync operation.
   *
   * @param operation the operation
   * @param count     the number of successes
   */
  void reportSyncOperationSuccess(SyncOperation operation, long count) {
    mSuccessOperationCount[operation.getValue()].addAndGet(count);
  }

  /**
   * Sets the sync failed.
   */
  public void setSyncFailed() {
    mSyncFailed = true;
  }

  /**
   * @return if the sync failed
   */
  public boolean getSyncFailed() {
    return mSyncFailed;
  }

  /**
   * Reports a sync fail reason.
   * @param request the load request
   * @param loadResult the load result
   * @param reason the sync fail reason
   * @param t the exception
   */
  void reportSyncFailReason(
      LoadRequest request, @Nullable LoadResult loadResult,
      SyncFailReason reason, Throwable t) {
    mSyncFailReasons.putIfAbsent(
        request.getLoadRequestId(), new SyncFailure(request, loadResult, reason, t)
    );
  }

  /**
   * @return the sync fail reason map
   * The key is the load request id and the value is the failure.
   * A reported error does not necessarily fail the sync as we retry. This map only records all
   * failures we even encountered. Please refer to BaseTask::getState to get the sync task state.
   */
  public Map<Long, SyncFailure> getSyncFailReasons() {
    return mSyncFailReasons;
  }

  /**
   * The sync failure.
   */
  public static class SyncFailure {
    private final LoadRequest mLoadRequest;
    @Nullable
    private final LoadResult mLoadResult;
    private final Throwable mThrowable;
    private final SyncFailReason mFailReason;

    /**
     * Constructs an object.
     * @param loadRequest the load request
     * @param loadResult the load result
     * @param failReason the fail reason
     * @param throwable the exception
     */
    public SyncFailure(
        LoadRequest loadRequest, @Nullable LoadResult loadResult,
        SyncFailReason failReason, Throwable throwable) {
      mLoadRequest = loadRequest;
      mLoadResult = loadResult;
      mThrowable = throwable;
      mFailReason = failReason;
    }

    /**
     * @return the sync fail reason
     */
    public SyncFailReason getSyncFailReason() {
      return mFailReason;
    }

    @Override
    public String toString() {
      String loadFrom = "{beginning}";
      if (mLoadRequest.getPreviousLoadLast().isPresent()) {
        loadFrom = mLoadRequest.getPreviousLoadLast().get().toString();
      }
      String loadUntil = "{N/A}";
      if (mLoadResult != null && mLoadResult.getUfsLoadResult().getLastItem().isPresent()) {
        loadUntil = mLoadResult.getUfsLoadResult().getLastItem().get().toString();
      }

      MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
          .add("LoadRequestId", mLoadRequest.getLoadRequestId())
          .add("FailReason", mFailReason)
          .add("DescendantType", mLoadRequest.getDescendantType())
          .add("LoadPath", mLoadRequest.getLoadRequestId())
          .add("LoadFrom", loadFrom)
          .add("LoadUntil", loadUntil)
          .add("Exception", mThrowable);
      return helper.toString();
    }
  }
}
