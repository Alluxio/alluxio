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

import alluxio.master.file.metasync.SyncFailReason;
import alluxio.master.file.metasync.SyncOperation;
import alluxio.util.CommonUtils;

import com.google.common.base.MoreObjects;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

public class TaskStats {
  final AtomicInteger mBatches = new AtomicInteger();
  final AtomicInteger mStatuses = new AtomicInteger();
  final AtomicInteger mLoadErrors = new AtomicInteger();
  final AtomicInteger mLoadRequests = new AtomicInteger();
  final AtomicInteger mProcessStarted = new AtomicInteger();
  final AtomicInteger mProcessCompleted = new AtomicInteger();
  volatile boolean mLoadFailed;
  volatile boolean mProcessFailed;
  volatile boolean mFirstLoadFile;
  volatile boolean mFirstLoadHadResult;
  volatile AtomicLong mSyncStartTime = new AtomicLong(Long.MAX_VALUE);
  volatile AtomicLong mSyncFinishTime = new AtomicLong(Long.MIN_VALUE);
  volatile boolean mSyncFailed = false;
  volatile boolean mComplete = false;

  final Map<SyncOperation, AtomicLong> mSuccessOperationCount = new ConcurrentHashMap<>();
  final Map<Long, SyncFailure> mSyncFailReasons =
      new ConcurrentHashMap<>();

  public static class SyncFailure {
    private final LoadRequest mLoadRequest;
    @Nullable
    private final LoadResult mLoadResult;
    private final Throwable mThrowable;
    private final SyncFailReason mFailReason;

    public SyncFailure(
        LoadRequest loadRequest, @Nullable LoadResult loadResult,
        SyncFailReason failReason, Throwable throwable) {
      mLoadRequest = loadRequest;
      mLoadResult = loadResult;
      mThrowable = throwable;
      mFailReason = failReason;
    }

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

  @Override
  public String toString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
        .add("Complete", mComplete)
        .add("Sync duration", getSyncDuration())
        .add("Success op count", mSuccessOperationCount)
        .add("# of batches", mBatches.get())
        .add("# of objects loaded from UFS", mStatuses.get())
        .add("# of load requests", mLoadRequests.get())
        .add("# of load errors", mLoadErrors.get())
        .add("Load failed", mLoadFailed)
        .add("Process failed", mProcessFailed)
        .add("First load had result", mFirstLoadHadResult)
        .add("First load was file", mFirstLoadFile)
        .add("Failed load requests", mSyncFailReasons);
    return helper.toString();
  }

  public String toReportString() {
    MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    final String state;
    if (mLoadFailed) {
      state = "LOAD_FAILED";
    } else if (mProcessFailed) {
      state = "PROCESS_FAILED";
    } else if (mComplete) {
      state = "COMPLETED";
    } else {
      state = "IN_PROGRESS";
    }
    helper.add("State", state)
        .add("Sync duration", getSyncDuration() + "ms")
        .add("Success op count", mSuccessOperationCount)
        .add("# of batches", mBatches.get())
        .add("# of objects loaded from UFS", mStatuses.get())
        .add("# of load requests", mLoadRequests.get())
        .add("# of load errors", mLoadErrors.get());
    if (mSyncFailReasons.size() > 0) {
      helper.add("Failed load requests", mSyncFailReasons);
    }
    return helper.toString();
  }

  public boolean firstLoadWasFile() {
    return mFirstLoadFile;
  }

  public boolean firstLoadHadResult() {
    return mFirstLoadHadResult;
  }

  public boolean isLoadFailed() {
    return mLoadFailed;
  }

  public boolean isProcessFailed() {
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
  public int getStatusCount() {
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

  void setFirstLoadHadResult() {
    mFirstLoadHadResult = true;
  }

  void setComplete() {
    mComplete = true;
  }

  void setFirstLoadFile() {
    mFirstLoadFile = true;
  }

  /**
   * @return success operation count map
   */
  public Map<SyncOperation, AtomicLong> getSuccessOperationCount() {
    return mSuccessOperationCount;
  }

  /**
   * reports the completion of a successful sync operation.
   * @param operation the operation
   * @param count the number of successes
   */
  public void reportSyncOperationSuccess(SyncOperation operation, long count) {
    mSuccessOperationCount.compute(operation, (k, v) -> {
      if (v == null) {
        return new AtomicLong(count);
      }
      v.addAndGet(count);
      return v;
    });
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
   * @param timestamp the timestamp
   */
  public void updateSyncStartTime(long timestamp) {
    mSyncStartTime.updateAndGet(
        (ts) -> Math.min(ts, timestamp)
    );
  }

  /**
   * @param timestamp the timestamp
   */
  public void updateSyncFinishTime(long timestamp) {
    mSyncFinishTime.updateAndGet(
        (ts) -> Math.max(ts, timestamp)
    );
  }

  /**
   * @return the sync duration in ms
   */
  public Long getSyncDuration() {
    long start = mSyncStartTime.get();
    long finish = mSyncFinishTime.get();
    if (!mComplete && start != Long.MAX_VALUE) {
      return CommonUtils.getCurrentMs() - start;
    }
    if (start == Long.MAX_VALUE || finish == Long.MIN_VALUE) {
      return null;
    }
    return finish - start;
  }


  public void reportSyncFailReason(LoadRequest request, @Nullable LoadResult loadResult, SyncFailReason reason, Throwable t) {
    mSyncFailReasons.putIfAbsent(
        request.getLoadRequestId(), new SyncFailure(request, loadResult, reason, t)
    );
  }

  public Map<Long, SyncFailure> getSyncFailReasons() {
    return mSyncFailReasons;
  }
}
