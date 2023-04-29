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

import java.util.concurrent.atomic.AtomicInteger;

class TaskStats {
  final AtomicInteger mBatches = new AtomicInteger();
  final AtomicInteger mStatuses = new AtomicInteger();
  final AtomicInteger mLoadErrors = new AtomicInteger();
  final AtomicInteger mLoadRequests = new AtomicInteger();
  final AtomicInteger mProcessStarted = new AtomicInteger();
  final AtomicInteger mProcessCompleted = new AtomicInteger();
  volatile boolean mLoadFailed;
  volatile boolean mProcessFailed;

  @Override
  public String toString() {
    return String.format("{TaskStats, batches %d, statuses %d, load errors %d, "
        + "load requests %d, load failed: %s, process failed: %s}",
        mBatches.get(), mStatuses.get(), mLoadErrors.get(), mLoadRequests.get(),
        mLoadFailed, mProcessFailed);
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
}
