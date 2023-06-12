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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import alluxio.Constants;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsLoadResult;
import alluxio.util.logging.SamplingLogger;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

class LoadRequestExecutor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LoadRequestExecutor.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 5L * Constants.SECOND_MS);

  /** Limit the number of running (or completed but not yet processed) load requests. **/
  private final AtomicInteger mRemainingTickets;
  private final int mMaxRunning;

  private final Map<Long, PathLoaderTask> mPathLoaderTasks = new ConcurrentHashMap<>();
  // Loader tasks with pending loads
  private final Set<Long> mPathLoaderTasksWithPendingLoads = new ConcurrentHashSet<>();
  // Same as above, except ordered by priority
  private final ConcurrentLinkedDeque<Long> mPathLoaderTaskQueue = new ConcurrentLinkedDeque<>();
  // Load requests in order of to be processed
  private final BlockingQueue<LoadRequest> mLoadRequests = new LinkedBlockingQueue<>();
  // Rate limited loads that are not yet ready to be run
  private final PriorityQueue<RateLimitedRequest> mRateLimited = new PriorityQueue<>();

  private final LoadResultExecutor mResultExecutor;

  private final Thread mExecutor;

  LoadRequestExecutor(int maxRunning, LoadResultExecutor resultExecutor) {
    mMaxRunning = maxRunning;
    mRemainingTickets = new AtomicInteger(maxRunning);
    mResultExecutor = resultExecutor;
    mExecutor = new Thread(() -> {
      while (!Thread.interrupted()) {
        try {
          runNextLoadTask();
        } catch (InterruptedException e) {
          return;
        }
      }
      LOG.info("Load request runner thread exiting");
    }, "LoadRequestRunner");
    mExecutor.start();
    registerMetrics();
  }

  synchronized void addPathLoaderTask(PathLoaderTask task) {
    long id = task.getTaskInfo().getId();
    task.runOnPendingLoad(() -> hasNewLoadTask(id));
    mPathLoaderTasks.put(id, task);
    mPathLoaderTaskQueue.add(id);
    mPathLoaderTasksWithPendingLoads.add(id);
    notifyAll();
  }

  synchronized void hasNewLoadTask(long taskId) {
    if (!mPathLoaderTasksWithPendingLoads.contains(taskId)) {
      mPathLoaderTaskQueue.add(taskId);
      mPathLoaderTasksWithPendingLoads.add(taskId);
      notifyAll();
    }
  }

  private void onLoadError(LoadRequest request, Throwable t) {
    // Errors are reported on an attempt basis. A reported load error does not
    // lead to the sync failure because we retry on UFS load failure. The sync
    // can still proceed if the following try succeeds.
    // Please refer to BaseTask::getState to get the sync task state.
    if (t instanceof DefaultSyncProcess.MountPointNotFoundRuntimeException) {
      request.getTaskInfo().getStats().reportSyncFailReason(
          request, null, SyncFailReason.LOADING_MOUNT_POINT_DOES_NOT_EXIST, t);
    } else {
      request.getTaskInfo().getStats().reportSyncFailReason(
          request, null, SyncFailReason.LOADING_UFS_IO_FAILURE, t);
    }
    releaseRunning();
    request.onError(t);
  }

  private void processLoadResult(LoadRequest request, UfsLoadResult ufsLoadResult) {
    Optional<LoadResult> loadResult = request.getTaskInfo().getMdSync()
        .onReceiveLoadRequestOutput(request.getBaseTaskId(),
            request.getLoadRequestId(), ufsLoadResult);
    synchronized (this) {
      PathLoaderTask task = mPathLoaderTasks.get(request.getBaseTaskId());
      if (task != null && loadResult.isPresent()) {
        LoadResult result = loadResult.get();
        mResultExecutor.processLoadResult(result, () -> {
          releaseRunning();
          result.getTaskInfo().getStats().mProcessStarted.incrementAndGet();
        }, v -> {
          result.getTaskInfo().getStats().mProcessCompleted.incrementAndGet();
          result.onProcessComplete(v);
        }, result::onProcessError);
      } else {
        releaseRunning();
        if (loadResult.isPresent()) {
          LOG.debug("Got a load result for id {} with no corresponding"
              + "path loader task", request.getBaseTaskId());
        }
      }
    }
  }

  private void runNextLoadTask() throws InterruptedException {
    // loop until there is a task ready to execute
    synchronized (this) {
      while ((mLoadRequests.isEmpty() || mRemainingTickets.get() == 0)
          && (mRateLimited.isEmpty() || !mRateLimited.peek().isReady())) {
        // check if a task is ready to run, and we have tickets remaining
        if (mRemainingTickets.get() > 0 && !mPathLoaderTaskQueue.isEmpty()) {
          Long nextId = mPathLoaderTaskQueue.poll();
          if (nextId != null) {
            checkNextLoad(nextId);
          }
        } else { // otherwise, sleep
          long waitNanos = 0;
          if (!mRateLimited.isEmpty()) {
            waitNanos = mRateLimited.peek().getWaitTime();
            if (waitNanos <= 0) {
              break;
            }
          }
          // wait until a rate limited task is ready, or this.notifyAll() is called
          if (waitNanos == 0) {
            wait();
          } else {
            // we only sleep if our wait time is less than 1 ms
            // otherwise we spin wait
            if (waitNanos >= Constants.MS_NANO) {
              NANOSECONDS.timedWait(this, waitNanos);
            }
          }
        }
      }
    }
    SAMPLING_LOG.info("Concurrent running ufs load tasks {}, tasks with pending load requests {},"
            + " rate limited pending requests {}",
        mMaxRunning - mRemainingTickets.get(), mPathLoaderTasks.size(), mRateLimited.size());
    if (!mRateLimited.isEmpty() && mRateLimited.peek().isReady()) {
      RateLimitedRequest request = mRateLimited.remove();
      runTask(request.mTask, request.mLoadRequest);
    } else {
      LoadRequest nxtRequest = mLoadRequests.take();
      PathLoaderTask task = mPathLoaderTasks.get(nxtRequest.getBaseTaskId());
      if (task != null) {
        Preconditions.checkState(mRemainingTickets.decrementAndGet() >= 0);
        Optional<Long> rateLimit = task.getRateLimiter().acquire();
        if (rateLimit.isPresent()) {
          mRateLimited.add(new RateLimitedRequest(task, nxtRequest, rateLimit.get()));
        } else {
          runTask(task, nxtRequest);
        }
      } else {
        LOG.debug("Got load request {} with task id {} with no corresponding task",
            nxtRequest.getLoadRequestId(), nxtRequest.getLoadRequestId());
      }
    }
  }

  private synchronized void releaseRunning() {
    mRemainingTickets.incrementAndGet();
    notifyAll();
  }

  synchronized void onTaskComplete(long taskId) {
    mPathLoaderTasks.remove(taskId);
  }

  private void runTask(PathLoaderTask task, LoadRequest loadRequest) {
    try (CloseableResource<UfsClient> client = task.getClient()) {
      @Nullable String startAfter = null;
      if (loadRequest.isFirstLoad()) {
        startAfter = loadRequest.getTaskInfo().getStartAfter();
      }
      client.get().performListingAsync(loadRequest.getLoadPath().getPath(),
          loadRequest.getContinuationToken(), startAfter,
          loadRequest.getDescendantType(), loadRequest.isFirstLoad(),
          ufsLoadResult -> processLoadResult(loadRequest, ufsLoadResult),
          t -> onLoadError(loadRequest, t));
    } catch (Throwable t) {
      onLoadError(loadRequest, t);
    }
  }

  private void checkNextLoad(long id) {
    PathLoaderTask task = mPathLoaderTasks.get(id);
    if (task == null || task.isComplete()) {
      mPathLoaderTasks.remove(id);
      mPathLoaderTasksWithPendingLoads.remove(id);
      return;
    }
    Optional<LoadRequest> nxtRequest = task.getNext();
    if (nxtRequest.isPresent()) {
      try {
        mLoadRequests.put(nxtRequest.get());
        mPathLoaderTaskQueue.addLast(id);
      } catch (InterruptedException e) {
        throw new InternalRuntimeException("Not expected to block here", e);
      }
    } else {
      mPathLoaderTasksWithPendingLoads.remove(id);
    }
  }

  @Override
  public void close() throws IOException {
    mExecutor.interrupt();
    try {
      mExecutor.join(5_000);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for load request runner to terminate");
    }
    mResultExecutor.close();
  }

  private void registerMetrics() {
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(
            MetricKey.MASTER_METADATA_SYNC_QUEUED_LOADS.getName()),
        () -> {
          synchronized (this) {
            int count = 0;
            for (PathLoaderTask task : mPathLoaderTasks.values()) {
              count += task.getPendingLoadCount();
            }
            return count;
          }
        });
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(
            MetricKey.MASTER_METADATA_SYNC_RUNNING_LOADS.getName()),
        () -> mMaxRunning - mRemainingTickets.get());
  }
}
