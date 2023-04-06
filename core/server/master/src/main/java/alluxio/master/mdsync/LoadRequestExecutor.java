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

import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

class LoadRequestExecutor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LoadRequestExecutor.class);

  /** Limit the number of running (or completed but not yet processed) load requests. **/
  private final Semaphore mRunning;
  private final int mMaxRunning;

  private final Map<Long, PathLoaderTask> mPathLoaderTasks = new ConcurrentHashMap<>();
  // Loader tasks with pending loads
  private final Set<Long> mPathLoaderTasksWithPendingLoads = new ConcurrentHashSet<>();
  // Same as above, except ordered by priority
  private final ConcurrentLinkedDeque<Long> mPathLoaderTaskQueue = new ConcurrentLinkedDeque<>();
  // Load requests in order of to be processed
  private final BlockingQueue<LoadRequest> mLoadRequests = new LinkedBlockingQueue<>();

  private final LoadResultExecutor mResultExecutor;

  private final Thread mExecutor;

  LoadRequestExecutor(int maxRunning, LoadResultExecutor resultExecutor) {
    mMaxRunning = maxRunning;
    mRunning = new Semaphore(maxRunning);
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
  }

  synchronized void addPathLoaderTask(PathLoaderTask task) {
    long id = task.getTaskInfo().getId();
    task.runOnPendingLoad(() -> hasNewLoadTask(id));
    mPathLoaderTasks.put(id, task);
    mPathLoaderTaskQueue.add(id);
    mPathLoaderTasksWithPendingLoads.add(id);
    notify();
  }

  synchronized void hasNewLoadTask(long taskId) {
    if (!mPathLoaderTasksWithPendingLoads.contains(taskId)) {
      mPathLoaderTaskQueue.add(taskId);
      notify();
    }
  }

  private void onLoadError(LoadRequest request, Throwable t) {
    mRunning.release();
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
          mRunning.release();
          result.getTaskInfo().getStats().mProcessStarted.incrementAndGet();
        }, v -> {
          result.getTaskInfo().getStats().mProcessCompleted.incrementAndGet();
          result.onProcessComplete(v);
        }, result::onProcessError);
      } else {
        mRunning.release();
        LOG.debug("Got a load result for id {} with no corresponding"
            + "path loader task", request.getBaseTaskId());
      }
    }
  }

  private void runNextLoadTask() throws InterruptedException {
    mRunning.acquire();
    while (mLoadRequests.isEmpty()) {
      synchronized (this) {
        Long nextId = mPathLoaderTaskQueue.poll();
        if (nextId != null) {
          checkNextLoad(nextId);
        } else {
          wait();
        }
      }
    }

    LoadRequest nxtRequest = mLoadRequests.take();
    PathLoaderTask task = mPathLoaderTasks.get(nxtRequest.getBaseTaskId());
    if (task != null) {
      try (CloseableResource<UfsClient> client = task.getClient()) {
        if (nxtRequest.isFirstLoad()) {
          client.get().performGetStatusAsync(nxtRequest.getLoadPath().getPath(),
              ufsLoadResult -> processLoadResult(nxtRequest, ufsLoadResult),
              t -> onLoadError(nxtRequest, t));
        } else {
          client.get().performListingAsync(nxtRequest.getLoadPath().getPath(),
              nxtRequest.getContinuationToken(), nxtRequest.getTaskInfo().getStartAfter(),
              nxtRequest.getDescendantType(),
              ufsLoadResult -> processLoadResult(nxtRequest, ufsLoadResult),
              t -> onLoadError(nxtRequest, t));
        }
      } catch (Throwable t) {
        onLoadError(nxtRequest, t);
      }
    } else {
      LOG.debug("Got load request {} with task id {} with no corresponding task",
          nxtRequest.getLoadRequestId(), nxtRequest.getLoadRequestId());
      mRunning.release();
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
}
