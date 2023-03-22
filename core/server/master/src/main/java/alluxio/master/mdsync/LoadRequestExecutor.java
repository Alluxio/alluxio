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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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
  private final ArrayDeque<Long> mPathLoaderTaskQueue = new ArrayDeque<>();
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
          break;
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
  }

  synchronized void hasNewLoadTask(long taskId) {
    if (!mPathLoaderTasksWithPendingLoads.contains(taskId)) {
      mPathLoaderTaskQueue.add(taskId);
      notify();
    }
  }

  @Override
  public synchronized void close() {
    mExecutor.interrupt();
    try {
      mExecutor.join(5_000);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for load request runner to terminate");
    }
  }

  private synchronized void onLoadError(LoadRequest request, Throwable t) {
    request.onError(t);
  }

  private synchronized void processLoadResult(LoadRequest request, UfsLoadResult ufsLoadResult) {
    Optional<LoadResult> loadResult = request.onComplete(ufsLoadResult);
    PathLoaderTask task = mPathLoaderTasks.get(request.getLoadTaskId());
    if (task != null) {
      loadResult.ifPresent(result ->
          mResultExecutor.processLoadResult(result, mRunning::release,
              result::onComplete, result::onProcessError));
    } else {
      LOG.debug("Got a load result for id {} with no corresponding"
          + "path loader task", request.getLoadTaskId());
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
    PathLoaderTask task = mPathLoaderTasks.get(nxtRequest.getLoadTaskId());
    if (task != null) {
      task.getClient().performQueryAsync(nxtRequest.getLoadPath().getPath(),
          nxtRequest.getContinuationToken(), nxtRequest.getDescendantType(),
          ufsLoadResult -> processLoadResult(nxtRequest, ufsLoadResult),
          t -> onLoadError(nxtRequest, t));
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
}
