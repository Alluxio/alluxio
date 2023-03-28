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
import alluxio.collections.Pair;
import alluxio.conf.path.TrieNode;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.UfsSyncPathCache;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

/**
 * Tracks metadata sync tasks. The tasks will be submitted by UFS URL by user RPC threads.
 */
public class TaskTracker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskTracker.class);

  private final TrieNode<BaseTask> mActiveRecursiveListTasks;
  private final TrieNode<BaseTask> mActiveListTasks;
  private final TrieNode<BaseTask> mActiveStatusTasks;
  private final HashMap<Long, BaseTask> mTaskMap = new HashMap<>();
  private final LoadRequestExecutor mLoadRequestExecutor;
  private final UfsSyncPathCache mSyncPathCache;

  private long mNxtId = 0;

  /**
   * Create a new TaskTracker.
   * @param executorThreads the number of threads to run the metadata sync processing
   * @param maxUfsRequests the maximum number of concurrently running
   *                       (or completed but not yet processed) Ufs requests
   * @param allowConcurrentNonRecursiveList if true, non-recursive lists tasks will
   *                                        run concurrently with recursive list tasks
   * @param allowConcurrentGetStatus if true, getStatus tasks will run concurrently
   *                                 with recursive list tasks
   * @param syncPathCache the sync path cache
   * @param syncProcess the sync process
   */
  public TaskTracker(
      int executorThreads, int maxUfsRequests,
      boolean allowConcurrentGetStatus, boolean allowConcurrentNonRecursiveList,
      UfsSyncPathCache syncPathCache, SyncProcess syncProcess) {
    mSyncPathCache = syncPathCache;
    mLoadRequestExecutor = new LoadRequestExecutor(maxUfsRequests,
        new LoadResultExecutor(syncProcess, executorThreads, syncPathCache));
    mActiveRecursiveListTasks = new TrieNode<>();
    if (allowConcurrentNonRecursiveList) {
      mActiveListTasks = new TrieNode<>();
    } else {
      mActiveListTasks = mActiveRecursiveListTasks;
    }
    if (allowConcurrentGetStatus) {
      mActiveStatusTasks = new TrieNode<>();
    } else {
      mActiveStatusTasks = mActiveRecursiveListTasks;
    }
  }

  synchronized Optional<BaseTask> getTask(long taskId) {
    return Optional.ofNullable(mTaskMap.get(taskId));
  }

  synchronized boolean hasRunningTasks() {
    return mActiveListTasks.getCommonRoots().hasNext()
        || mActiveStatusTasks.getCommonRoots().hasNext()
        || mActiveRecursiveListTasks.getCommonRoots().hasNext();
  }

  synchronized void taskComplete(long taskId, boolean isFile) {
    BaseTask baseTask = mTaskMap.remove(taskId);
    if (baseTask != null) {
      LOG.debug("Task {} completed", baseTask);
      mSyncPathCache.notifySyncedPath(baseTask.getTaskInfo().getBasePath(),
          baseTask.getTaskInfo().getDescendantType(), baseTask.getStartTime(),
          null, isFile);
      TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(
          baseTask.getTaskInfo().getDescendantType());
      Preconditions.checkNotNull(activeTasks.deleteIf(
              baseTask.getTaskInfo().getBasePath().getPath(), a -> true),
          "task missing");
    } else {
      LOG.debug("Task with id {} completed, but was already removed", taskId);
    }
  }

  synchronized void taskError(long taskId, Throwable t) {
    BaseTask baseTask = mTaskMap.remove(taskId);
    if (baseTask != null) {
      LOG.debug("Task {} failed with error {}", baseTask, t);
      TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(
          baseTask.getTaskInfo().getDescendantType());
      Preconditions.checkNotNull(activeTasks.deleteIf(
              baseTask.getTaskInfo().getBasePath().getPath(), a -> true),
          "task missing");
    } else {
      LOG.debug("Task with id {} failed with error, but was already removed", taskId, t);
    }
  }

  synchronized void cancelTasksUnderPath(AlluxioURI path) {
    mActiveRecursiveListTasks.getLeafChildren(path.getPath()).forEach(nxt ->
        mTaskMap.remove(nxt.getValue().cancel()));
    mActiveListTasks.getLeafChildren(path.getPath()).forEach(nxt ->
        mTaskMap.remove(nxt.getValue().cancel()));
    mActiveStatusTasks.getLeafChildren(path.getPath()).forEach(nxt ->
        mTaskMap.remove(nxt.getValue().cancel()));
  }

  private TrieNode<BaseTask> getActiveTasksForDescendantType(DescendantType depth) {
    switch (depth) {
      case NONE:
        return mActiveStatusTasks;
      case ONE:
        return mActiveListTasks;
      default:
        return mActiveRecursiveListTasks;
    }
  }

  Pair<Boolean, BaseTask> checkTask(
      MdSync mdSync,
      AlluxioURI path, DescendantType depth, long syncInterval,
      DirectoryLoadType loadByDirectory) {
    BaseTask task;
    synchronized (this) {
      TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(depth);
      task = activeTasks.getLeafChildren(path.getPath())
          .map(TrieNode::getValue).filter(nxt -> nxt.pathIsCovered(path, depth)).findFirst()
          .orElseGet(() -> {
            TrieNode<BaseTask> newNode = activeTasks.insert(path.getPath());
            final long id = mNxtId++;
            BaseTask newTask = BaseTask.create(
                new TaskInfo(mdSync, path, depth, syncInterval, loadByDirectory, id),
                mSyncPathCache.recordStartSync());
            mTaskMap.put(id, newTask);
            newNode.setValue(newTask);
            mLoadRequestExecutor.addPathLoaderTask(newTask.getLoadTask());
            return newTask;
          });
    }
    return new Pair<>(task.waitForSync(path), task);
  }

  @Override
  public void close() throws IOException {
    mLoadRequestExecutor.close();
  }
}
