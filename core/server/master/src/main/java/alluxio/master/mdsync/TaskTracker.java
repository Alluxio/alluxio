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
import alluxio.conf.path.TrieNode;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.UfsSyncPathCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashMap;

/**
 * Tracks metadata sync tasks. The tasks will be submitted by UFS URL by user RPC threads.
 */
public class TaskTracker {
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
   */
  public TaskTracker(
      int executorThreads, int maxUfsRequests,
      boolean allowConcurrentGetStatus, boolean allowConcurrentNonRecursiveList,
      UfsSyncPathCache syncPathCache) {
    mSyncPathCache = syncPathCache;
    mLoadRequestExecutor = new LoadRequestExecutor(maxUfsRequests,
        new LoadResultExecutor(executorThreads));
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

  private synchronized void taskComplete(long taskId, boolean isFile) {
    BaseTask baseTask = mTaskMap.remove(taskId);
    if (baseTask != null) {
      LOG.debug("Task {} completed", baseTask);
      mSyncPathCache.notifySyncedPath(baseTask.getTaskInfo().getBasePath(),
          baseTask.getTaskInfo().getDescendantType(), baseTask.getStartTime(),
          null, isFile);
    } else {
      LOG.debug("Task with id {} completed, but was already removed", taskId);
    }
  }

  private synchronized void taskError(long taskId, Throwable t) {
    BaseTask baseTask = mTaskMap.remove(taskId);
    if (baseTask != null) {
      LOG.debug("Task {} failed with error {}", baseTask, t);
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

  void checkTask(
      AlluxioURI path, DescendantType depth,
      DirectoryLoadType loadByDirectory) {
    BaseTask task;
    synchronized (this) {
      TrieNode<BaseTask> activeTasks;
      switch (depth) {
        case NONE:
          activeTasks = mActiveStatusTasks;
          break;
        case ONE:
          activeTasks = mActiveListTasks;
          break;
        default:
          activeTasks = mActiveRecursiveListTasks;
      }
      task = activeTasks.getLeafChildren(path.getPath())
          .map(TrieNode::getValue).filter(nxt -> nxt.pathIsCovered(path, depth)).findFirst()
          .orElseGet(() -> {
            TrieNode<BaseTask> newNode = activeTasks.insert(path.getPath());
            final long id = mNxtId++;
            BaseTask newTask = BaseTask.create(new TaskInfo(path, depth, loadByDirectory, id),
                mSyncPathCache.recordStartSync(), isFile -> taskComplete(id, isFile), t -> taskError(id, t));
            mTaskMap.put(id, newTask);
            newNode.setValue(newTask);
            mLoadRequestExecutor.addPathLoaderTask(newTask.getLoadTask());
            return newTask;
          });
    }
    task.waitForSync(path);
  }
}
