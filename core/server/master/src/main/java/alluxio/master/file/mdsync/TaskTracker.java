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

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.path.TrieNode;
import alluxio.exception.status.NotFoundException;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.grpc.SyncMetadataTask;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Tracks metadata sync tasks. The tasks will be submitted by UFS URL by user RPC threads.
 */
public class TaskTracker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TaskTracker.class);

  private final TrieNode<BaseTask> mActiveRecursiveListTasks;
  private final TrieNode<BaseTask> mActiveListTasks;
  private final TrieNode<BaseTask> mActiveStatusTasks;
  private final HashMap<Long, BaseTask> mActiveTaskMap = new HashMap<>();
  // TODO(elega) make this a configurable property
  private final Cache<Long, SyncMetadataTask> mFinishedTaskMap =
      CacheBuilder.newBuilder().maximumSize(1000).build();
  private final LoadRequestExecutor mLoadRequestExecutor;
  private final UfsSyncPathCache mSyncPathCache;
  private final UfsAbsentPathCache mAbsentPathCache;
  private final Function<AlluxioURI, CloseableResource<UfsClient>> mClientSupplier;

  public static final Counter COMPLETED_TASK_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_COMPLETED_TASKS.getName());
  public static final Counter FAILED_TASK_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_FAILED_TASKS.getName());
  public static final Counter CANCELLED_TASK_COUNT
      = MetricsSystem.counter(MetricKey.MASTER_METADATA_SYNC_CANCELLED_TASKS.getName());

  private long mNxtId = 0;

  /**
   * Create a new TaskTracker.
   * @param executorThreads the number of threads to run the metadata sync processing
   * @param maxUfsRequests the maximum number of concurrently running
   *  (or completed but not yet processed) Ufs requests
   * @param allowConcurrentNonRecursiveList if true, non-recursive lists tasks will
   *  run concurrently with recursive list tasks
   * @param allowConcurrentGetStatus if true, getStatus tasks will run concurrently
   *  with recursive list tasks
   * @param syncPathCache the sync path cache
   * @param absentPathCache the absent cache
   * @param syncProcess the sync process
   * @param clientSupplier the client supplier
   */
  public TaskTracker(
      int executorThreads, int maxUfsRequests,
      boolean allowConcurrentGetStatus, boolean allowConcurrentNonRecursiveList,
      UfsSyncPathCache syncPathCache,
      UfsAbsentPathCache absentPathCache,
      SyncProcess syncProcess,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier) {
    LOG.info("Metadata sync executor threads {}, max concurrent ufs requests {}",
        executorThreads, maxUfsRequests);
    mSyncPathCache = syncPathCache;
    mAbsentPathCache = absentPathCache;
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
    mClientSupplier = clientSupplier;
    registerMetrics();
  }

  /**
   * @param taskId the task id
   * @return the task
   */
  public synchronized Optional<BaseTask> getActiveTask(long taskId) {
    return Optional.ofNullable(mActiveTaskMap.get(taskId));
  }

  /**
   * @param taskId the task id
   * @return the task
   */
  public synchronized Optional<SyncMetadataTask> getTaskProto(long taskId) {
    BaseTask task = mActiveTaskMap.get(taskId);
    if (task != null) {
      return Optional.of(task.toProtoTask());
    }
    return Optional.ofNullable(mFinishedTaskMap.getIfPresent(taskId));
  }

  synchronized boolean hasRunningTasks() {
    return mActiveListTasks.getCommonRoots().hasNext()
        || mActiveStatusTasks.getCommonRoots().hasNext()
        || mActiveRecursiveListTasks.getCommonRoots().hasNext();
  }

  void taskComplete(long taskId, boolean isFile) {
    synchronized (this) {
      BaseTask baseTask = mActiveTaskMap.get(taskId);
      if (baseTask != null) {
        if (!baseTask.removeOnComplete()) {
          mFinishedTaskMap.put(taskId, baseTask.toProtoTask());
        }
        COMPLETED_TASK_COUNT.inc();
        mActiveTaskMap.remove(taskId);
        LOG.debug("Task {} completed", baseTask);
        mSyncPathCache.notifySyncedPath(baseTask.getTaskInfo().getBasePath(),
            baseTask.getTaskInfo().getDescendantType(), baseTask.getStartTime(),
            null, isFile);
        if (baseTask.getTaskInfo().getStats().getStatusCount() == 0) {
          mAbsentPathCache.addSinglePath(baseTask.getTaskInfo().getBasePath());
        } else {
          mAbsentPathCache.processExisting(baseTask.getTaskInfo().getBasePath());
        }
        TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(
            baseTask.getTaskInfo().getDescendantType());
        Preconditions.checkNotNull(activeTasks.deleteIf(
                baseTask.getTaskInfo().getBasePath().toString(), a -> true),
            "task missing").setValue(null);
      } else {
        LOG.debug("Task with id {} completed, but was already removed", taskId);
      }
    }
    mLoadRequestExecutor.onTaskComplete(taskId);
  }

  void taskError(long taskId, Throwable t) {
    synchronized (this) {
      BaseTask baseTask = mActiveTaskMap.remove(taskId);
      if (baseTask != null) {
        FAILED_TASK_COUNT.inc();
        LOG.debug("Task {} failed with error {}", baseTask, t);
        TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(
            baseTask.getTaskInfo().getDescendantType());
        Preconditions.checkNotNull(activeTasks.deleteIf(
                baseTask.getTaskInfo().getBasePath().toString(), a -> true),
            "task missing").setValue(null);
        if (!baseTask.removeOnComplete()) {
          mFinishedTaskMap.put(taskId, baseTask.toProtoTask());
        }
      } else {
        LOG.debug("Task with id {} failed with error, but was already removed", taskId, t);
      }
    }
    mLoadRequestExecutor.onTaskComplete(taskId);
  }

  synchronized void cancelTasksUnderPath(AlluxioURI path) {
    mActiveRecursiveListTasks.getLeafChildren(path.toString()).forEach(nxt ->
        mActiveTaskMap.remove(nxt.getValue().cancel()));
    mActiveListTasks.getLeafChildren(path.toString()).forEach(nxt ->
        mActiveTaskMap.remove(nxt.getValue().cancel()));
    mActiveStatusTasks.getLeafChildren(path.toString()).forEach(nxt ->
        mActiveTaskMap.remove(nxt.getValue().cancel()));
  }

  /**
   * Cancels an ongoing sync task.
   * @param taskId the task id
   */
  public synchronized void cancelTaskById(long taskId) throws NotFoundException {
    BaseTask baseTask = mActiveTaskMap.get(taskId);
    if (baseTask == null) {
      throw new NotFoundException("Task " + taskId + " not found or has already been canceled.");
    }
    if (baseTask.isCompleted().isPresent()) {
      return;
    }
    if (!baseTask.removeOnComplete()) {
      mFinishedTaskMap.put(taskId, baseTask.toProtoTask());
    }
    CANCELLED_TASK_COUNT.inc();
    mActiveTaskMap.remove(taskId);
    baseTask.cancel();
    TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(
        baseTask.getTaskInfo().getDescendantType());
    Preconditions.checkNotNull(activeTasks.deleteIf(
            baseTask.getTaskInfo().getBasePath().toString(), a -> true), "task missing")
        .setValue(null);
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

  /**
   * Launches a metadata sync task asynchronously with the given parameters.
   * This function should be used when manually launching metadata sync tasks.
   * @param metadataSyncHandler the MdSync object
   * @param ufsPath the ufsPath to sync
   * @param alluxioPath the alluxio path matching the mounted ufsPath
   * @param startAfter if the sync should start after a given internal path
   * @param depth the depth of descendents to load
   * @param syncInterval the sync interval
   * @param loadByDirectory the load by directory type
   * @param removeOnComplete if the task should be removed on complete
   * @return the running task object
   */
  public BaseTask launchTaskAsync(
      MetadataSyncHandler metadataSyncHandler,
      AlluxioURI ufsPath, AlluxioURI alluxioPath,
      @Nullable String startAfter,
      DescendantType depth, long syncInterval,
      DirectoryLoadType loadByDirectory,
      boolean removeOnComplete) {
    BaseTask task;
    synchronized (this) {
      TrieNode<BaseTask> activeTasks = getActiveTasksForDescendantType(depth);
      task = activeTasks.getLeafChildren(ufsPath.toString())
          .map(TrieNode::getValue).filter(nxt -> nxt.pathIsCovered(ufsPath, depth)).findFirst()
          .orElseGet(() -> {
            TrieNode<BaseTask> newNode = activeTasks.insert(ufsPath.toString());
            Preconditions.checkState(newNode.getValue() == null);
            final long id = mNxtId++;
            BaseTask newTask = BaseTask.create(
                new TaskInfo(metadataSyncHandler, ufsPath, alluxioPath, startAfter,
                    depth, syncInterval, loadByDirectory, id),
                mSyncPathCache.recordStartSync(),
                mClientSupplier,
                removeOnComplete);
            mActiveTaskMap.put(id, newTask);
            newNode.setValue(newTask);
            mLoadRequestExecutor.addPathLoaderTask(newTask.getLoadTask());
            return newTask;
          });
    }
    return task;
  }

  /**
   * Launches a metadata sync task with the given parameters.
   * This function should be used when traversing the tree, and the
   * path being traversed is needing a sync.
   * This method will not return until the initial sync path has been
   * synchronized. For example if the alluxio sync path is "/mount/file"
   * it will not return until "file" has been synchronized. If instead
   * the path being synchronized is a directory, e.g. "/mount/directory/"
   * then the function will return as soon as the first batch of items
   * in the directory has been synchronized, e.g. "/mount/directory/first",
   * allowing the user to start listing the file before the sync has been
   * completed entirely. As the directory is traversed, this function should
   * be called on each subsequent path until the sync is complete.
   * TODO(tcrain) integrate this in the filesystem operations traversal
   * @param metadataSyncHandler the MdSync object
   * @param ufsPath the ufsPath to sync
   * @param alluxioPath the alluxio path matching the mounted ufsPath
   * @param startAfter if the sync should start after a given internal path
   * @param depth the depth of descendents to load
   * @param syncInterval the sync interval
   * @param loadByDirectory the load by directory type
   * @return the running task object
   */
  @VisibleForTesting
  public Pair<Boolean, BaseTask> checkTask(
      MetadataSyncHandler metadataSyncHandler,
      AlluxioURI ufsPath, AlluxioURI alluxioPath,
      @Nullable String startAfter,
      DescendantType depth, long syncInterval,
      DirectoryLoadType loadByDirectory) {
    // TODO(elega/tcrain) This method needs to be updated to support nested sync
    BaseTask task = launchTaskAsync(metadataSyncHandler, ufsPath, alluxioPath, startAfter,
        depth, syncInterval, loadByDirectory, true);
    return new Pair<>(task.waitForSync(ufsPath), task);
  }

  @Override
  public void close() throws IOException {
    mLoadRequestExecutor.close();
  }

  private void registerMetrics() {
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(
            MetricKey.MASTER_METADATA_SYNC_RUNNING_TASKS.getName()),
        () -> {
          synchronized (this) {
            return mActiveTaskMap.size();
          }
        });
  }
}
