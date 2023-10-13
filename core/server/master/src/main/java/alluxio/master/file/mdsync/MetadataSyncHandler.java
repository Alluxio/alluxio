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
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.underfs.UfsLoadResult;

import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;

/**
 * The interactions between different task processing steps is exposed through this
 * standard interface in order to allow changes in the future, for example calling
 * separate components over the network.
 */
public class MetadataSyncHandler {

  private final TaskTracker mTaskTracker;
  @VisibleForTesting
  final DefaultFileSystemMaster mFsMaster;
  private final InodeTree mInodeTree;

  /**
   * Creates a metadata sync kernel.
   * @param taskTracker the task tracker
   * @param fsMaster the file system master
   * @param inodeTree the inode tree
   */
  public MetadataSyncHandler(
      TaskTracker taskTracker, DefaultFileSystemMaster fsMaster, InodeTree inodeTree) {
    mTaskTracker = taskTracker;
    mFsMaster = fsMaster;
    mInodeTree = inodeTree;
  }

  void onLoadRequestError(long taskId, long loadId, Throwable t) {
    mTaskTracker.getActiveTask(taskId).ifPresent(
        task -> task.getPathLoadTask().onLoadRequestError(loadId, t));
  }

  void onFailed(long taskId, Throwable t) {
    mTaskTracker.getActiveTask(taskId).ifPresent(task -> {
      task.onFailed(t);
    });
  }

  void onProcessError(long taskId, Throwable t) {
    mTaskTracker.getActiveTask(taskId).ifPresent(task ->
        task.getPathLoadTask().onProcessError(t));
  }

  void onEachResult(long taskId, SyncProcessResult result) {
    mTaskTracker.getActiveTask(taskId).ifPresent(task -> task.nextCompleted(result));
  }

  void onTaskError(long taskId, Throwable t) {
    mTaskTracker.getActiveTask(taskId).ifPresent(task -> mTaskTracker.taskError(taskId, t));
  }

  void onTaskComplete(long taskId, boolean isFile) {
    mTaskTracker.taskComplete(taskId, isFile);
  }

  void onPathLoadComplete(long taskId, boolean isFile) {
    mTaskTracker.getActiveTask(taskId).ifPresent(
        task -> task.onComplete(isFile, mFsMaster, mInodeTree));
  }

  /**
   * Loads a nested directory.
   * @param taskId the task id
   * @param path the load path
   */
  public void loadNestedDirectory(long taskId, AlluxioURI path) {
    mTaskTracker.getActiveTask(taskId).ifPresent(
        task -> task.getPathLoadTask().loadNestedDirectory(path));
  }

  Optional<LoadResult> onReceiveLoadRequestOutput(long taskId, long loadId, UfsLoadResult result) {
    return mTaskTracker.getActiveTask(taskId).flatMap(task ->
        task.getPathLoadTask().createLoadResult(loadId, result));
  }

  void onProcessComplete(long taskId, long loadRequestId, SyncProcessResult result) {
    mTaskTracker.getActiveTask(taskId).ifPresent(task ->
        task.getPathLoadTask().onProcessComplete(loadRequestId, result));
  }
}
