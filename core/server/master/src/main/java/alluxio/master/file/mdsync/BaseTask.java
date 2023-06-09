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
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.DeadlineExceededRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.grpc.SyncMetadataState;
import alluxio.grpc.SyncMetadataTask;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.journal.JournalContext;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.util.CommonUtils;
import alluxio.util.ExceptionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * This is the overall task for a sync operation.
 */
public abstract class BaseTask implements PathWaiter {
  enum State {
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELED;

    SyncMetadataState toProto() {
      switch (this) {
        case RUNNING:
          return SyncMetadataState.RUNNING;
        case SUCCEEDED:
          return SyncMetadataState.SUCCEEDED;
        case FAILED:
          return SyncMetadataState.FAILED;
        case CANCELED:
          return SyncMetadataState.CANCELED;
        default:
          return SyncMetadataState.UNKNOWN;
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(BaseTask.class);

  private final long mStartTime;
  private volatile Long mFinishTime = null;
  BaseTaskResult mIsCompleted = null;
  private final TaskInfo mTaskInfo;
  private final PathLoaderTask mPathLoadTask;
  private final boolean mRemoveOnComplete;

  /**
   * @return the task state
   */
  public synchronized State getState() {
    if (!isCompleted().isPresent()) {
      return State.RUNNING;
    }
    BaseTaskResult result = isCompleted().get();
    if (result.succeeded()) {
      return State.SUCCEEDED;
    } else if (result.getThrowable().orElse(null) instanceof CancelledException) {
      return State.CANCELED;
    } else {
      return State.FAILED;
    }
  }

  /**
   * @return true if the task is completed
   */
  public synchronized Optional<BaseTaskResult> isCompleted() {
    return Optional.ofNullable(mIsCompleted);
  }

  /**
   * @return if the task is succeeded
   */
  public synchronized boolean succeeded() {
    return mIsCompleted != null && mIsCompleted.succeeded();
  }

  @VisibleForTesting
  PathLoaderTask getPathLoadTask() {
    return mPathLoadTask;
  }

  static BaseTask create(
      TaskInfo info, long startTime,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier,
      boolean removeOnComplete) {
    if (info.getLoadByDirectory() != DirectoryLoadType.SINGLE_LISTING
        && info.getDescendantType() == DescendantType.ALL) {
      return new DirectoryPathWaiter(
          info, startTime, clientSupplier, removeOnComplete);
    } else {
      return new BatchPathWaiter(
          info, startTime, clientSupplier, removeOnComplete);
    }
  }

  static BaseTask create(
      TaskInfo info, long startTime,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier) {
    return create(info, startTime, clientSupplier, true);
  }

  BaseTask(
      TaskInfo info, long startTime,
      Function<AlluxioURI, CloseableResource<UfsClient>> clientSupplier, boolean removeOnComplete) {
    mTaskInfo = info;
    mStartTime = startTime;
    mPathLoadTask = new PathLoaderTask(mTaskInfo, null, clientSupplier);
    mRemoveOnComplete = removeOnComplete;
  }

  /**
   * @return the task info
   */
  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  /**
   * @return true, if the task should be removed on completion, otherwise it will be
   * moved to a completed task cache.
   */
  boolean removeOnComplete() {
    return mRemoveOnComplete;
  }

  /**
   * @return the sync task time in ms
   */
  public synchronized long getStartTime() {
    Preconditions.checkState(mIsCompleted != null,
        "Task must be completed before accessing the start time");
    return mStartTime;
  }

  PathLoaderTask getLoadTask() {
    return mPathLoadTask;
  }

  synchronized void onComplete(
      boolean isFile, DefaultFileSystemMaster fileSystemMaster, InodeTree inodeTree) {
    if (mIsCompleted != null) {
      return;
    }
    updateDirectChildrenLoaded(fileSystemMaster, inodeTree);
    mFinishTime = CommonUtils.getCurrentMs();
    mIsCompleted = new BaseTaskResult(null);
    mTaskInfo.getMdSync().onTaskComplete(mTaskInfo.getId(), isFile);
    notifyAll();
  }

  /**
   * Blocking waits until the task completes.
   * If the task fails, the exception causing the failure is thrown.
   * If the wait times-out a {@link DeadlineExceededRuntimeException} is thrown.
   *
   * @param timeoutMs the timeout in ms, 0 for an endless wait
   */
  public synchronized void waitComplete(long timeoutMs) throws Throwable {
    Stopwatch sw = Stopwatch.createStarted();
    long waitTime = timeoutMs;
    while (mIsCompleted == null && (timeoutMs == 0 || waitTime > 0)) {
      wait(waitTime);
      if (timeoutMs != 0) {
        waitTime = waitTime - sw.elapsed(TimeUnit.MILLISECONDS);
        sw.reset();
      }
    }
    if (mIsCompleted == null) {
      throw new DeadlineExceededRuntimeException("Task still running.");
    }
    if (mIsCompleted.getThrowable().isPresent()) {
      throw mIsCompleted.getThrowable().get();
    }
  }

  synchronized void onFailed(Throwable t) {
    mFinishTime = CommonUtils.getCurrentMs();
    if (mIsCompleted != null) {
      return;
    }
    mIsCompleted = new BaseTaskResult(t);
    LOG.warn("Task {} failed with error", mTaskInfo, t);
    cancel();
    mTaskInfo.getMdSync().onTaskError(mTaskInfo.getId(), t);
  }

  synchronized long cancel() {
    mFinishTime = CommonUtils.getCurrentMs();
    if (mIsCompleted == null) {
      mIsCompleted = new BaseTaskResult(new CancelledException("Task was cancelled"));
    }
    mPathLoadTask.cancel();
    notifyAll();
    return mTaskInfo.getId();
  }

  boolean pathIsCovered(AlluxioURI path, DescendantType depth) {
    switch (mTaskInfo.getDescendantType()) {
      case NONE:
        return depth == DescendantType.NONE && mTaskInfo.getBasePath().equals(path);
      case ONE:
        return (depth != DescendantType.ALL && mTaskInfo.getBasePath().equals(path))
            || (depth == DescendantType.NONE && mTaskInfo.getBasePath().equals(path.getParent()));
      case ALL:
        try {
          return mTaskInfo.getBasePath().isAncestorOf(path);
        } catch (InvalidPathException e) {
          throw new InternalRuntimeException(e);
        }
      default:
        throw new InternalRuntimeException(String.format(
            "Unknown descendant type %s", mTaskInfo.getDescendantType()));
    }
  }

  /**
   * @return the sync duration in ms
   */
  public long getSyncDuration() {
    final Long finishTime = mFinishTime;
    if (finishTime == null) {
      return CommonUtils.getCurrentMs() - mStartTime;
    }
    return mFinishTime - mStartTime;
  }

  /**
   * @return the sync metadata task in proto
   */
  public synchronized SyncMetadataTask toProtoTask() {
    SyncMetadataTask.Builder builder = SyncMetadataTask.newBuilder();
    builder.setId(getTaskInfo().getId());
    builder.setState(getState().toProto());
    builder.setSyncDurationMs(getSyncDuration());
    Throwable t = null;
    if (mIsCompleted != null && mIsCompleted.getThrowable().isPresent()) {
      t = mIsCompleted.getThrowable().get();
    }
    if (t != null && getState() != State.CANCELED) {
      builder.setException(SyncMetadataTask.Exception.newBuilder()
          .setExceptionType(t.getClass().getTypeName())
          .setExceptionMessage(t.getMessage() == null ? "" : t.getMessage())
          .setStacktrace(ExceptionUtils.asPlainText(t)));
    }
    builder.setTaskInfoString(getTaskInfo().toString());
    Pair<Long, String> statReport = getTaskInfo().getStats().toReportString();
    builder.setSuccessOpCount(statReport.getFirst());
    builder.setTaskStatString(statReport.getSecond());
    return builder.build();
  }

  /**
   * Updates direct children loaded for directories affected by the metadata sync.
   * @param fileSystemMaster the file system master
   * @param inodeTree the inode tree
   */
  public void updateDirectChildrenLoaded(
      DefaultFileSystemMaster fileSystemMaster, InodeTree inodeTree) {
    try (JournalContext journalContext = fileSystemMaster.createJournalContext()) {
      getTaskInfo().getPathsToUpdateDirectChildrenLoaded().forEach(
          uri -> {
            try (LockedInodePath lockedInodePath =
                     inodeTree.lockInodePath(
                         uri, InodeTree.LockPattern.WRITE_INODE,
                         journalContext)) {
              if (lockedInodePath.fullPathExists() && lockedInodePath.getInode().isDirectory()
                  && !lockedInodePath.getInode().asDirectory().isDirectChildrenLoaded()) {
                inodeTree.setDirectChildrenLoaded(
                    () -> journalContext,
                    lockedInodePath.getInode().asDirectory());
              }
            } catch (FileDoesNotExistException | InvalidPathException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (UnavailableException e) {
      throw new RuntimeException(e);
    }
  }
}
