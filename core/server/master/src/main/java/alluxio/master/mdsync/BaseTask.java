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
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.status.CancelledException;
import alluxio.file.options.DescendantType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * This is the overall task for a sync operation.
 */
abstract class BaseTask implements PathWaiter {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTask.class);

  private final long mStartTime;
  BaseTaskResult mIsCompleted = null;
  private final TaskInfo mTaskInfo;
  private final PathLoaderTask mPathLoadTask;
  private final Consumer<Boolean> mOnComplete;
  private final Consumer<Throwable> mOnError;

  @VisibleForTesting
  Optional<BaseTaskResult> isCompleted() {
    return Optional.ofNullable(mIsCompleted);
  }

  @VisibleForTesting
  PathLoaderTask getPathLoadTask() {
    return mPathLoadTask;
  }

  static BaseTask create(
      TaskInfo info, long startTime, Consumer<Boolean> onComplete,
      Consumer<Throwable> onError) {
    if (info.getLoadByDirectory() != DirectoryLoadType.NONE
        && info.getDescendantType() == DescendantType.ALL) {
      return new DirectoryPathWaiter(info, startTime, onComplete, onError);
    } else {
      return new BatchPathWaiter(info, startTime, onComplete, onError);
    }
  }

  BaseTask(
      TaskInfo info, long startTime, Consumer<Boolean> onComplete,
      Consumer<Throwable> onError) {
    mTaskInfo = info;
    mStartTime = startTime;
    mPathLoadTask = new PathLoaderTask(mTaskInfo, null,
        this::nextCompleted, this::onComplete, this::onLoadError);
    mOnComplete = onComplete;
    mOnError = onError;
  }

  public TaskInfo getTaskInfo() {
    return mTaskInfo;
  }

  public long getStartTime() {
    Preconditions.checkState(mIsCompleted != null,
        "Task must be completed before accessing the start time");
    return mStartTime;
  }

  PathLoaderTask getLoadTask() {
    return mPathLoadTask;
  }

  private synchronized void onComplete(boolean isFile) {
    mIsCompleted = new BaseTaskResult(null);
    mOnComplete.accept(isFile);
    notifyAll();
  }

  private synchronized void onLoadError(Throwable t) {
    LOG.warn("Task {} failed with load error", mTaskInfo, t);
    mOnError.accept(t);
    cancel();
  }

  synchronized long cancel() {
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
}
