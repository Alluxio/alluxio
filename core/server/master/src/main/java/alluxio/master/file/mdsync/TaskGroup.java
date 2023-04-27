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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.exception.runtime.DeadlineExceededRuntimeException;
import alluxio.grpc.SyncMetadataTask;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A TaskGroup represents a set of {@link BaseTask} objects.
 */
public class TaskGroup {
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private final BaseTask[] mTasks;
  private final long mGroupId;

  /**
   * Creates a new task group.
   * @param groupId the id for this task group
   * @param tasks the tasks to group
   */
  public TaskGroup(long groupId, BaseTask... tasks) {
    Preconditions.checkState(tasks != null && tasks.length > 0);
    mGroupId = groupId;
    mTasks = tasks;
  }

  /**
   * @return the base task for this group
   */
  public BaseTask getBaseTask() {
    return mTasks[0];
  }

  /**
   * @return a stream of the tasks
   */
  public Stream<BaseTask> getTasks() {
    return Arrays.stream(mTasks);
  }

  /**
   * @return the task count
   */
  public int getTaskCount() {
    return mTasks.length;
  }

  /**
   * @return true if all tasks succeeded
   */
  public boolean allSucceeded() {
    return Arrays.stream(mTasks).allMatch(BaseTask::succeeded);
  }

  /**
   * @return a stream of the tasks in protobuf format
   */
  public Stream<SyncMetadataTask> toProtoTasks() {
    return getTasks().map(BaseTask::toProtoTask);
  }

  /**
   * @return the unique group id for this task
   */
  public long getGroupId() {
    return mGroupId;
  }

  /**
   * Waits for all the tasks to complete or until
   * a timeout occurs. If any tasks fail it will throw the
   * error caused by the failed task.
   * If the wait times-out a {@link DeadlineExceededRuntimeException} is thrown.
   * @param timeoutMs the time in milliseconds to wait for the task
   *                  to complete, or 0 to wait forever
   */
  public void waitAllComplete(long timeoutMs) throws Throwable {
    Stopwatch sw = Stopwatch.createStarted();
    for (BaseTask task : mTasks) {
      task.waitComplete(getRemainingTime(sw, timeoutMs));
    }
  }

  private static long getRemainingTime(
      Stopwatch sw, long timeoutMs) throws DeadlineExceededRuntimeException {
    // Endless wait
    if (timeoutMs == 0) {
      return 0;
    }
    long remaining = timeoutMs - sw.elapsed(TimeUnit.MILLISECONDS);
    if (remaining <= 0) {
      throw new DeadlineExceededRuntimeException("Task still running.");
    }
    return remaining;
  }
}
