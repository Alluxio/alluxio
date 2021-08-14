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

package alluxio.master.job.plan;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.plan.PlanConfig;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.wire.Status;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.workflow.WorkflowTracker;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

/**
 * The {@link PlanTracker} is used to create, remove, and provide access to the set of currently
 * scheduled or finished jobs.
 *
 * All modification of the status of jobs should occur within this class,
 */
@ThreadSafe
public class PlanTracker {
  private static final Logger LOG = LoggerFactory.getLogger(PlanTracker.class);

  /**
   * The maximum amount of jobs that will get purged when removing jobs from the queue.
   *
   * By default this value will be -1 (unlimited), however situations may arise where it is
   * desirable to cap the amount of jobs removed. A scenario where the max capacity is large
   * (10M+) could cause an RPC to the job master to time out due to significant time to remove a
   * large number of jobs. While one RPC of 10M may seem insignificant, providing guarantees that
   * RPCs won't fail on long-running production clusters is probably something that we want to
   * provide.
   *
   * One caveat to setting this value is that there will be a lower bound on the amount of
   * memory that the job master will use when it is at capacity. This may or may not be a
   * reasonable trade-off to guarantee that RPCs don't time out due to job eviction.
   * @see PropertyKey#JOB_MASTER_FINISHED_JOB_PURGE_COUNT
   */
  private final long mMaxJobPurgeCount;

  /** The maximum amount of jobs that can be tracked at any one time. */
  private final long mCapacity;

  /** The minimum amount of time that finished jobs should be retained for. */
  private final long mRetentionMs;

  /** The main index to track jobs through their Job Id. */
  private final ConcurrentHashMap<Long, PlanCoordinator> mCoordinators;

  private final SortedSet<PlanInfo> mFailed;

  /** A FIFO queue used to track jobs which have status {@link Status#isFinished()} as true. */
  private final LinkedBlockingQueue<PlanInfo> mFinished;

  private final WorkflowTracker mWorkflowTracker;

  /**
   * Create a new instance of {@link PlanTracker}.
   *
   * @param capacity the capacity of jobs that can be handled
   * @param retentionMs the minimum amount of time to retain jobs
   * @param maxJobPurgeCount the max amount of jobs to purge when reaching max capacity
   * @param workflowTracker the workflow tracker instance
   */
  public PlanTracker(long capacity, long retentionMs,
                     long maxJobPurgeCount, WorkflowTracker workflowTracker) {
    Preconditions.checkArgument(capacity >= 0 && capacity <= Integer.MAX_VALUE);
    mCapacity = capacity;
    Preconditions.checkArgument(retentionMs >= 0);
    mRetentionMs = retentionMs;
    mMaxJobPurgeCount = maxJobPurgeCount <= 0 ? Long.MAX_VALUE : maxJobPurgeCount;
    mCoordinators = new ConcurrentHashMap<>(0,
        0.95f, ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM));
    mFailed = Collections.synchronizedSortedSet(new TreeSet<>((left, right) -> {
      long diffTime = right.getLastStatusChangeMs() - left.getLastStatusChangeMs();
      if (diffTime != 0) {
        return Long.signum(diffTime);
      }
      return Long.signum(right.getId() - left.getId());
    }));
    mFinished = new LinkedBlockingQueue<>();
    mWorkflowTracker = workflowTracker;
  }

  private void statusChangeCallback(PlanInfo planInfo) {
    if (planInfo == null) {
      return;
    }
    mWorkflowTracker.onPlanStatusChange(planInfo);
    Status status = planInfo.getStatus();
    if (!status.isFinished()) {
      return;
    }

    if (status.equals(Status.FAILED)) {
      while (mFailed.size() >= mCapacity) {
        mFailed.remove(mFailed.last());
      }
      mFailed.add(planInfo);
    }

    // Retry if offering to mFinished doesn't work
    for (int i = 0; i < 2; i++) {
      if (mFinished.offer(planInfo)) {
        return;
      }
      if (!removeFinished()) {
        // Still couldn't add to mFinished - remove from coordinators so that it doesn't
        // get stuck
        LOG.warn("Failed to remove any jobs from the finished queue in status change callback");
      }
    }
    if (mFinished.offer(planInfo)) {
      return;
    }
    //remove from the coordinator map preemptively so that it doesn't get lost forever even if
    // it's still within the retention time
    LOG.warn("Failed to offer job id {} to finished queue, removing from tracking preemptively",
        planInfo.getId());
  }

  /**
   * Gets a {@link PlanCoordinator} associated with the given job Id.
   *
   * @param jobId the job id associated with the {@link PlanCoordinator}
   * @return The {@link PlanCoordinator} associated with the id, or null if there is no association
   */
  @Nullable
  public PlanCoordinator getCoordinator(long jobId) {
    return mCoordinators.get(jobId);
  }

  /**
   * Adds a job with the given {@link JobConfig} to the job tracker.
   *
   * @param jobConfig configuration for the job
   * @param manager command manager for jobs
   * @param ctx the {@link JobServerContext} from the job master
   * @param workers a list of available workers
   * @param jobId job id of the newly added job
   * @throws JobDoesNotExistException   if the job type does not exist
   * @throws ResourceExhaustedException if there is no more space available in the job master
   */
  public synchronized void run(PlanConfig jobConfig, CommandManager manager,
      JobServerContext ctx, List<WorkerInfo> workers, long jobId) throws
      JobDoesNotExistException, ResourceExhaustedException {
    if (removeFinished()) {
      PlanCoordinator planCoordinator = PlanCoordinator.create(manager, ctx,
          workers, jobId, jobConfig, this::statusChangeCallback);
      mCoordinators.put(jobId, planCoordinator);
    } else {
      throw new ResourceExhaustedException(
          ExceptionMessage.JOB_MASTER_FULL_CAPACITY.getMessage(mCapacity));
    }
  }

  /**
   * Removes all finished jobs outside of the retention time from the queue.
   *
   * @return true if at least one job was removed, or if not at maximum capacity yet, false if at
   *         capacity and no job was removed
   */
  private synchronized boolean removeFinished() {
    boolean removedJob = false;
    boolean isFull = mCoordinators.size() >= mCapacity;
    if (!isFull) {
      return true;
    }
    // coordinators at max capacity
    // Try to clear the queue
    if (mFinished.isEmpty()) {
      // The job master is at full capacity and no job has finished.
      return false;
    }

    ArrayList<Long> removedJobIds = Lists.newArrayList();
    int removeCount = 0;
    while (!mFinished.isEmpty() && removeCount < mMaxJobPurgeCount) {
      PlanInfo oldestJob = mFinished.peek();
      if (oldestJob == null) { // no items to remove
        break;
      }
      long timeSinceCompletion = CommonUtils.getCurrentMs() - oldestJob.getLastStatusChangeMs();
      // Once inserted into mFinished, the status of a job should not change - so the peek()
      // /poll() methods guarantee to some extent that the job at the top of the queue is one
      // of the oldest jobs. Thus, if it is still within retention time here, we likely can't
      // remove anything else from the queue. Though it should be noted that it is not strictly
      // guaranteed that the job at the top of is the oldest.
      if (timeSinceCompletion < mRetentionMs) {
        break;
      }

      // Remove the top item since we know it's old enough now.
      // Assumes there are no concurrent poll() operations taking place between here and the
      // first peek()
      if (mFinished.poll() == null) {
        // This should not happen because peek() returned an element
        // there should be no other concurrent operations that remove from mFinished
        LOG.warn("Polling the queue resulted in a null element");
        break;
      }

      long oldestJobId = oldestJob.getId();
      removedJobIds.add(oldestJobId);
      // Don't remove from the plan coordinator yet because WorkflowTracker may need these job info
      if (mCoordinators.get(oldestJobId) == null) {
        LOG.warn("Did not find a coordinator with id {}", oldestJobId);
      } else {
        removedJob = true;
        removeCount++;
      }
    }

    mWorkflowTracker.cleanup(removedJobIds);

    // Remove from the plan coordinator
    for (long removedJobId : removedJobIds) {
      mCoordinators.remove(removedJobId);
    }

    return removedJob;
  }

  /**
   * A collection of all job Ids currently tracked in the job master. Jobs may be in a finished
   * state.
   *
   * @return An unmodifiable collection of all tracked Job Ids
   */
  public Collection<Long> list() {
    return Collections.unmodifiableCollection(mCoordinators.keySet());
  }

  /**
   * A collection of all {@link PlanCoordinator} currently tracked by the job master. May contain
   * coordinators for jobs which have finished.
   *
   * @return An unmodifiable collection of all tracked {@link PlanCoordinator}
   */
  public Collection<PlanCoordinator> coordinators() {
    return Collections.unmodifiableCollection(mCoordinators.values());
  }

  /**
   * @return list of failed plans
   */
  public Stream<PlanInfo> failed() {
    return mFailed.stream();
  }

  /**
   * @param name job name filter
   * @param statusList status list filter
   * @return job ids matching conditions
   */
  public Set<Long> findJobs(String name, List<Status> statusList) {
    return mCoordinators.entrySet().stream()
        .filter(x ->
            statusList.isEmpty()
                || statusList.contains(x.getValue().getPlanInfoWire(false).getStatus())
                && (name == null || name.isEmpty()
                || x.getValue().getPlanInfoWire(false).getName().equals(name)))
        .map(Map.Entry::getKey).collect(Collectors.toSet());
  }
}
