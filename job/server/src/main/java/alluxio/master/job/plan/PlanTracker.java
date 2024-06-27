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

import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.plan.PlanConfig;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.plan.replicate.SetReplicaConfig;
import alluxio.job.wire.Status;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.workflow.WorkflowTracker;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The {@link PlanTracker} is used to create, remove, and provide access to the set of currently
 * scheduled or finished jobs.
 *
 * All modification of the status of jobs should occur within this class,
 */
@ThreadSafe
public class PlanTracker {
  private static final Logger LOG = LoggerFactory.getLogger(PlanTracker.class);

  /** The maximum amount of jobs that can be tracked at any one time. */
  private final long mCapacity;

  /** The main index to track jobs through their Job Id. */
  private final ConcurrentHashMap<Long, PlanCoordinator> mCoordinators;

  private final SortedSet<PlanInfo> mFailed;

  private final WorkflowTracker mWorkflowTracker;

  /**
   * Create a new instance of {@link PlanTracker}.
   *
   * @param capacity the capacity of jobs that can be handled
   * @param workflowTracker the workflow tracker instance
   */
  public PlanTracker(long capacity, WorkflowTracker workflowTracker) {
    Preconditions.checkArgument(capacity >= 0 && capacity <= Integer.MAX_VALUE);
    mCapacity = capacity;
    mCoordinators = new ConcurrentHashMap<>(0, 0.95f,
        Math.max(8, 2 * Runtime.getRuntime().availableProcessors()));
    mFailed = Collections.synchronizedSortedSet(new TreeSet<>((left, right) -> {
      long diffTime = right.getLastStatusChangeMs() - left.getLastStatusChangeMs();
      if (diffTime != 0) {
        return Long.signum(diffTime);
      }
      return Long.signum(right.getId() - left.getId());
    }));
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
    checkActiveSetReplicaJobs(jobConfig);
    if (mCoordinators.size() < mCapacity) {
      PlanCoordinator planCoordinator = PlanCoordinator.create(manager, ctx,
              workers, jobId, jobConfig, this::statusChangeCallback);
      mCoordinators.put(jobId, planCoordinator);
    } else {
      throw new ResourceExhaustedException(
              ExceptionMessage.JOB_MASTER_FULL_CAPACITY.getMessage(mCapacity));
    }
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

  /**
   * Remove expired jobs in PlanTracker.
   * @param jobIds the list of removed jobId
   */
  public void removeJobs(List<Long> jobIds) {
    mWorkflowTracker.cleanup(jobIds);
    for (Long jobId : jobIds) {
      PlanInfo removedPlanInfo = mCoordinators.get(jobId).getPlanInfo();
      mCoordinators.remove(jobId);
      if (Status.FAILED.equals(removedPlanInfo.getStatus())) {
        mFailed.remove(removedPlanInfo);
      }
    }
  }

  private void checkActiveSetReplicaJobs(JobConfig jobConfig) throws JobDoesNotExistException {
    if (jobConfig instanceof SetReplicaConfig) {
      Set<Pair<String, Long>> activeJobs = mCoordinators.values().stream()
          .filter(x -> x.getPlanInfo().getJobConfig() instanceof SetReplicaConfig)
          .map(x -> ((SetReplicaConfig) x.getPlanInfo().getJobConfig()))
          .map(x -> new Pair<>(x.getPath(), x.getBlockId())).collect(Collectors.toSet());
      SetReplicaConfig config = (SetReplicaConfig) jobConfig;
      String path = config.getPath();
      long blockId = config.getBlockId();
      Pair<String, Long> block = new Pair<>(path, blockId);
      if (activeJobs.contains(block)) {
        throw new JobDoesNotExistException(String.format(
            "There's SetReplica job running for path:%s blockId:%s, try later", path, blockId));
      }
    }
  }
}
