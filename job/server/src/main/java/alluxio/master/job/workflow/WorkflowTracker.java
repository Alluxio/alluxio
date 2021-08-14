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

package alluxio.master.job.workflow;

import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.job.ErrorUtils;
import alluxio.job.JobConfig;
import alluxio.job.plan.meta.PlanInfo;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.workflow.WorkflowConfig;
import alluxio.job.workflow.WorkflowExecution;
import alluxio.job.workflow.WorkflowExecutionRegistry;
import alluxio.job.wire.WorkflowInfo;
import alluxio.master.job.JobMaster;

import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The {@link WorkflowTracker}.
 */
public class WorkflowTracker {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowTracker.class);

  private final JobMaster mJobMaster;

  private final ConcurrentHashMap<Long, WorkflowExecution> mWorkflows;
  private final ConcurrentHashMap<Long, ConcurrentHashSet<Long>> mWaitingOn;
  private final ConcurrentHashMap<Long, ConcurrentHashSet<Long>> mChildren;
  private final ConcurrentHashMap<Long, Long> mParentWorkflow;

  /**
   * Constructor for {@link WorkflowTracker}.
   * @param jobMaster the job master
   */
  public WorkflowTracker(JobMaster jobMaster) {
    mJobMaster = jobMaster;
    mWorkflows = new ConcurrentHashMap<>();
    mWaitingOn = new ConcurrentHashMap<>();
    mChildren = new ConcurrentHashMap<>();
    mParentWorkflow = new ConcurrentHashMap<>();
  }

  /**
   * Runs a workflow with the given configuration and job id.
   *
   * @param workflowConfig the workflow configuration
   * @param jobId the job id
   */
  public synchronized void run(WorkflowConfig workflowConfig, long jobId)
      throws JobDoesNotExistException, ResourceExhaustedException {
    WorkflowExecution workflowExecution =
        WorkflowExecutionRegistry.INSTANCE.getExecution(workflowConfig);

    mWorkflows.put(jobId, workflowExecution);

    next(jobId);
  }

  /**
   * Cancels a job with a particular job id.
   * @param jobId the job id
   * @return true if job exists, false otherwise
   */
  public synchronized boolean cancel(long jobId) {
    ConcurrentHashSet<Long> children = mChildren.get(jobId);
    if (children == null) {
      return false;
    }

    for (Long child : children) {
      try {
        mJobMaster.cancel(child);
      } catch (JobDoesNotExistException e) {
        LOG.info("Tried to cancel jobId: {} but the job did not exist", child);
      }
    }
    return true;
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @param verbose whether the output should be verbose
   * @return null if the job id isn't know by the workflow tracker. WorkflowInfo otherwise
   */
  public WorkflowInfo getStatus(long jobId, boolean verbose) {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);

    if (workflowExecution == null) {
      return null;
    }

    ArrayList<Long> children = Lists.newArrayList(mChildren.get(jobId).iterator());
    Collections.sort(children);

    List<JobInfo> jobInfos = Lists.newArrayList();

    if (verbose) {
      for (long child : children) {
        try {
          jobInfos.add(mJobMaster.getStatus(child));
        } catch (JobDoesNotExistException e) {
          LOG.info(String.format("No job info on child job id %s. Skipping", child));
        }
      }
    }

    WorkflowInfo workflowInfo = new WorkflowInfo(jobId, workflowExecution.getName(),
        workflowExecution.getStatus(), workflowExecution.getLastUpdated(),
        workflowExecution.getErrorType(), workflowExecution.getErrorMessage(), jobInfos);
    return workflowInfo;
  }

  /**
   * @return all workflow infos
   */
  public Collection<WorkflowInfo> getAllInfo() {
    ArrayList<WorkflowInfo> res = Lists.newArrayList();

    for (Long workflowId : mWorkflows.keySet()) {
      res.add(getStatus(workflowId, false));
    }
    return res;
  }

  /**
   * @param name job name filter
   * @param statusList status list filter
   * @return job ids matching conditions
   */
  public Set<Long> findJobs(String name, List<Status> statusList) {
    Set<Long> jobs = new HashSet<>();
    for (Long workflowId : mWorkflows.entrySet().stream()
        .filter(x -> statusList.isEmpty() || statusList.contains(x.getValue().getStatus()))
        .map(Map.Entry::getKey).collect(Collectors.toList())) {
      if (name == null || name.isEmpty() || mWorkflows.get(workflowId).getName().equals(name)) {
        jobs.add(workflowId);
      }
    }
    return jobs;
  }

  /**
   * @return a list of all workflow ids
   */
  public Collection<Long> list() {
    return Collections.unmodifiableCollection(mWorkflows.keySet());
  }

  /**
   * Recursively cleanup the parent workflows given plans to be removed from the PlanTracker.
   * @param removedPlanIds the plan ids that are being cleaned up by PlanTracker
   */
  public synchronized void cleanup(Collection<Long> removedPlanIds) {
    for (long removedPlanId : removedPlanIds) {
      clean(removedPlanId);
    }
  }

  private synchronized void clean(long jobId) {
    mWorkflows.remove(jobId);
    mWaitingOn.remove(jobId);
    mChildren.remove(jobId);

    Long parentId = mParentWorkflow.remove(jobId);

    if (parentId == null) {
      return;
    }

    // TODO(bradley): share details of the child job to the parent workflow before deleting.
    ConcurrentHashSet<Long> siblings = mChildren.get(parentId);
    siblings.remove(jobId);

    if (siblings.isEmpty()) {
      WorkflowExecution parentExecution = mWorkflows.get(parentId);
      if (parentExecution != null && parentExecution.getStatus().isFinished()) {
        clean(parentId);
      }
    }
  }

  private synchronized void done(long jobId) {
    Long parentJobId = mParentWorkflow.get(jobId);

    if (parentJobId == null) {
      return;
    }

    ConcurrentHashSet<Long> siblings = mWaitingOn.get(parentJobId);

    siblings.remove(jobId);

    if (siblings.isEmpty()) {
      next(parentJobId);
    }
  }

  private synchronized void stop(long jobId, Status status, String errorType,
                                 String errorMessage) {
    Long parentJobId = mParentWorkflow.get(jobId);

    if (parentJobId == null) {
      return;
    }

    WorkflowExecution workflowExecution = mWorkflows.get(parentJobId);

    workflowExecution.stop(status, errorType, errorMessage);

    stop(parentJobId, status, errorType, errorMessage);
    return;
  }

  private synchronized void next(long jobId) {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);
    mChildren.putIfAbsent(jobId, new ConcurrentHashSet<>());

    Set<JobConfig> childJobConfigs = workflowExecution.next();

    if (childJobConfigs.isEmpty()) {
      done(jobId);
      return;
    }

    ConcurrentHashSet<Long> childJobIds = new ConcurrentHashSet<>();
    for (int i = 0; i < childJobConfigs.size(); i++) {
      childJobIds.add(mJobMaster.getNewJobId());
    }

    mWaitingOn.put(jobId, childJobIds);
    mChildren.get(jobId).addAll(childJobIds);

    for (Long childJobId : childJobIds) {
      mParentWorkflow.put(childJobId, jobId);
    }

    Iterator<Long> childJobIdsIter = childJobIds.iterator();
    Iterator<JobConfig> childJobConfigsIter = childJobConfigs.iterator();

    while (childJobIdsIter.hasNext() && childJobConfigsIter.hasNext()) {
      Long childJobId = childJobIdsIter.next();
      JobConfig childJobConfig = childJobConfigsIter.next();
      try {
        mJobMaster.run(childJobConfig, childJobId);
      } catch (JobDoesNotExistException | ResourceExhaustedException e) {
        LOG.warn(e.getMessage());
        final String errorType = ErrorUtils.getErrorType(e);
        workflowExecution.stop(Status.FAILED, errorType, e.getMessage());
        stop(jobId, Status.FAILED, errorType, e.getMessage());
      }
    }
  }

  /**
   * Updates internal state of the workflows based on the updated state of a plan.
   * @param planInfo info of the plan that had its status changed
   */
  public void onPlanStatusChange(PlanInfo planInfo) {
    Status status = planInfo.getStatus();
    switch (status) {
      case COMPLETED:
        done(planInfo.getId());
        break;
      case CANCELED:
      case FAILED:
        stop(planInfo.getId(), status, planInfo.getErrorType(), planInfo.getErrorMessage());
        break;
      default:
        break;
    }
  }
}
