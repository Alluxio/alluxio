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
import alluxio.job.JobConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @return null if the job id isn't know by the workflow tracker. WorkflowInfo otherwise
   */
  public WorkflowInfo getStatus(long jobId) {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);

    if (workflowExecution == null) {
      return null;
    }

    ConcurrentHashSet<Long> children = mChildren.get(jobId);

    List<JobInfo> jobInfos = Lists.newArrayList();

    for (long child : children) {
      try {
        jobInfos.add(mJobMaster.getStatus(child));
      } catch (JobDoesNotExistException e) {
        LOG.info(String.format("No job info on child job id %s. Skipping", child));
      }
    }

    WorkflowInfo workflowInfo = new WorkflowInfo(jobId, workflowExecution.getStatus(),
        workflowExecution.getLastUpdated(), jobInfos);
    return workflowInfo;
  }

  /**
   * @return all workflow infos
   */
  public Collection<WorkflowInfo> getAllInfo() {
    ArrayList<WorkflowInfo> res = Lists.newArrayList();

    for (Long workflowId : mWorkflows.keySet()) {
      res.add(getStatus(workflowId));
    }
    return res;
  }

  /**
   * @return a list of all workflow ids
   */
  public Collection<Long> list() {
    return Collections.unmodifiableCollection(mWorkflows.keySet());
  }

  private synchronized void done(long jobId) throws ResourceExhaustedException {
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

  private synchronized void fail(long jobId, Status status) {
    Long parentJobId = mParentWorkflow.get(jobId);

    if (parentJobId == null) {
      return;
    }

    WorkflowExecution workflowExecution = mWorkflows.get(parentJobId);

    workflowExecution.fail(status);

    fail(parentJobId, status);
    return;
  }

  private synchronized void next(long jobId) throws ResourceExhaustedException {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);

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

    mChildren.putIfAbsent(jobId, new ConcurrentHashSet<>());
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
      } catch (JobDoesNotExistException e) {
        LOG.warn(e.getMessage());
        fail(jobId, Status.FAILED);
      }
    }
  }

  /**
   * Updates internal state of the workflows based on the updated state of the tasks.
   * @param taskInfoList list of tasks that have been updated
   * @throws ResourceExhaustedException if new jobs can't be scheduled
   */
  public synchronized void workerHeartbeat(List<TaskInfo> taskInfoList)
      throws ResourceExhaustedException {

    for (TaskInfo taskInfo : taskInfoList) {
      Long planId = taskInfo.getParentId();
      JobInfo jobInfo = null;
      try {
        jobInfo = mJobMaster.getStatus(planId);
      } catch (JobDoesNotExistException e) {
        LOG.info("Received heartbeat for a task with an unknown parent. Skipping", planId);
        continue;
      }
      Status status = jobInfo.getStatus();
      if (status.equals(Status.COMPLETED)) {
        done(planId);
      } else if (status.equals(Status.CANCELED) || status.equals(Status.FAILED)) {
        fail(planId, status);
      }
    }
  }
}
