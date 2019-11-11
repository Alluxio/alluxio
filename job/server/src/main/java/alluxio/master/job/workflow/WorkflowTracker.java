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
import alluxio.job.plan.wire.TaskInfo;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.workflow.WorkflowConfig;
import alluxio.job.workflow.WorkflowExecution;
import alluxio.job.workflow.WorkflowExecutionFactory;
import alluxio.job.workflow.WorkflowExecutionFactoryRegistry;
import alluxio.job.wire.WorkflowInfo;
import alluxio.master.job.JobMaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final ConcurrentHashMap<Long, Long> mParentWorkflow;

  /**
   * Constructor for {@link WorkflowTracker}.
   * @param jobMaster the job master
   */
  public WorkflowTracker(JobMaster jobMaster) {
    mJobMaster = jobMaster;
    mWorkflows = new ConcurrentHashMap<>();
    mWaitingOn = new ConcurrentHashMap<>();
    mParentWorkflow = new ConcurrentHashMap<>();
  }

  /**
   * TO DO.
   * @param workflowConfig TODO(bradley)
   * @param jobId TODO(bradley)
   */
  public void run(WorkflowConfig workflowConfig, long jobId)
      throws JobDoesNotExistException, ResourceExhaustedException {
    WorkflowExecutionFactory<WorkflowConfig> executionFactory =
        WorkflowExecutionFactoryRegistry.INSTANCE.getExecutionFactory(workflowConfig);

    WorkflowExecution workflowExecution = executionFactory.create(workflowConfig);

    mWorkflows.put(jobId, workflowExecution);

    next(jobId);
  }

  public WorkflowInfo getStatus(long jobId) {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);

    if (workflowExecution == null) {
      return null;
    }

    boolean done = workflowExecution.isDone();

    Status status = Status.RUNNING;

    if (done) {
      status = Status.COMPLETED;
    }

    return new WorkflowInfo(jobId, null, status);
  }

  /**
   * @return a list of workflow ids
   */
  public Collection<Long> list() {
    return Collections.unmodifiableCollection(mWorkflows.keySet());
  }

  private void done(long jobId) throws ResourceExhaustedException, JobDoesNotExistException {
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

  private void next(long jobId) throws ResourceExhaustedException, JobDoesNotExistException {
    WorkflowExecution workflowExecution = mWorkflows.get(jobId);

    Set<JobConfig> childJobConfigs = workflowExecution.nextJobs();

    if (childJobConfigs.isEmpty()) {
      done(jobId);
      return;
    }

    ConcurrentHashSet<Long> childJobIds = new ConcurrentHashSet<>();
    for (int i = 0; i < childJobConfigs.size(); i++) {
      childJobIds.add(mJobMaster.getNewJobId());
    }

    mWaitingOn.put(jobId, childJobIds);

    for (Long childJobId : childJobIds) {
      mParentWorkflow.put(childJobId, jobId);
    }

    Iterator<Long> childJobIdsIter = childJobIds.iterator();
    Iterator<JobConfig> childJobConfigsIter = childJobConfigs.iterator();

    while (childJobIdsIter.hasNext() && childJobConfigsIter.hasNext()) {
      Long childJobId = childJobIdsIter.next();
      JobConfig childJobConfig = childJobConfigsIter.next();
      mJobMaster.run(childJobConfig, childJobId);
    }
  }

  /**
   * TODO(bradley).
   * @param taskInfoList TODO(bradley)
   * @throws JobDoesNotExistException TODO(bradley)
   * @throws ResourceExhaustedException TODO(bradley)
   */
  public synchronized void workerHeartbeat(List<TaskInfo> taskInfoList)
      throws JobDoesNotExistException, ResourceExhaustedException {

    for (TaskInfo taskInfo : taskInfoList) {
      Long planId = taskInfo.getParentId();
      JobInfo jobInfo = mJobMaster.getStatus(planId);
      if (jobInfo.getStatus().isFinished()) {
        done(planId);
      }
    }
  }
}
