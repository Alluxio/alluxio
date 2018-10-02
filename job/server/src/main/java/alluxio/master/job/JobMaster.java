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

package alluxio.master.job;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobConfig;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.meta.JobInfo;
import alluxio.job.meta.MasterWorkerInfo;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.master.AbstractNonJournaledMaster;
import alluxio.master.MasterContext;
import alluxio.master.job.command.CommandManager;
import alluxio.thrift.JobCommand;
import alluxio.thrift.JobMasterWorkerService;
import alluxio.thrift.RegisterCommand;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public final class JobMaster extends AbstractNonJournaledMaster {
  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);
  private static final long CAPACITY = Configuration.getLong(PropertyKey.JOB_MASTER_JOB_CAPACITY);
  private static final long RETENTION_MS =
      Configuration.getLong(PropertyKey.JOB_MASTER_FINISHED_JOB_RETENTION_MS);

  // Worker metadata management.
  private final IndexDefinition<MasterWorkerInfo, Long> mIdIndex =
      new IndexDefinition<MasterWorkerInfo, Long>(true) {
        @Override
        public Long getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private final IndexDefinition<MasterWorkerInfo, WorkerNetAddress> mAddressIndex =
      new IndexDefinition<MasterWorkerInfo, WorkerNetAddress>(true) {
        @Override
        public WorkerNetAddress getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  /**
   * All worker information. Access must be synchronized on mWorkers. If both block and worker
   * metadata must be locked, mBlocks must be locked first.
   */
  private final IndexedSet<MasterWorkerInfo> mWorkers = new IndexedSet<>(mIdIndex, mAddressIndex);

  /** The next worker id to use. */
  private final AtomicLong mNextWorkerId = new AtomicLong(CommonUtils.getCurrentMs());

  /** Manage all the jobs' status. */
  private final JobIdGenerator mJobIdGenerator;
  private final CommandManager mCommandManager;
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;
  private final SortedSet<JobInfo> mFinishedJobs;
  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param masterContext the context for Alluxio master
   * @param ufsManager the ufs manager
   */
  public JobMaster(MasterContext masterContext, UfsManager ufsManager) {
    super(masterContext, new SystemClock(), ExecutorServiceFactories
        .cachedThreadPool(Constants.JOB_MASTER_NAME));
    mJobIdGenerator = new JobIdGenerator();
    mCommandManager = new CommandManager();
    mIdToJobCoordinator = Maps.newHashMap();
    mFinishedJobs = Collections.synchronizedSortedSet(Sets.<JobInfo>newTreeSet());
    mUfsManager = ufsManager;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    // Fail any jobs that were still running when the last job master stopped.
    for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
      JobInfo jobInfo = jobCoordinator.getJobInfo();
      if (!jobInfo.getStatus().isFinished()) {
        jobInfo.setStatus(Status.FAILED);
        jobInfo.setErrorMessage("Job failed: Job master shut down during execution");
      }
    }
    if (isLeader) {
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION,
              new LostWorkerDetectionHeartbeatExecutor(),
              Configuration.getInt(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL_MS)));
    }
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = Maps.newHashMap();
    services.put(Constants.JOB_MASTER_WORKER_SERVICE_NAME,
        new JobMasterWorkerService.Processor<>(new JobMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.JOB_MASTER_NAME;
  }

  /**
   * Runs a job with the given configuration.
   *
   * @param jobConfig the job configuration
   * @return the job id tracking the progress
   * @throws JobDoesNotExistException when the job doesn't exist
   * @throws ResourceExhaustedException if the job master is too busy to run the job
   */
  public synchronized long run(JobConfig jobConfig)
      throws JobDoesNotExistException, ResourceExhaustedException {
    long jobId = mJobIdGenerator.getNewJobId();
    JobInfo jobInfo = new JobInfo(jobId, jobConfig, new Function<JobInfo, Void>() {
      @Override
      public Void apply(JobInfo jobInfo) {
        Status status = jobInfo.getStatus();
        mFinishedJobs.remove(jobInfo);
        if (status.isFinished()) {
          mFinishedJobs.add(jobInfo);
        }
        return null;
      }
    });
    if (mIdToJobCoordinator.size() == CAPACITY) {
      if (mFinishedJobs.isEmpty()) {
        // The job master is at full capacity and no job has finished.
        throw new ResourceExhaustedException("Job master is at full capacity");
      }
      // Check if the oldest finished job can be discarded.
      Iterator<JobInfo> jobIterator = mFinishedJobs.iterator();
      JobInfo oldestJob = jobIterator.next();
      if (CommonUtils.getCurrentMs() - oldestJob.getLastStatusChangeMs() < RETENTION_MS) {
        // do not evict the candidate job if it has finished recently
        throw new ResourceExhaustedException("Job master is at full capacity");
      }
      jobIterator.remove();
      mIdToJobCoordinator.remove(oldestJob.getId());
    }
    JobCoordinator jobCoordinator =
        JobCoordinator.create(mCommandManager, mUfsManager, getWorkerInfoList(), jobInfo);
    mIdToJobCoordinator.put(jobId, jobCoordinator);
    return jobId;
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job
   * @throws JobDoesNotExistException when the job does not exist
   */
  public synchronized void cancel(long jobId) throws JobDoesNotExistException {
    if (!mIdToJobCoordinator.containsKey(jobId)) {
      throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
    }
    JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
    jobCoordinator.cancel();
  }

  /**
   * @return list all the job ids
   */
  public synchronized List<Long> list() {
    return Lists.newArrayList(mIdToJobCoordinator.keySet());
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @return the job information
   * @throws JobDoesNotExistException if the job does not exist
   */
  public synchronized alluxio.job.wire.JobInfo getStatus(long jobId)
      throws JobDoesNotExistException {
    if (!mIdToJobCoordinator.containsKey(jobId)) {
      throw new JobDoesNotExistException(jobId);
    }
    JobInfo jobInfo = mIdToJobCoordinator.get(jobId).getJobInfo();
    return new alluxio.job.wire.JobInfo(jobInfo);
  }

  /**
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public synchronized long registerWorker(WorkerNetAddress workerNetAddress) {
    if (mWorkers.contains(mAddressIndex, workerNetAddress)) {
      // If the worker is trying to re-register, it must have died and been restarted. We need to
      // clean up the dead worker.
      LOG.info("Worker at address {} is re-registering. Failing tasks for previous worker at that "
          + "address", workerNetAddress);
      MasterWorkerInfo deadWorker = mWorkers.getFirstByField(mAddressIndex, workerNetAddress);
      for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
        jobCoordinator.failTasksForWorker(deadWorker.getId());
      }
      mWorkers.remove(deadWorker);
    }

    // Generate a new worker id.
    long workerId = mNextWorkerId.getAndIncrement();
    mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

    LOG.info("registerWorker(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
      workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
    }
    return workerInfoList;
  }

  /**
   * Updates the tasks' status when a worker periodically heartbeats with the master, and sends the
   * commands for the worker to execute.
   *
   * @param workerId the worker id
   * @param taskInfoList the list of the task information
   * @return the list of {@link JobCommand} to the worker
   */
  public synchronized List<JobCommand> workerHeartbeat(long workerId,
      List<TaskInfo> taskInfoList) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
    if (worker == null) {
      return Collections.singletonList(JobCommand.registerCommand(new RegisterCommand()));
    }
    worker.updateLastUpdatedTimeMs();
    // update the job info
    List<Long> updatedJobIds = new ArrayList<>();
    for (TaskInfo taskInfo : taskInfoList) {
      JobInfo jobInfo = mIdToJobCoordinator.get(taskInfo.getJobId()).getJobInfo();
      if (jobInfo == null) {
        // The master must have restarted and forgotten about the job.
        continue;
      }
      jobInfo.setTaskInfo(taskInfo.getTaskId(), taskInfo);
      updatedJobIds.add(taskInfo.getJobId());
    }
    for (long updatedJobId : updatedJobIds) {
      // update the job status
      JobCoordinator jobCoordinator = mIdToJobCoordinator.get(updatedJobId);
      jobCoordinator.updateStatus();
    }

    return mCommandManager.pollAllPendingCommands(workerId);
  }

  /**
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      int masterWorkerTimeoutMs = Configuration.getInt(PropertyKey.JOB_MASTER_WORKER_TIMEOUT_MS);
      synchronized (JobMaster.this) {
        for (MasterWorkerInfo worker : mWorkers) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.warn("The worker {} timed out after {}ms without a heartbeat!", worker, lastUpdate);
            for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
              jobCoordinator.failTasksForWorker(worker.getId());
            }
            mWorkers.remove(worker);
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
