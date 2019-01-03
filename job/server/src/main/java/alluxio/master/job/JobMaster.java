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
import alluxio.grpc.GrpcService;
import alluxio.grpc.JobCommand;
import alluxio.grpc.RegisterCommand;
import alluxio.grpc.ServiceType;
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
import alluxio.resource.LockResource;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.jcip.annotations.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
   * All worker information. Access must be controlled on mWorkers using the RW lock(mWorkerRWLock).
   */
  @GuardedBy("mWorkerRWLock")
  private final IndexedSet<MasterWorkerInfo> mWorkers = new IndexedSet<>(mIdIndex, mAddressIndex);

  /**
   * An RW lock that is used to control access to mWorkers.
   */
  private final ReentrantReadWriteLock mWorkerRWLock = new ReentrantReadWriteLock(true);

  /**
   * The next worker id to use.
   */
  private final AtomicLong mNextWorkerId = new AtomicLong(CommonUtils.getCurrentMs());

  /**
   * Used to generate Id for new jobs.
   */
  private final JobIdGenerator mJobIdGenerator;

  /**
   * Manager for worker tasks.
   */
  private final CommandManager mCommandManager;

  /**
   * Used to store JobCoordinator instances per job Id.
   * This member is accessed concurrently and its instance type is ConcurrentHashMap.
   */
  private final Map<Long, JobCoordinator> mIdToJobCoordinator;

  /**
   * Used to keep track of finished jobs that are still within retention policy.
   * This member is accessed concurrently and its instance type is ConcurrentSkipListSet.
   */
  private final SortedSet<JobInfo> mFinishedJobs;

  /**
   * The manager for all ufs.
   */
  private UfsManager mUfsManager;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param masterContext the context for Alluxio master
   * @param ufsManager the ufs manager
   */
  public JobMaster(MasterContext masterContext, UfsManager ufsManager) {
    super(masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.JOB_MASTER_NAME));
    mJobIdGenerator = new JobIdGenerator();
    mCommandManager = new CommandManager();
    mIdToJobCoordinator = new ConcurrentHashMap<>();
    mFinishedJobs = new ConcurrentSkipListSet<>();
    mUfsManager = ufsManager;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    // Fail any jobs that were still running when the last job master stopped.
    for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
      if (!jobCoordinator.isJobFinished()) {
        jobCoordinator.setJobAsFailed("Job failed: Job master shut down during execution");
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
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = Maps.newHashMap();
    services.put(ServiceType.JOB_MASTER_CLIENT_SERVICE,
        new GrpcService(new JobMasterClientServiceHandler(this)));
    services.put(ServiceType.JOB_MASTER_WORKER_SERVICE,
        new GrpcService(new JobMasterWorkerServiceHandler(this)));
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
   *
   * @throws JobDoesNotExistException when the job doesn't exist
   * @throws ResourceExhaustedException if the job master is too busy to run the job
   */
  public synchronized long run(JobConfig jobConfig)
      throws JobDoesNotExistException, ResourceExhaustedException {
    if (mIdToJobCoordinator.size() == CAPACITY) {
      if (mFinishedJobs.isEmpty()) {
        // The job master is at full capacity and no job has finished.
        throw new ResourceExhaustedException("Job master is at full capacity");
      }
      // Discard old jobs that have completion time beyond retention policy
      Iterator<JobInfo> jobIterator = mFinishedJobs.iterator();
      // Used to denote whether space could be reserved for the new job
      // It's 'true' if job master is at full capacity
      boolean isfull = true;
      while (jobIterator.hasNext()) {
        JobInfo oldestJob = jobIterator.next();
        long completedBeforeMs = CommonUtils.getCurrentMs() - oldestJob.getLastStatusChangeMs();
        if (completedBeforeMs < RETENTION_MS) {
          // mFinishedJobs is sorted. Can't iterate to a job within retention policy
          break;
        }
        jobIterator.remove();
        mIdToJobCoordinator.remove(oldestJob.getId());
        isfull = false;
      }
      if (isfull) {
        throw new ResourceExhaustedException("Job master is at full capacity");
      }
    }
    long jobId = mJobIdGenerator.getNewJobId();
    JobCoordinator jobCoordinator = JobCoordinator.create(mCommandManager, mUfsManager,
        getWorkerInfoList(), jobId, jobConfig, (jobInfo) -> {
          Status status = jobInfo.getStatus();
          mFinishedJobs.remove(jobInfo);
          if (status.isFinished()) {
            mFinishedJobs.add(jobInfo);
          }
          return null;
        });
    mIdToJobCoordinator.put(jobId, jobCoordinator);
    return jobId;
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job
   * @throws JobDoesNotExistException when the job does not exist
   */
  public void cancel(long jobId) throws JobDoesNotExistException {
    JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
    if (jobCoordinator == null) {
      throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
    }
    jobCoordinator.cancel();
  }

  /**
   * @return list all the job ids
   */
  public List<Long> list() {
    return Lists.newArrayList(mIdToJobCoordinator.keySet());
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @return the job information
   * @throws JobDoesNotExistException if the job does not exist
   */
  public alluxio.job.wire.JobInfo getStatus(long jobId) throws JobDoesNotExistException {
    JobCoordinator jobCoordinator = mIdToJobCoordinator.get(jobId);
    if (jobCoordinator == null) {
      throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
    }
    return jobCoordinator.getJobInfoWire();
  }

  /**
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public long registerWorker(WorkerNetAddress workerNetAddress) {
    // Run under exclusive lock for mWorkers
    try (LockResource workersLockExclusive = new LockResource(mWorkerRWLock.writeLock())) {
      // Check if worker has already been registered with this job master
      if (mWorkers.contains(mAddressIndex, workerNetAddress)) {
        // If the worker is trying to re-register, it must have died and been restarted. We need to
        // clean up the dead worker.
        LOG.info(
            "Worker at address {} is re-registering. Failing tasks for previous worker at that "
                + "address",
            workerNetAddress);
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
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    // Run under shared lock for mWorkers
    try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
      for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
        workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
      }
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
  public List<JobCommand> workerHeartbeat(long workerId, List<TaskInfo> taskInfoList) {
    // Run under shared lock for mWorkers
    try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
      MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
      if (worker == null) {
        return Collections.singletonList(JobCommand.newBuilder()
            .setRegisterCommand(RegisterCommand.getDefaultInstance()).build());
      }
      // Update last-update-time of this particular worker under lock
      // to prevent lost worker detector clearing it under race
      worker.updateLastUpdatedTimeMs();
    }

    // Update task infos for all jobs involved
    Map<Long, List<TaskInfo>> taskInfosPerJob = new HashMap<>();
    for (TaskInfo taskInfo : taskInfoList) {
      if (!taskInfosPerJob.containsKey(taskInfo.getJobId())) {
        taskInfosPerJob.put(taskInfo.getJobId(), new ArrayList<TaskInfo>());
      }
      taskInfosPerJob.get(taskInfo.getJobId()).add(taskInfo);
    }
    for (Map.Entry<Long, List<TaskInfo>> taskInfosPair : taskInfosPerJob.entrySet()) {
      JobCoordinator jobCoordinator = mIdToJobCoordinator.get(taskInfosPair.getKey());
      jobCoordinator.updateTasks(taskInfosPair.getValue());
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
      List<MasterWorkerInfo> lostWorkers = new ArrayList<MasterWorkerInfo>();
      // Run under shared lock for mWorkers
      try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
        for (MasterWorkerInfo worker : mWorkers) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.warn("The worker {} timed out after {}ms without a heartbeat!", worker, lastUpdate);
            lostWorkers.add(worker);
            for (JobCoordinator jobCoordinator : mIdToJobCoordinator.values()) {
              jobCoordinator.failTasksForWorker(worker.getId());
            }
          }
        }
      }
      // Remove lost workers
      if (!lostWorkers.isEmpty()) {
        // Run under exclusive lock for mWorkers
        try (LockResource workersLockExclusive = new LockResource(mWorkerRWLock.writeLock())) {
          for (MasterWorkerInfo lostWorker : lostWorkers) {
            // Check last update time for lost workers again as it could have been changed while
            // waiting for exclusive lock.
            final long lastUpdate = mClock.millis() - lostWorker.getLastUpdatedTimeMs();
            if (lastUpdate > masterWorkerTimeoutMs) {
              mWorkers.remove(lostWorker);
            }
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
