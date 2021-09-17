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

import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JobCommand;
import alluxio.grpc.ListAllPOptions;
import alluxio.grpc.RegisterCommand;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.MasterWorkerInfo;
import alluxio.job.meta.JobIdGenerator;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.job.wire.WorkflowInfo;
import alluxio.job.workflow.WorkflowConfig;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.job.command.CommandManager;
import alluxio.master.job.workflow.WorkflowTracker;
import alluxio.master.journal.NoopJournaled;
import alluxio.master.job.plan.PlanCoordinator;
import alluxio.master.job.plan.PlanTracker;
import alluxio.resource.LockResource;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.grpc.Context;
import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The master that handles all job managing operations.
 */
@ThreadSafe
public class JobMaster extends AbstractMaster implements NoopJournaled {
  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

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
   * The Filesystem context that the job master uses for its client.
   */
  private final JobServerContext mJobServerContext;

  /**
   * All worker information. Access must be controlled on mWorkers using the RW lock(mWorkerRWLock).
   */
  @GuardedBy("mWorkerRWLock")
  private final IndexedSet<MasterWorkerInfo> mWorkers = new IndexedSet<>(mIdIndex, mAddressIndex);

  /**
   * All worker health information.
   */
  private final ConcurrentHashMap<Long, JobWorkerHealth> mWorkerHealth;

  /**
   * An RW lock that is used to control access to mWorkers.
   */
  private final ReentrantReadWriteLock mWorkerRWLock = new ReentrantReadWriteLock(true);

  /**
   * The next worker id to use.
   */
  private final AtomicLong mNextWorkerId = new AtomicLong(CommonUtils.getCurrentMs());

  /**
   * Manager for worker tasks.
   */
  private final CommandManager mCommandManager;

  /**
   * Manager for adding and removing plans.
   */
  private final PlanTracker mPlanTracker;

  /**
   * Manager for adding and removing workflows.
   */
  private final WorkflowTracker mWorkflowTracker;

  /**
   * The job id generator.
   */
  private final JobIdGenerator mJobIdGenerator;

  /**
   * Creates a new instance of {@link JobMaster}.
   *
   * @param masterContext the context for Alluxio master
   * @param filesystem    the Alluxio filesystem client the job master uses to communicate
   * @param fsContext     the filesystem client's underlying context
   * @param ufsManager    the ufs manager
   */
  public JobMaster(MasterContext masterContext, FileSystem filesystem,
      FileSystemContext fsContext, UfsManager ufsManager) {
    super(masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.JOB_MASTER_NAME));
    mJobServerContext = new JobServerContext(filesystem, fsContext, ufsManager);
    mCommandManager = new CommandManager();
    mJobIdGenerator = new JobIdGenerator();
    mWorkflowTracker = new WorkflowTracker(this);

    mPlanTracker = new PlanTracker(
        ServerConfiguration.getLong(PropertyKey.JOB_MASTER_JOB_CAPACITY),
        ServerConfiguration.getMs(PropertyKey.JOB_MASTER_FINISHED_JOB_RETENTION_TIME),
        ServerConfiguration.getLong(PropertyKey.JOB_MASTER_FINISHED_JOB_PURGE_COUNT),
        mWorkflowTracker);

    mWorkerHealth = new ConcurrentHashMap<>();
  }

  /**
   * @return new job id
   */
  public long getNewJobId() {
    return mJobIdGenerator.getNewJobId();
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    // Fail any jobs that were still running when the last job master stopped.
    for (PlanCoordinator planCoordinator : mPlanTracker.coordinators()) {
      if (!planCoordinator.isJobFinished()) {
        planCoordinator.setJobAsFailed("JobMasterShutdown",
            "Job failed: Job master shut down during execution");
      }
    }
    if (isLeader) {
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.JOB_MASTER_LOST_WORKER_DETECTION,
              new LostWorkerDetectionHeartbeatExecutor(),
              (int) ServerConfiguration.getMs(PropertyKey.JOB_MASTER_LOST_WORKER_INTERVAL),
              ServerConfiguration.global(), mMasterContext.getUserState()));
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
   * @throws JobDoesNotExistException   when the job doesn't exist
   * @throws ResourceExhaustedException if the job master is too busy to run the job
   */
  public synchronized long run(JobConfig jobConfig)
      throws JobDoesNotExistException, ResourceExhaustedException {
    long jobId = getNewJobId();
    run(jobConfig, jobId);
    return jobId;
  }

  /**
   * Runs a job with the given configuration and job id.
   *
   * @param jobConfig the job configuration
   * @param jobId the job id
   * @throws JobDoesNotExistException when the job doesn't exist
   * @throws ResourceExhaustedException if the job master is too busy to run the job
   */
  public synchronized void run(JobConfig jobConfig, long jobId)
      throws JobDoesNotExistException, ResourceExhaustedException {
    // This RPC service implementation triggers another RPC.
    // Run the implementation under forked context to avoid interference.
    // Then restore the current context at the end.
    Context forkedCtx = Context.current().fork();
    Context prevCtx = forkedCtx.attach();
    try {
      if (jobConfig instanceof PlanConfig) {
        mPlanTracker.run((PlanConfig) jobConfig, mCommandManager, mJobServerContext,
            getWorkerInfoList(), jobId);
        return;
      } else if (jobConfig instanceof WorkflowConfig) {
        mWorkflowTracker.run((WorkflowConfig) jobConfig, jobId);
        return;
      }
      throw new JobDoesNotExistException(
          ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));
    } finally {
      forkedCtx.detach(prevCtx);
    }
  }

  /**
   * Cancels a job.
   *
   * @param jobId the id of the job
   * @throws JobDoesNotExistException when the job does not exist
   */
  public void cancel(long jobId) throws JobDoesNotExistException {
    PlanCoordinator planCoordinator = mPlanTracker.getCoordinator(jobId);
    if (planCoordinator == null) {
      if (!mWorkflowTracker.cancel(jobId)) {
        throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
      }
      return;
    }
    planCoordinator.cancel();
  }

  /**
   * @return list of all job ids
   * @param options listing options
   */
  public List<Long> list(ListAllPOptions options) {
    List<Long> ids = new ArrayList<>();
    ids.addAll(mPlanTracker.findJobs(options.getName(),
        options.getStatusList().stream()
            .map(status -> Status.valueOf(status.name())).collect(Collectors.toList())));
    ids.addAll(mWorkflowTracker.findJobs(options.getName(),
        options.getStatusList().stream()
            .map(status -> Status.valueOf(status.name())).collect(Collectors.toList())));
    Collections.sort(ids);
    return ids;
  }

  /**
   * @return list of all job infos
   */
  public List<JobInfo> listDetailed() {
    List<JobInfo> jobInfos = new ArrayList<>();

    for (PlanCoordinator coordinator : mPlanTracker.coordinators()) {
      jobInfos.add(coordinator.getPlanInfoWire(false));
    }

    jobInfos.addAll(mWorkflowTracker.getAllInfo());

    jobInfos.sort(Comparator.comparingLong(JobInfo::getId));

    return jobInfos;
  }

  /**
   * @param limit maximum number of jobInfos to return
   * @param before filters out on or after this timestamp (in ms) (-1 to disable)
   * @param after filter out on or before this timestamp (in ms) (-1 to disable)
   * @return list of all failed job infos ordered by when it failed (recently failed first)
   */
  public List<JobInfo> failed(int limit, long before, long after) {
    List<JobInfo> jobInfos = new ArrayList<>();
    mPlanTracker.failed()
        .filter((planInfoMeta) -> {
          final long lastStatusChangeMs = planInfoMeta.getLastStatusChangeMs();
          if (before >= 0 && before <= lastStatusChangeMs) {
            return false;
          }
          return after < lastStatusChangeMs;
        }).filter((planInfoMeta) -> planInfoMeta.getLastStatusChangeMs() > after)
        .limit(limit)
        .forEachOrdered((planInfoMeta) ->
            jobInfos.add(new alluxio.job.wire.PlanInfo(planInfoMeta, false)));
    return jobInfos;
  }

  /**
   * Gets information of the given job id (verbose = True).
   *
   * @param jobId the id of the job
   * @return the job information
   * @throws JobDoesNotExistException if the job does not exist
   */
  public JobInfo getStatus(long jobId) throws JobDoesNotExistException {
    return getStatus(jobId, true);
  }

  /**
   * Gets information of the given job id.
   *
   * @param jobId the id of the job
   * @param verbose whether the job info should be verbose
   * @return the job information
   * @throws JobDoesNotExistException if the job does not exist
   */
  public JobInfo getStatus(long jobId, boolean verbose) throws JobDoesNotExistException {
    PlanCoordinator planCoordinator = mPlanTracker.getCoordinator(jobId);
    if (planCoordinator == null) {

      WorkflowInfo status = mWorkflowTracker.getStatus(jobId, verbose);

      if (status == null) {
        throw new JobDoesNotExistException(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
      }
      return status;
    }
    return planCoordinator.getPlanInfoWire(verbose);
  }

  /**
   * Gets summary of the job service.
   *
   * @return {@link JobServiceSummary}
   */
  public alluxio.job.wire.JobServiceSummary getSummary() {
    return new JobServiceSummary(listDetailed());
  }

  /**
   * @return health metrics for each of the job workers
   */
  public List<JobWorkerHealth> getAllWorkerHealth() {
    ArrayList<JobWorkerHealth> result = Lists.newArrayList(mWorkerHealth.values());
    Collections.sort(result, Comparator.comparingLong((a) -> a.getWorkerId()));
    return result;
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
        for (PlanCoordinator planCoordinator : mPlanTracker.coordinators()) {
          planCoordinator.failTasksForWorker(deadWorker.getId());
        }
        mWorkerHealth.remove(deadWorker.getId());
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
   * @param taskPoolSize the task pool size for the job workers
   */
  public void setTaskPoolSize(int taskPoolSize) {
    try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
      for (MasterWorkerInfo worker : mWorkers) {
        mCommandManager.submitSetTaskPoolSizeCommand(worker.getId(), taskPoolSize);
      }
    }
  }

  /**
   * Updates the tasks' status when a worker periodically heartbeats with the master, and sends the
   * commands for the worker to execute.
   *
   * @param jobWorkerHealth the job worker health info
   * @param taskInfoList the list of the task information
   * @return the list of {@link JobCommand} to the worker
   */
  public List<JobCommand> workerHeartbeat(JobWorkerHealth jobWorkerHealth,
      List<TaskInfo> taskInfoList) throws ResourceExhaustedException {

    long workerId = jobWorkerHealth.getWorkerId();

    String hostname;
    // Run under shared lock for mWorkers
    try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
      MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
      if (worker == null) {
        return Collections.singletonList(JobCommand.newBuilder()
            .setRegisterCommand(RegisterCommand.getDefaultInstance()).build());
      }
      hostname = worker.getWorkerAddress().getHost();
      // Update last-update-time of this particular worker under lock
      // to prevent lost worker detector clearing it under race
      worker.updateLastUpdatedTimeMs();
    }
    mWorkerHealth.put(workerId, jobWorkerHealth);

    // Update task infos for all jobs involved
    Map<Long, List<TaskInfo>> taskInfosPerJob = new HashMap<>();
    for (TaskInfo taskInfo : taskInfoList) {
      taskInfo.setWorkerHost(hostname);
      if (!taskInfosPerJob.containsKey(taskInfo.getJobId())) {
        taskInfosPerJob.put(taskInfo.getJobId(), new ArrayList());
      }
      taskInfosPerJob.get(taskInfo.getJobId()).add(taskInfo);
    }
    for (Map.Entry<Long, List<TaskInfo>> taskInfosPair : taskInfosPerJob.entrySet()) {
      PlanCoordinator planCoordinator = mPlanTracker.getCoordinator(taskInfosPair.getKey());
      if (planCoordinator != null) {
        planCoordinator.updateTasks(taskInfosPair.getValue());
      }
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
      int masterWorkerTimeoutMs = (int) ServerConfiguration
          .getMs(PropertyKey.JOB_MASTER_WORKER_TIMEOUT);
      List<MasterWorkerInfo> lostWorkers = new ArrayList<MasterWorkerInfo>();
      // Run under shared lock for mWorkers
      try (LockResource workersLockShared = new LockResource(mWorkerRWLock.readLock())) {
        for (MasterWorkerInfo worker : mWorkers) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.warn("The worker {} timed out after {}ms without a heartbeat!", worker, lastUpdate);
            lostWorkers.add(worker);
            for (PlanCoordinator planCoordinator : mPlanTracker.coordinators()) {
              planCoordinator.failTasksForWorker(worker.getId());
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
              mWorkerHealth.remove(lostWorker.getId());
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
