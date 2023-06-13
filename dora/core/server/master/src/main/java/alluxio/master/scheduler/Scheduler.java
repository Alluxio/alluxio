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

package alluxio.master.scheduler;

import static java.lang.String.format;

import alluxio.Constants;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobMetaStore;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.scheduler.job.WorkerProvider;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The Scheduler which controls jobs. It is responsible for managing active workers, updating jobs
 *  and update job information to job meta store.
 * The workflow is:
 *  1. Submit a job to the scheduler.
 *  2. The scheduler will pull the task from the job and assign the task to a worker.
 *  3. The worker will execute the task and report the result to the job.
 *  4. The job will update the progress. And schedule the next task if the job is not done.
 *  5. One worker would have one task running for one job description at a time.
 */
@ThreadSafe
public final class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
  private static final int CAPACITY = 100;
  private static final long WORKER_UPDATE_INTERVAL = Configuration.getMs(
      PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME);
  private final long mSchedulerInitialDelay = Configuration.getMs(
      PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY
  );
  private static final int EXECUTOR_SHUTDOWN_MS = 10 * Constants.SECOND_MS;
  private static AtomicReference<Scheduler> sInstance = new AtomicReference<>();
  private final Map<JobDescription, Job<?>> mExistingJobs = new ConcurrentHashMap<>();
  private final Map<Job<?>, ConcurrentHashSet<Task<?>>> mJobToRunningTasks =
      new ConcurrentHashMap<>();
  private final JobMetaStore mJobMetaStore;
  // initial thread in start method since we would stop and start thread when gainPrimacy
  private ScheduledExecutorService mSchedulerExecutor;
  private volatile boolean mRunning = false;
  private final FileSystemContext mFileSystemContext;
  private final WorkerInfoHub mWorkerInfoHub;

  /**
   * Worker information hub.
   */
  @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Already performed null check")
  public class WorkerInfoHub {
    public Map<WorkerInfo, CloseableResource<BlockWorkerClient>> mActiveWorkers = ImmutableMap.of();
    private final WorkerProvider mWorkerProvider;

    /**
     * Constructor.
     * @param scheduler scheduler
     * @param workerProvider worker provider
     */
    public WorkerInfoHub(Scheduler scheduler, WorkerProvider workerProvider) {
      mWorkerProvider = workerProvider;
    }

    private final Map<WorkerInfo, PriorityBlockingQueue<Task>> mWorkerToTaskQ
        = new ConcurrentHashMap<>();

    /**
     * Enqueue task for worker.
     * @param workerInfo the worker
     * @param task the task
     * @param kickStartTask kick-start task
     * @return whether the task is enqueued successfully
     */
    public boolean enqueueTaskForWorker(@Nullable WorkerInfo workerInfo, Task task,
        boolean kickStartTask) {
      if (workerInfo == null) {
        return false;
      }
      PriorityBlockingQueue workerTaskQ = mWorkerToTaskQ
          .computeIfAbsent(workerInfo, k -> new PriorityBlockingQueue<>());
      if (!workerTaskQ.offer(task)) {
        return false;
      }
      CloseableResource<BlockWorkerClient> blkWorkerClientResource = mActiveWorkers.get(workerInfo);
      if (blkWorkerClientResource == null) {
        LOG.warn("Didn't find corresponding BlockWorkerClient for workerInfo:{}",
            workerInfo);
        return false;
      }
      if (kickStartTask) {
        // track running tasks of a job
        ConcurrentHashSet<Task<?>> tasks = mJobToRunningTasks.computeIfAbsent(task.getJob(),
                j -> new ConcurrentHashSet<>());
        tasks.add(task);
        task.execute(blkWorkerClientResource.get(), workerInfo);
        task.getResponseFuture().addListener(() -> {
          Job job = task.getJob();
          try {
            job.processResponse(task); // retry on failure logic inside
            // TODO(lucy) currently processJob is only called in the single
            // threaded scheduler thread context, in future once tasks are
            // completed, they should be able to call processJob to resume
            // their own job to schedule next set of tasks to run.
          } catch (Exception e) {
            // Unknown exception. This should not happen, but if it happens we don't
            // want to lose the worker thread, thus catching it here. Any exception
            // surfaced here should be properly handled.
            LOG.error("Unexpected exception thrown in response future listener.", e);
            job.failJob(new InternalRuntimeException(e));
          } finally {
            // whether task succeed or fail, remove it from q,
            workerTaskQ.remove(task);
            ConcurrentHashSet<Task<?>> runningTaskSet = mJobToRunningTasks.get(job);
            runningTaskSet.remove(task);
          }
        }, mSchedulerExecutor);
      }
      return true;
    }

    /**
     * Removes task from worker queue.
     * @param task the task
     * @return true if task exists and is removed, false otherwise
     */
    public boolean removeTaskFromWorkerQ(Task task) {
      WorkerInfo workerInfo = task.getMyRunningWorker();
      if (mWorkerToTaskQ.containsKey(workerInfo)) {
        return false;
      }
      PriorityBlockingQueue<Task> pq = mWorkerToTaskQ.get(workerInfo);
      return pq.remove(task);
    }

    /**
     * @return the worker to task queue
     */
    public Map<WorkerInfo, PriorityBlockingQueue<Task>> getWorkerToTaskQ() {
      return mWorkerToTaskQ;
    }

    /**
     * Refresh active workers.
     */
    @VisibleForTesting
    public void updateWorkers() {
      if (Thread.currentThread().isInterrupted()) {
        return;
      }
      Set<WorkerInfo> workerInfos;
      try {
        try {
          workerInfos = ImmutableSet.copyOf(mWorkerProvider.getWorkerInfos());
        } catch (AlluxioRuntimeException e) {
          LOG.warn("Failed to get worker info, using existing worker infos of {} workers",
              mActiveWorkers.size());
          return;
        }
        if (workerInfos.size() == mActiveWorkers.size()
            && workerInfos.containsAll(mActiveWorkers.keySet())) {
          return;
        }

        ImmutableMap.Builder<WorkerInfo, CloseableResource<BlockWorkerClient>> updatedWorkers =
            ImmutableMap.builder();
        for (WorkerInfo workerInfo : workerInfos) {
          try {
            if (mActiveWorkers.get(workerInfo) != null) {
              CloseableResource<BlockWorkerClient> workerClient = Preconditions.checkNotNull(
                  mActiveWorkers.get(workerInfo));
              updatedWorkers.put(workerInfo, workerClient);
            } else {
              updatedWorkers.put(workerInfo, mWorkerProvider.getWorkerClient(
                  workerInfo.getAddress()));
            }
          } catch (AlluxioRuntimeException e) {
            LOG.warn("Updating worker {} address failed", workerInfo.getAddress(), e);
            // skip the worker if we cannot obtain a client
          }
        }
        // Close clients connecting to lost workers
        for (Map.Entry<WorkerInfo, CloseableResource<BlockWorkerClient>> entry :
            mActiveWorkers.entrySet()) {
          WorkerInfo workerInfo = entry.getKey();
          if (!workerInfos.contains(workerInfo)) {
            CloseableResource<BlockWorkerClient> resource = entry.getValue();
            resource.close();
            LOG.debug("Closed BlockWorkerClient to lost worker {}", workerInfo);
          }
        }
        // Build the clients to the current active worker list
        mActiveWorkers = updatedWorkers.build();
      } catch (Exception e) {
        // Unknown exception. This should not happen, but if it happens we don't want to lose the
        // scheduler thread, thus catching it here. Any exception surfaced here should be properly
        // handled.
        LOG.error("Unexpected exception thrown in updateWorkers.", e);
      }
    }
  }

  /**
   * Constructor.
   *
   * @param fsCtx file system context
   * @param workerProvider   workerProvider
   * @param jobMetaStore     jobMetaStore
   */
  public Scheduler(FileSystemContext fsCtx, WorkerProvider workerProvider,
      JobMetaStore jobMetaStore) {
    mFileSystemContext = fsCtx;
    mJobMetaStore = jobMetaStore;
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricKey.MASTER_JOB_SCHEDULER_RUNNING_COUNT.getName(), mJobToRunningTasks::size);
    mWorkerInfoHub = new WorkerInfoHub(this, workerProvider);
    // the scheduler won't be instantiated twice
    sInstance.compareAndSet(null, this);
  }

  /**
   * Get the singleton instance of Scheduler.
   * getInstance won't be called before constructor.
   * @return Scheduler instance
   */
  public static @Nullable Scheduler getInstance() {
    return sInstance.get();
  }

  /**
   * Start scheduler.
   */
  public void start() {
    if (!mRunning) {
      retrieveJobs();
      mSchedulerExecutor = Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("scheduler", false));
      mSchedulerExecutor.scheduleAtFixedRate(mWorkerInfoHub::updateWorkers, 0,
          WORKER_UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
      mSchedulerExecutor.scheduleWithFixedDelay(this::processJobs, mSchedulerInitialDelay, 2000,
          TimeUnit.MILLISECONDS);
      mSchedulerExecutor.scheduleWithFixedDelay(this::cleanupStaleJob, 1, 1, TimeUnit.HOURS);
      mRunning = true;
    }
  }

  /**
   * Update workers.
   */
  public void updateWorkers() {
    mWorkerInfoHub.updateWorkers();
  }

  /*
   TODO(lucy) in future we should remove job automatically, but keep all history jobs in db to help
   user retrieve all submitted jobs status.
   */

  private void retrieveJobs() {
    for (Job<?> job : mJobMetaStore.getJobs()) {
      mExistingJobs.put(job.getDescription(), job);
      if (job.isDone()) {
        mJobToRunningTasks.remove(job);
      }
      else {
        job.initializeJob();
        mJobToRunningTasks.put(job, new ConcurrentHashSet<>());
      }
    }
  }

  /**
   * Stop scheduler.
   */
  public void stop() {
    if (mRunning) {
      mWorkerInfoHub.mActiveWorkers.values().forEach(CloseableResource::close);
      mWorkerInfoHub.mActiveWorkers = ImmutableMap.of();
      ThreadUtils.shutdownAndAwaitTermination(mSchedulerExecutor, EXECUTOR_SHUTDOWN_MS);
      mRunning = false;
    }
  }

  /**
   * Submit a job.
   * @param job the job
   * @return true if the job is new, false if the job has already been submitted
   * @throws ResourceExhaustedRuntimeException if the job cannot be submitted because the scheduler
   *  is at capacity
   * @throws UnavailableRuntimeException if the job cannot be submitted because the meta store is
   * not ready
   */
  public boolean submitJob(Job<?> job) {
    Job<?> existingJob = mExistingJobs.get(job.getDescription());
    if (existingJob != null && !existingJob.isDone()) {
      updateExistingJob(job, existingJob);
      return false;
    }

    if (mJobToRunningTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException(
          "Too many jobs running, please submit later.", true);
    }
    mJobMetaStore.updateJob(job);
    mExistingJobs.put(job.getDescription(), job);
    job.initializeJob();
    mJobToRunningTasks.putIfAbsent(job, new ConcurrentHashSet<>());
    LOG.info(format("start job: %s", job));
    return true;
  }

  private void updateExistingJob(Job<?> newJob, Job<?> existingJob) {
    existingJob.updateJob(newJob);
    mJobMetaStore.updateJob(existingJob);
    LOG.debug(format("updated existing job: %s from %s", existingJob, newJob));
    if (existingJob.getJobState() == JobState.STOPPED) {
      existingJob.setJobState(JobState.RUNNING, false);
      mJobToRunningTasks.compute(existingJob, (k, v) -> new ConcurrentHashSet<>());
      LOG.debug(format("restart existing job: %s", existingJob));
    }
  }

  /**
   * Stop a job.
   * @param jobDescription job identifier
   * @return true if the job is stopped, false if the job does not exist or has already finished
   */
  public boolean stopJob(JobDescription jobDescription) {
    Job<?> existingJob = mExistingJobs.get(jobDescription);
    if (existingJob != null && existingJob.isRunning()) {
      existingJob.setJobState(JobState.STOPPED, false);
      mJobMetaStore.updateJob(existingJob);
      // leftover tasks in mJobToRunningTasks would be removed by scheduling thread.
      return true;
    }
    return false;
  }

  /**
   * Get the job's progress report.
   * @param jobDescription job identifier
   * @param format progress report format
   * @param verbose whether to include details on failed files and failures
   * @return the progress report
   * @throws NotFoundRuntimeException if the job does not exist
   * @throws AlluxioRuntimeException if any other Alluxio exception occurs
   */
  public String getJobProgress(
      JobDescription jobDescription,
      JobProgressReportFormat format,
      boolean verbose) {
    Job<?> job = mExistingJobs.get(jobDescription);
    if (job == null) {
      throw new NotFoundRuntimeException(format("%s cannot be found.", jobDescription));
    }
    String progress = job.getProgress(format, verbose);
    return progress;
  }

  /**
   * @return the file system context
   */
  public FileSystemContext getFileSystemContext() {
    return mFileSystemContext;
  }

  /**
   * Get active workers.
   * @return active workers
   */
  @VisibleForTesting
  public Map<WorkerInfo, CloseableResource<BlockWorkerClient>> getActiveWorkers() {
    return mWorkerInfoHub.mActiveWorkers;
  }

  /**
   * Removes all finished jobs outside the retention time.
   */
  @VisibleForTesting
  public void cleanupStaleJob() {
    long current = System.currentTimeMillis();
    mExistingJobs
        .entrySet().removeIf(job -> !job.getValue().isRunning()
        && job.getValue().getEndTime().isPresent()
        && job.getValue().getEndTime().getAsLong() <= (current - Configuration.getMs(
        PropertyKey.JOB_RETENTION_TIME)));
  }

  /**
   * Get jobs.
   *
   * @return jobs
   */
  @VisibleForTesting
  public Map<JobDescription, Job<?>> getJobs() {
    return mExistingJobs;
  }

  private void processJobs() {
    if (Thread.currentThread().isInterrupted()) {
      return;
    }
    mJobToRunningTasks.forEach((k, v) -> processJob(k.getDescription(), k));
  }

  private void processJob(JobDescription jobDescription, Job<?> job) {
    if (!job.isRunning()) {
      try {
        LOG.debug("Job:{}, not running, updating metastore...", MoreObjects.toStringHelper(job)
            .add("JobId:", job.getJobId())
            .add("JobState:", job.getJobState())
            .add("JobDescription", job.getDescription()).toString());
        mJobMetaStore.updateJob(job);
      }
      catch (UnavailableRuntimeException e) {
        // This should not happen because the scheduler should not be started while master is
        // still processing journal entries. However, if it does happen, we don't want to throw
        // exception in a task running on scheduler thead. So just ignore it and hopefully later
        // retry will work.
        LOG.error("error writing to journal when processing job", e);
      }
      mJobToRunningTasks.remove(job);
      return;
    }
    if (!job.isHealthy()) {
      job.failJob(new InternalRuntimeException("Job failed because it's not healthy."));
      return;
    }

    try {
      List<Task> tasks;
      try {
        Set<WorkerInfo> workers = mWorkerInfoHub.mActiveWorkers.keySet();
        tasks = (List<Task>) job.getNextTasks(workers);
      } catch (AlluxioRuntimeException e) {
        LOG.warn(format("error getting next task for job %s", job), e);
        if (!e.isRetryable()) {
          job.failJob(e);
        }
        return;
      }
      // enqueue the worker task q and kick it start
      // TODO(lucy) add if worker q is too full tell job to save this task for retry kick-off
      for (Task task : tasks) {
        boolean taskEnqueued = getWorkerInfoHub().enqueueTaskForWorker(task.getMyRunningWorker(),
            task, true);
        if (!taskEnqueued) {
          job.onTaskSubmitFailure(task);
        }
      }
      if (mJobToRunningTasks.getOrDefault(job, new ConcurrentHashSet<>()).isEmpty()
          && job.isCurrentPassDone()) {
        if (job.needVerification()) {
          job.initiateVerification();
        }
        else {
          if (job.isHealthy()) {
            if (job.hasFailure()) {
              job.failJob(new InternalRuntimeException("Job partially failed."));
            }
            else {
              job.setJobSuccess();
            }
          }
          else {
            if (job.getJobState() != JobState.FAILED) {
              job.failJob(
                  new InternalRuntimeException("Job failed because it exceed healthy threshold."));
            }
          }
        }
      }
    } catch (Exception e) {
      // Unknown exception. This should not happen, but if it happens we don't want to lose the
      // scheduler thread, thus catching it here. Any exception surfaced here should be properly
      // handled.
      LOG.error("Unexpected exception thrown in processJob.", e);
      job.failJob(new InternalRuntimeException(e));
    }
  }

  /**
   * Get the workerinfo hub.
   * @return worker info hub
   */
  public WorkerInfoHub getWorkerInfoHub() {
    return mWorkerInfoHub;
  }

  /**
   * Get job meta store.
   * @return jobmetastore
   */
  public JobMetaStore getJobMetaStore() {
    return mJobMetaStore;
  }

  /**
   * Job/Tasks stats.
   */
  public static class SchedulerStats {
    public Map<Job, List<String>> mRunningJobToTasksStat = new HashMap<>();
    public Map<Job, String> mExistingJobAndProgresses = new HashMap<>();
  }

  /**
   * Print job status.
   * @return SchedulerStats
   */
  public SchedulerStats printJobsStatus() {
    SchedulerStats schedulerStats = new SchedulerStats();
    for (Map.Entry<Job<?>, ConcurrentHashSet<Task<?>>> entry : mJobToRunningTasks.entrySet()) {
      schedulerStats.mRunningJobToTasksStat.put(entry.getKey(),  //entry.getKey().getDescription(),
          entry.getValue().stream().map(t -> t.toString()).collect(Collectors.toList()));
    }
    for (Map.Entry<JobDescription, Job<?>> entry : mExistingJobs.entrySet()) {
      schedulerStats.mExistingJobAndProgresses.put(entry.getValue(),
          entry.getValue().getProgress(JobProgressReportFormat.JSON, true));
    }
    return schedulerStats;
  }
}
