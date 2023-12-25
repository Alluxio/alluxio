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
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
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
import java.util.concurrent.atomic.AtomicInteger;
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
@SuppressFBWarnings({"SE_NO_SERIALVERSIONID"})
public final class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
  private static final int CAPACITY = 100;
  private static final int MAX_TASK_PER_WORKER = 10;
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
      mExistingJobs.clear();
      mJobToRunningTasks.clear();
      mWorkerInfoHub.mWorkerToTaskQ.clear();
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
  public synchronized boolean submitJob(Job<?> job) {
    Job<?> existingJob = mExistingJobs.get(job.getDescription());
    if (existingJob != null && !existingJob.isDone()) {
      mJobToRunningTasks.compute(existingJob, (k, v) -> {
        if (k.getJobState() == JobState.STOPPED) {
          k.setJobState(JobState.RUNNING, true);
          LOG.debug(format("restart existing job: %s", existingJob));
          return new ConcurrentHashSet<>();
        }
        return v;
      });
      return false;
    }

    if (mJobToRunningTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException("Too many jobs running, please submit later.",
          true);
    }
    ConcurrentHashSet<Task<?>> result =
        mJobToRunningTasks.putIfAbsent(job, new ConcurrentHashSet<>());
    if (result != null) {
      LOG.warn("There's concurrent submit while job is still in cleaning state");
      return false;
    }
    mJobMetaStore.updateJob(job);
    mExistingJobs.put(job.getDescription(), job);
    job.initializeJob();
    LOG.info(format("start job: %s", job));
    return true;
  }

  /**
   * Stop a job.
   * @param jobDescription job identifier
   * @return true if the job is stopped, false if the job does not exist or has already finished
   */
  public boolean stopJob(JobDescription jobDescription) {
    Job<?> existingJob = mExistingJobs.get(jobDescription);
    if (existingJob != null && existingJob.isRunning()) {
      existingJob.setJobState(JobState.STOPPED, true);
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
   * Get the job's state.
   * @param jobDescription job identifier
   * @return the job state
   * @throws NotFoundRuntimeException if the job does not exist
   */
  public JobState getJobState(JobDescription jobDescription) {
    Job<?> job = mExistingJobs.get(jobDescription);
    if (job == null) {
      throw new NotFoundRuntimeException(format("%s cannot be found.", jobDescription));
    }
    return job.getJobState();
  }

  /**
   * Get active workers.
   * @return active workers
   */
  @VisibleForTesting
  public Set<WorkerInfo> getActiveWorkers() {
    return mWorkerInfoHub.mActiveWorkers.keySet().stream()
        .map(x -> x.mWorkerInfo).collect(Collectors.toSet());
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
    mJobToRunningTasks.forEach((k, v) -> processJob(k));
    // kickstart the head task from each q of the worker if it's not running
    mWorkerInfoHub.kickStartTasks();
  }

  private void processJob(Job<?> job) {
    ConcurrentHashSet<Task<?>> runningTasks = mJobToRunningTasks.compute(job, (k, v) -> {
      if (!k.isRunning()) {
        return null;
      }
      return v;
    });
    // job is not running anymore
    if (runningTasks == null) {
      return;
    }

    if (!job.isHealthy()) {
      job.failJob(new InternalRuntimeException("Job failed because it's not healthy."));
      return;
    }

    try {
      List<Task> tasks;
      try {
        Set<WorkerInfo> workers = mWorkerInfoHub.mActiveWorkers.keySet()
            .stream().map(x -> x.mWorkerInfo).collect(Collectors.toSet());
        tasks = (List<Task>) job.getNextTasks(workers);
      } catch (AlluxioRuntimeException e) {
        LOG.warn(format("error getting next task for job %s", job), e);
        if (!e.isRetryable()) {
          job.failJob(e);
        }
        return;
      }
      // enqueue the worker task q
      for (Task task : tasks) {
        boolean taskEnqueued = getWorkerInfoHub().enqueueTaskForWorker(
            task.getMyRunningWorker(), task);
        if (!taskEnqueued) {
          job.onTaskSubmitFailure(task);
        }
      }
      mJobToRunningTasks.compute(job, (k, v) -> {
        if ((v == null || v.isEmpty()) && k.isCurrentPassDone()) {
          checkAndSetJobStatus(k);
        }
        return v;
      });
    } catch (Exception e) {
      // Unknown exception. This should not happen, but if it happens we don't want to lose the
      // scheduler thread, thus catching it here. Any exception surfaced here should be properly
      // handled.
      LOG.error("Unexpected exception thrown in processJob.", e);
      job.failJob(new InternalRuntimeException(e));
    }
  }

  private static void checkAndSetJobStatus(Job<?> job) {
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
   * Bounded priority queue impl.
   * @param <E>
   */
  @SuppressFBWarnings({"SE_BAD_FIELD_INNER_CLASS", "SE_NO_SERIALVERSIONID"})
  public class BoundedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {

    private AtomicInteger mLen = new AtomicInteger(0);
    private final int mCapacity;

    /**
     * Constructor for Bounded priority queue with a max capacity.
     * @param capacity
     */
    public BoundedPriorityBlockingQueue(int capacity) {
      mCapacity = capacity;
    }

    @Override
    public boolean offer(E e) {
      if (mLen.incrementAndGet() > mCapacity) {
        mLen.decrementAndGet();
        return false;
      }
      // this will always return true
      return super.offer(e);
    }

    @Override
    public E poll() {
      E e = super.poll();
      if (e != null) {
        mLen.decrementAndGet();
      }
      return e;
    }

    @Override
    public boolean remove(Object o) {
      boolean removed = super.remove(o);
      if (removed) {
        mLen.decrementAndGet();
      }
      return removed;
    }
  }

  /**
   * Util class here for tracking unique identity of a worker as
   * WorkerInfo class uses constantly changing field such as
   * mLastContactSec for equals(), which can't be served as key
   * class in map.
   */
  public static class WorkerInfoIdentity {
    public final WorkerInfo mWorkerInfo;

    /**
     * Constructor for WorkerInfoIdentity from WorkerInfo.
     * @param workerInfo
     */
    public WorkerInfoIdentity(WorkerInfo workerInfo) {
      mWorkerInfo = workerInfo;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mWorkerInfo.getId(), mWorkerInfo.getAddress());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WorkerInfoIdentity anotherO = (WorkerInfoIdentity) o;
      return mWorkerInfo.getAddress().equals(anotherO.mWorkerInfo.getAddress())
          && mWorkerInfo.getId() == anotherO.mWorkerInfo.getId();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("id", mWorkerInfo.getId())
          .add("address", mWorkerInfo.getAddress())
          .toString();
    }
  }

  /**
   * Worker information hub.
   */
  @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
      justification = "Already performed null check")
  public class WorkerInfoHub {
    public Map<WorkerInfoIdentity, CloseableResource<BlockWorkerClient>>
        mActiveWorkers = ImmutableMap.of();
    private final WorkerProvider mWorkerProvider;

    /**
     * Constructor.
     * @param scheduler scheduler
     * @param workerProvider worker provider
     */
    public WorkerInfoHub(Scheduler scheduler, WorkerProvider workerProvider) {
      mWorkerProvider = workerProvider;
    }

    private final Map<WorkerInfoIdentity, BoundedPriorityBlockingQueue<Task>> mWorkerToTaskQ
        = new ConcurrentHashMap<>();

    /**
     * Kick stark tasks for each worker task q.
     */
    public void kickStartTasks() {
      // Kick off one task for each worker
      mWorkerToTaskQ.forEach((workerInfo, tasksQ) -> {
        LOG.debug("Kick start task for worker:{}, taskQ size:{}",
            workerInfo.mWorkerInfo.getAddress().getHost(),
            tasksQ.size());
        CloseableResource<BlockWorkerClient> blkWorkerClientResource
            = mActiveWorkers.get(workerInfo);
        Task task = tasksQ.peek();
        // only make sure 1 task is running at the time
        if (task == null || task.getResponseFuture() != null) {
          LOG.debug("head task is {}", (task == null) ? "NULL" : "already running");
          return;
        }
        if (blkWorkerClientResource == null) {
          LOG.warn("Didn't find corresponding BlockWorkerClient for workerInfo:{}",
              workerInfo);
          task.getJob().onWorkerUnavailable(task);
          return;
        }
        task.execute(blkWorkerClientResource.get(), workerInfo.mWorkerInfo);
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
            tasksQ.remove(task);
            mJobToRunningTasks.compute(job, (k, v) -> {
              if (v == null) {
                return null;
              }
              v.remove(task);
              return v;
            });
          }
        }, mSchedulerExecutor);
      });
    }

    /**
     * Enqueue task for worker.
     * @param workerInfo the worker
     * @param task the task
     * @return whether the task is enqueued successfully
     */
    public boolean enqueueTaskForWorker(@Nullable WorkerInfo workerInfo, Task task) {
      if (workerInfo == null) {
        return false;
      }
      BoundedPriorityBlockingQueue workerTaskQ = mWorkerToTaskQ
          .computeIfAbsent(new WorkerInfoIdentity(workerInfo),
              k -> new BoundedPriorityBlockingQueue<>(MAX_TASK_PER_WORKER));
      if (!workerTaskQ.offer(task)) {
        LOG.debug("Exceeded maximum task per q[{}] for worker:{}",
            MAX_TASK_PER_WORKER, new WorkerInfoIdentity(workerInfo));
        return false;
      }
      ConcurrentHashSet<Task<?>> tasks = mJobToRunningTasks.computeIfAbsent(task.getJob(),
          j -> new ConcurrentHashSet<>());
      tasks.add(task);
      return true;
    }

    /**
     * @return the worker to task queue
     */
    public Map<WorkerInfoIdentity, BoundedPriorityBlockingQueue<Task>> getWorkerToTaskQ() {
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
      Set<WorkerInfoIdentity> workerInfoIds;
      try {
        try {
          workerInfoIds = ImmutableSet.copyOf(mWorkerProvider.getWorkerInfos()).stream()
              .map(x -> new WorkerInfoIdentity(x)).collect(Collectors.toSet());
        } catch (AlluxioRuntimeException e) {
          LOG.warn("Failed to get worker info, using existing worker infos of {} workers",
              mActiveWorkers.size());
          return;
        }
        if (workerInfoIds.size() == mActiveWorkers.size()
            && workerInfoIds.containsAll(mActiveWorkers.keySet())) {
          return;
        }

        ImmutableMap.Builder<WorkerInfoIdentity, CloseableResource<BlockWorkerClient>>
            updatedWorkers = ImmutableMap.builder();
        for (WorkerInfoIdentity workerInfoId : workerInfoIds) {
          try {
            if (mActiveWorkers.get(workerInfoId) != null) {
              CloseableResource<BlockWorkerClient> workerClient = Preconditions.checkNotNull(
                  mActiveWorkers.get(workerInfoId));
              updatedWorkers.put(workerInfoId, workerClient);
            } else {
              updatedWorkers.put(workerInfoId, mWorkerProvider.getWorkerClient(
                  workerInfoId.mWorkerInfo.getAddress()));
            }
          } catch (AlluxioRuntimeException e) {
            LOG.warn("Updating worker {} address failed",
                workerInfoId.mWorkerInfo.getAddress(), e);
            // skip the worker if we cannot obtain a client
          }
        }
        // Close clients connecting to lost workers
        for (Map.Entry<WorkerInfoIdentity, CloseableResource<BlockWorkerClient>> entry :
            mActiveWorkers.entrySet()) {
          WorkerInfoIdentity workerInfoId = entry.getKey();
          if (!workerInfoIds.contains(workerInfoId)) {
            CloseableResource<BlockWorkerClient> resource = entry.getValue();
            resource.close();
            LOG.debug("Closed BlockWorkerClient to lost worker {}", workerInfoId);
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
   * Job/Tasks stats.
   */
  public static class SchedulerStats {
    public Map<Job, List<String>> mRunningJobToTasksStat = new HashMap<>();
    public Map<Job, String> mExistingJobAndProgresses = new HashMap<>();
    public Map<String, String> mWorkerQInfos = new HashMap<>();
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
    for (Map.Entry<WorkerInfoIdentity, BoundedPriorityBlockingQueue<Task>> entry :
        mWorkerInfoHub.getWorkerToTaskQ().entrySet()) {
      String tasks = String.join(",",
          entry.getValue().stream().map(x ->
                  "Job:" + x.getJob().getJobId() + ":Task:" + x.getTaskId())
              .collect(Collectors.toList()));
      schedulerStats.mWorkerQInfos.put(entry.getKey().toString(), tasks);
    }
    return schedulerStats;
  }
}
