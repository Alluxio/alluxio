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
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.job.JobDescription;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The Scheduler which controls jobs. It's not thread-safe since start and stop
 * method is not thread-safe. But we should only have one thread call these two method.
 */
@ThreadSafe
public final class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
  private static final int CAPACITY = 100;
  private static final long WORKER_UPDATE_INTERVAL = Configuration.getMs(
      PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME);
  private static final int EXECUTOR_SHUTDOWN_MS = 10 * Constants.SECOND_MS;
  private final Map<JobDescription, Job<?>>
      mExistingJobs = new ConcurrentHashMap<>();
  private final Map<Job<?>, Set<WorkerInfo>> mRunningTasks = new ConcurrentHashMap<>();
  private final JobMetaStore mJobMetaStore;
  // initial thread in start method since we would stop and start thread when gainPrimacy
  private ScheduledExecutorService mSchedulerExecutor;
  private volatile boolean mRunning = false;
  private Map<WorkerInfo, CloseableResource<BlockWorkerClient>> mActiveWorkers = ImmutableMap.of();
  private final WorkerProvider mWorkerProvider;

  /**
   * Constructor.
   *
   * @param workerProvider   workerProvider
   * @param jobMetaStore     jobMetaStore
   */
  public Scheduler(WorkerProvider workerProvider, JobMetaStore jobMetaStore) {
    mWorkerProvider = workerProvider;
    mJobMetaStore = jobMetaStore;
  }

  /**
   * Start scheduler.
   */
  public void start() {
    if (!mRunning) {
      retrieveJobs();
      mSchedulerExecutor = Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("scheduler", false));
      mSchedulerExecutor.scheduleAtFixedRate(this::updateWorkers, 0, WORKER_UPDATE_INTERVAL,
          TimeUnit.MILLISECONDS);
      mSchedulerExecutor.scheduleWithFixedDelay(this::processJobs, 0, 100, TimeUnit.MILLISECONDS);
      mSchedulerExecutor.scheduleWithFixedDelay(this::cleanupStaleJob, 1, 1, TimeUnit.HOURS);
      mRunning = true;
    }
  }

  private void retrieveJobs() {
    for (Job<?> job : mJobMetaStore.getJobs()) {
      mExistingJobs.put(job.getDescription(), job);
      if (job.isDone()) {
        mRunningTasks.remove(job);
      }
      else {
        mRunningTasks.put(job, new HashSet<>());
      }
    }
  }

  /**
   * Stop scheduler.
   */
  public void stop() {
    if (mRunning) {
      mActiveWorkers.values().forEach(CloseableResource::close);
      mActiveWorkers = ImmutableMap.of();
      ThreadUtils.shutdownAndAwaitTermination(mSchedulerExecutor, EXECUTOR_SHUTDOWN_MS);
      mRunning = false;
    }
  }

  /**
   * Submit a job.
   * @param job the job
   * @return true if the job is new, false if the job has already been submitted
   */
  public boolean submitJob(Job<?> job) {
    Job<?> existingJob = mExistingJobs.get(job.getDescription());
    if (existingJob != null && !existingJob.isDone()) {
      updateExistingJob(job, existingJob);
      return false;
    }

    if (mRunningTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException(
          "Too many jobs running, please submit later.", true);
    }
    mJobMetaStore.updateJob(job);
    mExistingJobs.put(job.getDescription(), job);
    mRunningTasks.put(job, new HashSet<>());
    LOG.debug(format("start job: %s", job));
    return true;
  }

  private void updateExistingJob(Job<?> newJob, Job<?> existingJob) {
    existingJob.updateJob(newJob);
    mJobMetaStore.updateJob(existingJob);
    LOG.debug(format("updated existing job: %s from %s", existingJob, newJob));
    if (existingJob.getJobState() == JobState.STOPPED) {
      existingJob.setJobState(JobState.RUNNING);
      mRunningTasks.put(existingJob, new HashSet<>());
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
      existingJob.setJobState(JobState.STOPPED);
      mJobMetaStore.updateJob(existingJob);
      // leftover tasks in mRunningTasks would be removed by scheduling thread.
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
   */
  public String getJobProgress(
      JobDescription jobDescription,
      JobProgressReportFormat format,
      boolean verbose) {
    Job<?> job = mExistingJobs.get(jobDescription);
    if (job == null) {
      throw new NotFoundRuntimeException(format("%s cannot be found.", jobDescription));
    }
    return job.getProgress(format, verbose);
  }

  /**
   * Get active workers.
   * @return active workers
   */
  @VisibleForTesting
  public Map<WorkerInfo, CloseableResource<BlockWorkerClient>> getActiveWorkers() {
    return mActiveWorkers;
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
        if (mActiveWorkers.containsKey(workerInfo)) {
          updatedWorkers.put(workerInfo, mActiveWorkers.get(workerInfo));
        }
        else {
          try {
            updatedWorkers.put(workerInfo,
                mWorkerProvider.getWorkerClient(workerInfo.getAddress()));
          } catch (AlluxioRuntimeException e) {
            // skip the worker if we cannot obtain a client
          }
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
    mRunningTasks.forEach(this::processJob);
  }

  private void processJob(Job<?> job, Set<WorkerInfo> runningWorkers) {
    try {
      if (!job.isRunning()) {
        try {
          mJobMetaStore.updateJob(job);
        }
        catch (UnavailableRuntimeException e) {
          // This should not happen because the scheduler should not be started while master is
          // still processing journal entries. However, if it does happen, we don't want to throw
          // exception in a task running on scheduler thead. So just ignore it and hopefully later
          // retry will work.
          LOG.error("error writing to journal when processing job", e);
        }
        mRunningTasks.remove(job);
        return;
      }
      if (!job.isHealthy()) {
        job.failJob(new InternalRuntimeException("Job failed because it's not healthy."));
        return;
      }

      // If there are new workers, schedule job onto new workers
      mActiveWorkers.forEach((workerInfo, workerClient) -> {
        if (!runningWorkers.contains(workerInfo) && scheduleTask(job, workerInfo, runningWorkers,
            workerClient)) {
          runningWorkers.add(workerInfo);
        }
      });

      if (runningWorkers.isEmpty() && job.isCurrentPassDone()) {
        if (job.needVerification()) {
          job.initiateVerification();
        }
        else {
          if (job.isHealthy()) {
            job.setJobState(JobState.SUCCEEDED);
          }
          else {
            job.failJob(new InternalRuntimeException("Job failed because it's not healthy."));
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

  private boolean scheduleTask(
      @SuppressWarnings("rawtypes") Job job,
      WorkerInfo workerInfo,
      Set<WorkerInfo> livingWorkers,
      CloseableResource<BlockWorkerClient> workerClient) {
    if (!job.isRunning()) {
      return false;
    }
    Optional<Task<?>> task;
    try {
      task = job.getNextTask(workerInfo);
    } catch (AlluxioRuntimeException e) {
      LOG.warn(format("error getting next task for job %s", job), e);
      if (!e.isRetryable()) {
        job.failJob(e);
      }
      return false;
    }
    if (!task.isPresent()) {
      return false;
    }
    Task<?> currentTask = task.get();
    currentTask.execute(workerClient.get());
    currentTask.getResponseFuture().addListener(() -> {
      try {
        if (!job.processResponse(currentTask)) {
          livingWorkers.remove(workerInfo);
        }
        // Schedule next batch for healthy job
        if (job.isHealthy()) {
          if (mActiveWorkers.containsKey(workerInfo)) {
            if (!scheduleTask(job, workerInfo, livingWorkers, mActiveWorkers.get(workerInfo))) {
              livingWorkers.remove(workerInfo);
            }
          }
          else {
            livingWorkers.remove(workerInfo);
          }
        }
      } catch (Exception e) {
        // Unknown exception. This should not happen, but if it happens we don't want to lose the
        // scheduler thread, thus catching it here. Any exception surfaced here should be properly
        // handled.
        LOG.error("Unexpected exception thrown in response future listener.", e);
        job.failJob(new InternalRuntimeException(e));
      }
    }, mSchedulerExecutor);
    return true;
  }
}
