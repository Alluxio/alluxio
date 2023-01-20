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

package alluxio.master.file.loadmanager;

import static java.lang.String.format;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.runtime.UnauthenticatedRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.LoadProgressReportFormat;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CheckAccessContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Job;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The Load manager which controls load operations. It's not thread-safe since start and stop
 * method is not thread-safe. But we should only have one thread call these two method.
 */
@ThreadSafe
public final class LoadManager implements Journaled {
  private static final Logger LOG = LoggerFactory.getLogger(LoadManager.class);
  private static final int CAPACITY = 100;
  private static final long WORKER_UPDATE_INTERVAL = Configuration.getMs(
      PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME);
  private static final int EXECUTOR_SHUTDOWN_MS = 10 * Constants.SECOND_MS;
  private final FileSystemMaster mFileSystemMaster;
  private final FileSystemContext mContext;
  private final Map<String, LoadJob> mLoadJobs = new ConcurrentHashMap<>();
  private final Map<LoadJob, Set<WorkerInfo>> mRunningTasks = new ConcurrentHashMap<>();
  // initial thread in start method since we would stop and start thread when gainPrimacy
  private ScheduledExecutorService mLoadScheduler;
  private volatile boolean mRunning = false;
  private Map<WorkerInfo, CloseableResource<BlockWorkerClient>> mActiveWorkers = ImmutableMap.of();

  /**
   * Constructor.
   * @param fileSystemMaster fileSystemMaster
   */
  public LoadManager(FileSystemMaster fileSystemMaster) {
    this(fileSystemMaster, FileSystemContext.create());
  }

  /**
   * Constructor.
   * @param fileSystemMaster fileSystemMaster
   * @param context fileSystemContext
   */
  @VisibleForTesting
  public LoadManager(FileSystemMaster fileSystemMaster, FileSystemContext context) {
    mFileSystemMaster = fileSystemMaster;
    mContext = context;
  }

  /**
   * Start load manager.
   */
  public void start() {
    if (!mRunning) {
      mLoadScheduler = Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("load-manager-scheduler", false));
      mLoadScheduler.scheduleAtFixedRate(this::updateWorkers, 0, WORKER_UPDATE_INTERVAL,
          TimeUnit.MILLISECONDS);
      mLoadScheduler.scheduleWithFixedDelay(this::processJobs, 0, 100, TimeUnit.MILLISECONDS);
      mLoadScheduler.scheduleWithFixedDelay(this::cleanupStaleJob, 1, 1, TimeUnit.HOURS);
      mRunning = true;
    }
  }

  /**
   * Stop load manager.
   */
  public void stop() {
    if (mRunning) {
      mActiveWorkers.values().forEach(CloseableResource::close);
      mActiveWorkers = ImmutableMap.of();
      ThreadUtils.shutdownAndAwaitTermination(mLoadScheduler, EXECUTOR_SHUTDOWN_MS);
      mRunning = false;
    }
  }

  /**
   * Submit a load job.
   * @param loadPath alluxio directory path to load into Alluxio
   * @param bandwidth bandwidth allocated to this load
   * @param usePartialListing whether to use partial listing or not
   * @param verificationEnabled whether to run verification step or not
   * @return true if the job is new, false if the job has already been submitted
   */
  public boolean submitLoad(String loadPath, OptionalLong bandwidth,
      boolean usePartialListing, boolean verificationEnabled) {
    try {
      mFileSystemMaster.checkAccess(new AlluxioURI(loadPath), CheckAccessContext.defaults());
    } catch (FileDoesNotExistException | InvalidPathException e) {
      throw new NotFoundRuntimeException(e);
    } catch (AccessControlException e) {
      throw new UnauthenticatedRuntimeException(e);
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
    return submitLoad(new LoadJob(
        loadPath,
        Optional.ofNullable(AuthenticatedClientUser.getOrNull()).map(User::getName), UUID
        .randomUUID().toString(), bandwidth,
        usePartialListing,
        verificationEnabled));
  }

  /**
   * Submit a load job.
   * @param loadJob the load job
   * @return true if the job is new, false if the job has already been submitted
   */
  @VisibleForTesting
  public boolean submitLoad(LoadJob loadJob) {
    LoadJob existingJob = mLoadJobs.get(loadJob.getPath());
    if (existingJob != null && !existingJob.isDone()) {
      updateExistingJob(loadJob, existingJob);
      return false;
    }

    if (mRunningTasks.size() >= CAPACITY) {
      throw new ResourceExhaustedRuntimeException(
          "Too many load jobs running, please submit later.", true);
    }
    writeJournal(loadJob);
    mLoadJobs.put(loadJob.getPath(), loadJob);
    mRunningTasks.put(loadJob, new HashSet<>());
    LOG.debug(format("start job: %s", loadJob));
    return true;
  }

  private void updateExistingJob(LoadJob loadJob, LoadJob existingJob) {
    existingJob.updateBandwidth(loadJob.getBandwidth());
    existingJob.setVerificationEnabled(loadJob.isVerificationEnabled());
    writeJournal(existingJob);
    LOG.debug(format("updated existing job: %s from %s", existingJob, loadJob));
    if (existingJob.getJobState() == LoadJobState.STOPPED) {
      existingJob.setJobState(LoadJobState.LOADING);
      mRunningTasks.put(existingJob, new HashSet<>());
    }
  }

  /**
   * Stop a load job.
   * @param loadPath alluxio directory path to load into Alluxio
   * @return true if the job is stopped, false if the job does not exist or has already finished
   */
  public boolean stopLoad(String loadPath) {
    LoadJob existingJob = mLoadJobs.get(loadPath);
    if (existingJob != null && existingJob.isRunning()) {
      existingJob.setJobState(LoadJobState.STOPPED);
      writeJournal(existingJob);
      // leftover tasks in mLoadTasks would be removed by scheduling thread.
      return true;
    }
    return false;
  }

  /**
   * Get the load job's progress report.
   * @param loadPath alluxio directory path of the load job
   * @param format progress report format
   * @param verbose whether to include details on failed files and failures
   * @return the progress report
   */
  public String getLoadProgress(
      String loadPath,
      LoadProgressReportFormat format,
      boolean verbose) {
    LoadJob job = mLoadJobs.get(loadPath);
    if (job == null) {
      throw new NotFoundRuntimeException(format("Load for path %s cannot be found.", loadPath));
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
    mLoadJobs.entrySet().removeIf(job -> !job.getValue().isRunning()
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
        // TODO(jianjian): need api for healthy worker instead
        workerInfos = ImmutableSet.copyOf(mFileSystemMaster.getWorkerInfoList());
      } catch (UnavailableException e) {
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
                mContext.acquireBlockWorkerClient(workerInfo.getAddress()));
          } catch (IOException e) {
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
   * Get load jobs.
   * @return load jobs
   */
  @VisibleForTesting
  public Map<String, LoadJob> getLoadJobs() {
    return mLoadJobs;
  }

  private void processJobs() {
    if (Thread.currentThread().isInterrupted()) {
      return;
    }
    mRunningTasks.forEach(this::processJob);
  }

  private void processJob(LoadJob loadJob, Set<WorkerInfo> loadWorkers) {
    try {
      if (!loadJob.isRunning()) {
        try {
          writeJournal(loadJob);
        }
        catch (UnavailableRuntimeException e) {
          // This should not happen because the load manager should not be started while master is
          // still processing journal entries. However, if it does happen, we don't want to throw
          // exception in a task running on scheduler thead. So just ignore it and hopefully later
          // retry will work.
          LOG.error("error writing to journal when processing job", e);
        }
        mRunningTasks.remove(loadJob);
        return;
      }
      if (!loadJob.isHealthy()) {
        loadJob.failJob(new InternalRuntimeException("Too many block load failed."));
        return;
      }

      // If there are new workers, schedule job onto new workers
      mActiveWorkers.forEach((workerInfo, workerClient) -> {
        if (!loadWorkers.contains(workerInfo) && scheduleBatch(loadJob, workerInfo, loadWorkers,
            workerClient, loadJob.getBatchSize())) {
          loadWorkers.add(workerInfo);
        }
      });

      if (loadWorkers.isEmpty() && loadJob.isCurrentLoadDone()) {
        if (loadJob.getCurrentBlockCount() > 0 && loadJob.isVerificationEnabled()) {
          loadJob.initiateVerification();
        }
        else {
          if (loadJob.isHealthy()) {
            loadJob.setJobState(LoadJobState.SUCCEEDED);
            JOB_LOAD_SUCCESS.inc();
          }
          else {
            loadJob.failJob(new InternalRuntimeException("Too many block load failed."));
          }
        }
      }
    } catch (Exception e) {
      // Unknown exception. This should not happen, but if it happens we don't want to lose the
      // scheduler thread, thus catching it here. Any exception surfaced here should be properly
      // handled.
      LOG.error("Unexpected exception thrown in processJob.", e);
      loadJob.failJob(new InternalRuntimeException(e));
    }
  }

  // Returns false if the whole task fails
  private boolean processResponse(
      LoadJob load,
      LoadRequest request,
      ListenableFuture<LoadResponse> responseFuture) {
    try {
      long totalBytes = request.getBlocksList().stream()
          .map(Block::getLength)
          .reduce(Long::sum)
          .orElse(0L);
      LoadResponse response = responseFuture.get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.debug(format("Get failure from worker: %s", response.getBlockStatusList()));
        for (BlockStatus status : response.getBlockStatusList()) {
          totalBytes -= status.getBlock().getLength();
          if (!load.isHealthy() || !status.getRetryable() || !load.addBlockToRetry(
              status.getBlock())) {
            load.addBlockFailure(status.getBlock(), status.getMessage(), status.getCode());
          }
        }
      }
      load.addLoadedBytes(totalBytes);
      JOB_LOAD_BLOCK_COUNT.inc(
          request.getBlocksCount() - response.getBlockStatusCount());
      JOB_LOAD_BLOCK_SIZE.inc(totalBytes);
      JOB_LOAD_RATE.mark(totalBytes);
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      LOG.warn("exception when trying to get load response.", e.getCause());
      for (Block block : request.getBlocksList()) {
        if (load.isHealthy()) {
          load.addBlockToRetry(block);
        }
        else {
          AlluxioRuntimeException exception = AlluxioRuntimeException.from(e.getCause());
          load.addBlockFailure(block, exception.getMessage(), exception.getStatus().getCode()
                                                                       .value());
        }
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("Task get canceled and will retry.", e);
      request.getBlocksList().forEach(load::addBlockToRetry);
      return true;
    }
    catch (InterruptedException e) {
      request.getBlocksList().forEach(load::addBlockToRetry);
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  private boolean scheduleBatch(
      LoadJob load,
      WorkerInfo workerInfo,
      Set<WorkerInfo> loadWorkers,
      CloseableResource<BlockWorkerClient> workerClient,
      int batchSize) {
    if (!load.isRunning()) {
      return false;
    }
    List<Block> batch;
    try {
      batch = load.getNextBatch(mFileSystemMaster, batchSize);
    } catch (AlluxioRuntimeException e) {
      LOG.warn(format("error getting next batch for load %s", load), e);
      if (!e.isRetryable()) {
        load.failJob(e);
      }
      return false;
    }

    if (batch.isEmpty()) {
      return false;
    }

    LoadRequest request = buildRequest(batch, load.getUser(), load.getJobId(), load.getBandwidth());
    ListenableFuture<LoadResponse> responseFuture = workerClient.get().load(request);
    responseFuture.addListener(() -> {
      try {
        if (!processResponse(load, request, responseFuture)) {
          loadWorkers.remove(workerInfo);
        }
        // Schedule next batch for healthy job
        if (load.isHealthy()) {
          if (mActiveWorkers.containsKey(workerInfo)) {
            if (!scheduleBatch(load, workerInfo, loadWorkers, mActiveWorkers.get(workerInfo),
                load.getBatchSize())) {
              loadWorkers.remove(workerInfo);
            }
          }
          else {
            loadWorkers.remove(workerInfo);
          }
        }
      } catch (Exception e) {
        // Unknown exception. This should not happen, but if it happens we don't want to lose the
        // scheduler thread, thus catching it here. Any exception surfaced here should be properly
        // handled.
        LOG.error("Unexpected exception thrown in response future listener.", e);
        load.failJob(new InternalRuntimeException(e));
      }
    }, mLoadScheduler);
    return true;
  }

  private void writeJournal(LoadJob job) {
    try (JournalContext context = mFileSystemMaster.createJournalContext()) {
      context.append(job.toJournalEntry());
    } catch (UnavailableException e) {
      throw new UnavailableRuntimeException(
          "There is an ongoing backup running, please submit later", e);
    }
  }

  private LoadRequest buildRequest(List<Block> blockBatch, Optional<String> user, String tag,
      OptionalLong bandwidth) {
    LoadRequest.Builder request = LoadRequest
        .newBuilder()
        .addAllBlocks(blockBatch);
    UfsReadOptions.Builder options =
        UfsReadOptions.newBuilder().setTag(tag).setPositionShort(false);
    if (bandwidth.isPresent()) {
      options.setBandwidth(bandwidth.getAsLong() / mActiveWorkers.size());
    }
    user.ifPresent(options::setUser);
    return request.setOptions(options.build()).build();
  }

  @Override
  public CloseableIterator<Journal.JournalEntry> getJournalEntryIterator() {
    return CloseableIterator.noopCloseable(
        Iterators.transform(mLoadJobs.values().iterator(), LoadJob::toJournalEntry));
  }

  @Override
  public boolean processJournalEntry(Journal.JournalEntry entry) {
    if (!entry.hasLoadJob()) {
      return false;
    }
    Job.LoadJobEntry loadJobEntry = entry.getLoadJob();
    LoadJob job = LoadJob.fromJournalEntry(loadJobEntry);
    mLoadJobs.put(loadJobEntry.getLoadPath(), job);
    if (job.isDone()) {
      mRunningTasks.remove(job);
    }
    else {
      mRunningTasks.put(job, new HashSet<>());
    }
    return true;
  }

  @Override
  public void resetState()
  {
    mLoadJobs.clear();
    mRunningTasks.clear();
  }

  @Override
  public CheckpointName getCheckpointName()
  {
    return CheckpointName.LOAD_MANAGER;
  }

  // metrics
  public static final Counter JOB_LOAD_SUCCESS =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_SUCCESS.getName());
  public static final Counter JOB_LOAD_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FAIL.getName());
  public static final Counter JOB_LOAD_BLOCK_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_COUNT.getName());
  public static final Counter JOB_LOAD_BLOCK_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_FAIL.getName());
  public static final Counter JOB_LOAD_BLOCK_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_SIZE.getName());
  public static final Meter JOB_LOAD_RATE =
          MetricsSystem.meter(MetricKey.MASTER_JOB_LOAD_RATE.getName());
}
