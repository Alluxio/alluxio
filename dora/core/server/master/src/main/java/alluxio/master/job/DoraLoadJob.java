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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadFailure;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.FormatUtils;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Load job that loads a file or a directory into Alluxio.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class DoraLoadJob extends AbstractJob<DoraLoadJob.DoraLoadTask> {
  private static final Logger LOG = LoggerFactory.getLogger(DoraLoadJob.class);
  public static final String TYPE = "load";
  private static final int RETRY_BLOCK_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_BLOCK_CAPACITY;
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);

  // Job configurations
  private final String mLoadPath;
  private OptionalLong mBandwidth;
  private boolean mUsePartialListing;
  private boolean mVerificationEnabled;

  // Job states
  private final Queue<LoadSubTask> mRetrySubTasks = new ArrayDeque<>();
  private final Map<LoadSubTask, Integer> mRetryCount = new ConcurrentHashMap<>();
  private final Map<String, String> mFailedFiles = new HashMap<>();
  private final AtomicLong mSkippedBlocksCount = new AtomicLong();
  private final AtomicLong mProcessedInodesCount = new AtomicLong();
  private final AtomicLong mLoadedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mSkippedByteCount = new AtomicLong();
  private final AtomicLong mProcessingFileCount = new AtomicLong();
  //including retry, do accurate stats later.
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private Iterator<UfsStatus> mUfsStatusIterator;
  private AtomicBoolean mPreparingTasks = new AtomicBoolean(false);
  private final UnderFileSystem mUfs;
  private boolean mLoadMetadataOnly;
  private static final double FAILURE_RATIO_THRESHOLD = Configuration.getDouble(
      PropertyKey.MASTER_DORA_LOAD_JOB_TOTAL_FAILURE_RATIO_THRESHOLD);
  private static final int FAILURE_COUNT_THRESHOLD = Configuration.getInt(
      PropertyKey.MASTER_DORA_LOAD_JOB_TOTAL_FAILURE_COUNT_THRESHOLD);
  private static final int RETRY_ATTEMPT_THRESHOLD = Configuration.getInt(
      PropertyKey.MASTER_DORA_LOAD_JOB_RETRIES);
  private final boolean mSkipIfExists;
  private final long mVirtualBlockSize = Configuration.getBytes(
      PropertyKey.DORA_READ_VIRTUAL_BLOCK_SIZE);
  private Iterator<LoadSubTask> mCurrentSubTaskIterator;

  /**
   * Constructor.
   *
   * @param path                file path
   * @param user                user for authentication
   * @param jobId               job identifier
   * @param bandwidth           bandwidth
   * @param usePartialListing   whether to use partial listing
   * @param verificationEnabled whether to verify the job after loaded
   * @param loadMetadataOnly    if set to true, only metadata will be loaded without loading
   * @param skipIfExists        skip if exists
   * @param ufsStatusIterator   ufsStatus iterable
   * @param ufs                 under file system
   */
  public DoraLoadJob(String path, Optional<String> user, String jobId, OptionalLong bandwidth,
      boolean usePartialListing, boolean verificationEnabled, boolean loadMetadataOnly,
      boolean skipIfExists, Iterator<UfsStatus> ufsStatusIterator, UnderFileSystem ufs) {
    super(user, jobId, new HashBasedWorkerAssignPolicy());
    mLoadPath = requireNonNull(path, "path is null");
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    mUfs = ufs;
    mLoadMetadataOnly = loadMetadataOnly;
    mSkipIfExists = skipIfExists;
    mUfsStatusIterator = ufsStatusIterator;
    LOG.info("DoraLoadJob for {} created.", path);
  }

  /**
   * Prepare next set of tasks waiting to be kicked off.
   * it is made sure only one thread should be calling this.
   * @param workers
   * @return list of DoraLoadTask
   */
  private List<DoraLoadTask> prepareNextTasks(Collection<WorkerInfo> workers) {
    LOG.debug("Preparing next set of tasks for jobId:{}", mJobId);
    int workerNum = workers.size();
    ImmutableList.Builder<LoadSubTask> batchBuilder = ImmutableList.builder();
    if (mCurrentSubTaskIterator == null) {
      if (mUfsStatusIterator.hasNext()) {
        mCurrentSubTaskIterator = initSubTaskIterator();
      }
      else {
        return Collections.emptyList();
      }
    }
    int i = 0;
    int startRetryListSize = mRetrySubTasks.size();
    int numSubTasks = 0;
    while (numSubTasks < RETRY_THRESHOLD
        && i++ < startRetryListSize && mRetrySubTasks.peek() != null) {
      LoadSubTask subTask = mRetrySubTasks.poll();
      String path = subTask.getUfsPath();
      try {
        mUfs.getStatus(path);
        batchBuilder.add(subTask);
        ++numSubTasks;
      } catch (IOException | AlluxioRuntimeException e) {
        // The previous list or get might contain stale file metadata.
        // For example, if a file gets removed before the worker actually loads it,
        // the load will fail and the scheduler will retry.
        // In such case, a FileNotFoundException might be thrown when we attempt to
        // get the file status again, and we simply ignore that file.
        if (!(e instanceof FileNotFoundException || e instanceof NotFoundRuntimeException)) {
          mRetrySubTasks.offer(subTask);
        }
      }
    }
    while (numSubTasks < BATCH_SIZE * workerNum) {
      if (!mCurrentSubTaskIterator.hasNext()) {
        if (!mUfsStatusIterator.hasNext()) {
          break;
        }
        else {
          mCurrentSubTaskIterator = initSubTaskIterator();
        }
      }
      batchBuilder.add(mCurrentSubTaskIterator.next());
      numSubTasks++;
    }
    ImmutableList<LoadSubTask> subTasks = batchBuilder.build();
    Map<WorkerInfo, DoraLoadTask> workerToTaskMap = pickWorkerForSubTasks(workers, subTasks);
    if (workerToTaskMap.isEmpty()) {
      return Collections.unmodifiableList(new ArrayList<>());
    }
    List<DoraLoadTask> tasks = workerToTaskMap.values().stream()
        .collect(Collectors.toList());
    LOG.debug("prepared tasks:{}", tasks);
    return tasks;
  }

  private Iterator<LoadSubTask> initSubTaskIterator() {
    return createSubTasks(mUfsStatusIterator.next()).listIterator();
  }

  private Map<WorkerInfo, DoraLoadTask> pickWorkerForSubTasks(
      Collection<WorkerInfo> workers, ImmutableList<LoadSubTask> subTasks) {
    Map<WorkerInfo, DoraLoadTask> workerToTaskMap = new HashMap<>();
    for (LoadSubTask subtask : subTasks) {
      // NOTE: active workers may not reflect all workers at start up,
      // but hash based policy will deterministically pick only among
      // current recognized active workers -> will change in future
      // once membership module is ready to tell all registered workers
      WorkerInfo pickedWorker = mWorkerAssignPolicy.pickAWorker(
          subtask.asString(), workers);
      if (pickedWorker == null) {
        mRetrySubTasks.offer(subtask);
        continue;
      }
      DoraLoadTask task = workerToTaskMap.computeIfAbsent(pickedWorker,
          w -> {
            DoraLoadTask t = new DoraLoadTask();
            t.setMyRunningWorker(pickedWorker);
            t.setJob(this);
            return t;
          });
      task.addSubTask(subtask);
      if (!mLoadMetadataOnly) {
        mTotalByteCount.addAndGet(subtask.getLength());
      }
      mProcessingFileCount.addAndGet(1);
    }
    return workerToTaskMap;
  }

  private List<LoadSubTask> createSubTasks(UfsStatus ufsStatus) {
    List<LoadSubTask> subTasks = new ArrayList<>();
        // add load metadata task
    subTasks.add(new LoadMetadataSubTask(ufsStatus, mVirtualBlockSize));
    if (mLoadMetadataOnly || ufsStatus.isDirectory()
        || ufsStatus.asUfsFileStatus().getContentLength() == 0) {
      return subTasks;
    }
    long contentLength = ufsStatus.asUfsFileStatus().getContentLength();
    if (mVirtualBlockSize > 0) {
      int numBlocks = (int) (contentLength / mVirtualBlockSize) + 1;
      for (int i = 0; i < numBlocks; i++) {
        long offset = mVirtualBlockSize * i;
        long leftover = contentLength - offset;
        subTasks.add(new LoadDataSubTask(ufsStatus, mVirtualBlockSize, offset,
            Math.min(leftover, mVirtualBlockSize)));
      }
    }
    else {
      subTasks.add(new LoadDataSubTask(ufsStatus, mVirtualBlockSize, 0, contentLength));
    }
    return subTasks;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mLoadPath;
  }

  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(mLoadPath).setType(TYPE).build();
  }

  /**
   * Get bandwidth.
   * @return the allocated bandwidth
   */
  public OptionalLong getBandwidth() {
    return mBandwidth;
  }

  /**
   * Set load state to FAILED with given reason.
   * @param reason failure exception
   */
  @Override
  public void failJob(AlluxioRuntimeException reason) {
    setJobState(JobState.FAILED, true);
    mFailedReason = Optional.of(reason);
    JOB_LOAD_FAIL.inc();
    LOG.info("Load Job {} fails with status: {}", mJobId, this);
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED, true);
    JOB_LOAD_SUCCESS.inc();
    LOG.info("Load Job {} succeeds with status {}", mJobId, this);
  }

  /**
   * Add bytes to total loaded bytes.
   * @param bytes bytes to be added to total
   */
  @VisibleForTesting
  public void addLoadedBytes(long bytes) {
    mLoadedByteCount.addAndGet(bytes);
  }

  /**
   * Add files to retry.
   * @param type the error type
   * @param message the error message
   * @param subTask subTask to be retried
   * @return true
   */
  @VisibleForTesting

  public boolean addSubTaskToRetry(LoadSubTask subTask, String type, String message) {
    LOG.debug("Retry file {}", subTask);
    int currentErrorCount = mRetryCount.getOrDefault(subTask, 0);
    if (currentErrorCount >= RETRY_ATTEMPT_THRESHOLD) {
      addFileFailure(subTask.getUfsPath(), type, message);
      mRetryCount.remove(subTask);
      return true;
    }
    mRetryCount.put(subTask, mRetryCount.getOrDefault(subTask, 0) + 1);
    mRetrySubTasks.offer(subTask);
    mTotalFailureCount.incrementAndGet();
    LOAD_FAIL_COUNT.inc();
    return true;
  }

  /**
   * Add failed files.
   * @param fileUfsPath
   * @param message
   * @param type
   */
  @VisibleForTesting
  public void addFileFailure(String fileUfsPath, String type, String message) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the first one.
    mFailedFiles.put(fileUfsPath,
        format("Status code: %s, message: %s", type, message));
    LOAD_FAIL_COUNT.inc();
  }

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    return (new LoadProgressReport(this, verbose)).getReport(format);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoraLoadJob that = (DoraLoadJob) o;
    return Objects.equal(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getDescription());
  }

  @Override
  public boolean isHealthy() {
    long totalFailureCount = mTotalFailureCount.get();
    if (FAILURE_RATIO_THRESHOLD >= 1.0 || FAILURE_COUNT_THRESHOLD < 0) {
      return true;
    }
    return mState != JobState.FAILED
        && totalFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) totalFailureCount / mProcessingFileCount.get() <= FAILURE_RATIO_THRESHOLD;
  }

  @Override
  public boolean isCurrentPassDone() {
    return !mUfsStatusIterator.hasNext()
        && !mCurrentSubTaskIterator.hasNext() && mRetrySubTasks.isEmpty()
        && mRetryTaskList.isEmpty();
  }

  @Override
  public void initiateVerification() {
    // No op for now
  }

  @Override
  public List<DoraLoadTask> getNextTasks(Collection<WorkerInfo> workers) {
    /* Both scheduler thread and worker thread will try to call getNextTasks,
    only one of them needs to do the preparation of next set of tasks and whoever
    wins will do the processjob and kick off those tasks.
     */
    List<DoraLoadTask> list = new ArrayList<>();
    if (mPreparingTasks.compareAndSet(false, true)) {
      try {
        Iterator<DoraLoadTask> it = mRetryTaskList.iterator();
        if (it.hasNext()) {
          DoraLoadTask task = it.next();
          LOG.debug("Re-submit retried DoraLoadTask:{} in getNextTasks.",
              task.getTaskId());
          list.add(task);
          it.remove();
          return Collections.unmodifiableList(list);
        }
        list = prepareNextTasks(workers);
        return Collections.unmodifiableList(list);
      } finally {
        mPreparingTasks.compareAndSet(true, false);
      }
    }
    return list;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("JobId", mJobId)
        .add("Path", mLoadPath)
        .add("User", mUser)
        .add("Bandwidth", mBandwidth)
        .add("UsePartialListing", mUsePartialListing)
        .add("VerificationEnabled", mVerificationEnabled)
        .add("RetrySubTasks", mRetrySubTasks)
        .add("FailedFiles", mFailedFiles)
        .add("StartTime", mStartTime)
        .add("SkippedFileCount", mSkippedBlocksCount)
        .add("ProcessedInodesCount", mProcessedInodesCount)
        .add("LoadedByteCount", mLoadedByteCount)
        .add("TotalFailureCount", mTotalFailureCount)
        .add("SkippedByteCount", mSkippedByteCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("UfsStatusIterator", mUfsStatusIterator)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.LoadJobEntry.Builder jobEntry = alluxio.proto.journal.Job.LoadJobEntry
        .newBuilder()
        .setLoadPath(mLoadPath)
        .setState(JobState.toProto(mState))
        .setPartialListing(mUsePartialListing)
        .setVerify(mVerificationEnabled)
        .setSkipIfExists(mSkipIfExists)
        .setJobId(mJobId);
    mUser.ifPresent(jobEntry::setUser);
    mBandwidth.ifPresent(jobEntry::setBandwidth);
    mEndTime.ifPresent(jobEntry::setEndTime);
    return Journal.JournalEntry
        .newBuilder()
        .setLoadJob(jobEntry.build())
        .build();
  }

  /**
   * Get duration in seconds.
   * @return job duration in seconds
   */
  @VisibleForTesting
  public long getDurationInSec() {
    return (mEndTime.orElse(System.currentTimeMillis()) - mStartTime) / 1000;
  }

  @Override
  public boolean processResponse(DoraLoadTask doraLoadTask) {
    try {
      long totalLoadedBytes = doraLoadTask.getSubTasks().stream()
                                          .map((it) -> (it.getLength()))
                                          .reduce(Long::sum)
                                          .orElse(0L);
      // what if timeout ? job needs to proactively check or task needs to be aware
      LoadFileResponse response = doraLoadTask.getResponseFuture().get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.warn(format("[DistributedLoad] Get failure from worker:%s, failed files:%s",
            doraLoadTask.getMyRunningWorker(), response.getFailuresList()));
        for (LoadFailure failure : response.getFailuresList()) {
          if (failure.getSubtask().hasLoadDataSubtask()) {
            totalLoadedBytes -= failure.getSubtask().getLoadDataSubtask().getLength();
          }
          String status = Status.fromCodeValue(failure.getCode()).toString();
          LoadSubTask subTask = LoadSubTask.from(failure, mVirtualBlockSize);
          if (!isHealthy() || !failure.getRetryable() || !addSubTaskToRetry(subTask, status,
              failure.getMessage())) {
            addFileFailure(
                subTask.getUfsPath(), status, failure.getMessage());
          }
        }
      }
      int totalLoadedInodes = doraLoadTask.getSubTasks().stream()
          .filter(LoadSubTask::isLoadMetadata).collect(Collectors.toList()).size()
          - response.getFailuresList().stream().filter(i -> i.getSubtask().hasLoadMetadataSubtask())
                    .collect(Collectors.toList()).size();
      if (!mLoadMetadataOnly) {
        addLoadedBytes(totalLoadedBytes - response.getBytesSkipped());
        LOAD_FILE_SIZE.inc(totalLoadedBytes);
        LOAD_RATE.mark(totalLoadedBytes);
        LOAD_RATE.mark(totalLoadedBytes);
      }
      mProcessedInodesCount.addAndGet(totalLoadedInodes - response.getNumSkipped());
      LOAD_FILE_COUNT.inc(totalLoadedInodes);
      mSkippedBlocksCount.addAndGet(response.getNumSkipped());
      mSkippedByteCount.addAndGet(response.getBytesSkipped());
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      Throwable cause = e.getCause();
      LOG.warn("exception when trying to get load response.", cause);
      for (LoadSubTask subTask : doraLoadTask.getSubTasks()) {
        AlluxioRuntimeException exception = AlluxioRuntimeException.from(cause);
        if (isHealthy()) {
          addSubTaskToRetry(subTask, exception.getStatus().toString(), exception.getMessage());
        } else {
          addFileFailure(subTask.getUfsPath(),
              exception.getStatus().toString(), exception.getMessage());
        }
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("[DistributedLoad] Task get canceled and will retry.", e);
      doraLoadTask.getSubTasks().forEach(it -> addSubTaskToRetry(it, "CANCELLED", e.getMessage()));
      return true;
    }
    catch (InterruptedException e) {
      doraLoadTask.getSubTasks().forEach(it -> addSubTaskToRetry(it, "ABORTED", e.getMessage()));
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  @Override
  public boolean hasFailure() {
    return !mFailedFiles.isEmpty();
  }

  /**
   * Is verification enabled.
   *
   * @return whether verification is enabled
   */
  @Override
  public boolean needVerification() {
    return false;
  }

  /**
   * Dora load task. It contains a list of subtasks. Each subtask would be executed concurrently
   * on a worker.
   */
  public class DoraLoadTask extends Task<LoadFileResponse> {
    protected List<LoadSubTask> mSubTasks;

    /**
     * Constructor.
     */
    public DoraLoadTask() {
      super(DoraLoadJob.this, DoraLoadJob.this.mTaskIdGenerator.incrementAndGet());
      mSubTasks = new ArrayList<>();
    }

    /**
     * Get files to load of this task.
     * @return list of UfsStatus
     */
    public List<LoadSubTask> getSubTasks() {
      return mSubTasks;
    }

    private void addSubTask(LoadSubTask subTask) {
      mSubTasks.add(subTask);
    }

    @Override
    protected ListenableFuture<LoadFileResponse> run(BlockWorkerClient workerClient) {
      LOG.debug("Start running task:{} on worker:{}", this, getMyRunningWorker());
      LoadFileRequest loadFileReqBuilder = constructRpcRequest();
      return workerClient.loadFile(loadFileReqBuilder);
    }

    private LoadFileRequest constructRpcRequest() {
      LoadFileRequest.Builder loadFileReqBuilder = LoadFileRequest.newBuilder();
      mSubTasks.stream().map(LoadSubTask::toProto).forEach(loadFileReqBuilder::addSubtasks);
      UfsReadOptions.Builder ufsReadOptions = UfsReadOptions
          .newBuilder()
          .setTag(mJobId)
          .setPositionShort(false);
      mUser.ifPresent(ufsReadOptions::setUser);
      loadFileReqBuilder.setOptions(ufsReadOptions);
      loadFileReqBuilder.setSkipIfExists(mSkipIfExists);
      return loadFileReqBuilder.build();
    }

    @Override
    public String toString() {
      final StringBuilder filesBuilder = new StringBuilder();
      getSubTasks().forEach(f -> {
        filesBuilder.append(f.asString() + ",");
      });
      return MoreObjects.toStringHelper(this)
          .add("taskJobType", mMyJob.getClass())
          .add("taskJobId", mMyJob.getJobId())
          .add("taskId", getTaskId())
          .add("taskFiles", filesBuilder.toString())
          .toString();
    }
  }

  private static class LoadProgressReport {
    private final boolean mVerbose;
    private final JobState mJobState;
    private final Long mBandwidth;
    private final boolean mVerificationEnabled;
    private final long mSkippedByteCount;
    private final long mLoadedByteCount;
    private final long mProcessedInodesCount;
    private final Long mTotalByteCount;
    private final Long mThroughput;
    private final double mFailurePercentage;
    private final AlluxioRuntimeException mFailureReason;
    private final long mFailedFileCount;
    private final Map<String, String> mFailedFilesWithReasons;
    private final boolean mSkipIfExists;
    private final boolean mMetadataOnly;

    /**
     * Constructor.
     * @param job the job
     * @param verbose verbose
     */
    public LoadProgressReport(DoraLoadJob job, boolean verbose)
    {
      mVerbose = verbose;
      mJobState = job.mState;
      mBandwidth = job.mBandwidth.isPresent() ? job.mBandwidth.getAsLong() : null;
      mVerificationEnabled = job.mVerificationEnabled;
      mProcessedInodesCount = job.mProcessedInodesCount.get();
      mLoadedByteCount = job.mLoadedByteCount.get();
      if (!job.mUsePartialListing) {
        mTotalByteCount = job.mTotalByteCount.get();
      }
      else {
        mTotalByteCount = null;
      }
      long duration = job.getDurationInSec();
      if (duration > 0) {
        mThroughput = job.mLoadedByteCount.get() / duration;
      }
      else {
        mThroughput = null;
      }
      mFailurePercentage =
          ((double) (job.mTotalFailureCount.get())
              / (mProcessedInodesCount)) * 100;
      mFailureReason = job.mFailedReason.orElse(null);
      mFailedFileCount = job.mFailedFiles.size();
      if (verbose && mFailedFileCount > 0) {
        mFailedFilesWithReasons = job.mFailedFiles;
      } else {
        mFailedFilesWithReasons = null;
      }
      mSkippedByteCount = job.mSkippedByteCount.get();
      mSkipIfExists = job.mSkipIfExists;
      mMetadataOnly = job.mLoadMetadataOnly;
    }

    public String getReport(JobProgressReportFormat format)
    {
      switch (format) {
        case TEXT:
          return getTextReport();
        case JSON:
          return getJsonReport();
        default:
          throw new InvalidArgumentRuntimeException(
              format("Unknown load progress report format: %s", format));
      }
    }

    private String getTextReport() {
      StringBuilder progress = new StringBuilder();
      progress.append(
          format("\tSettings:\tbandwidth: %s\tverify: %s\tmetadata-only: %s%n",
              mBandwidth == null ? "unlimited" : mBandwidth,
              mVerificationEnabled, mMetadataOnly));
      progress.append(format("\tJob State: %s%s%n", mJobState,
          mFailureReason == null
              ? "" : format(
              " (%s: %s)",
              mFailureReason.getClass().getName(),
              mFailureReason.getMessage())));
      if (mVerbose && mFailureReason != null) {
        for (StackTraceElement stack : mFailureReason.getStackTrace()) {
          progress.append(format("\t\t%s%n", stack.toString()));
        }
      }
      progress.append(format("\tInodes Processed: %d%n", mProcessedInodesCount));
      if (!mMetadataOnly) {
        progress.append(format("\tBytes Loaded: %s%s%n",
            FormatUtils.getSizeFromBytes(mLoadedByteCount),
            mTotalByteCount == null
                ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
        if (mThroughput != null) {
          progress.append(format("\tThroughput: %s/s%n",
              FormatUtils.getSizeFromBytes(mThroughput)));
        }
        progress.append(format("\tFailure rate: %.2f%%%n", mFailurePercentage));
      }
      if (mSkipIfExists) {
        progress.append(format("\tBytes Skipped: %s%s%n",
            FormatUtils.getSizeFromBytes(mSkippedByteCount),
            mTotalByteCount == null
                ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      }
      progress.append(format("\tFiles Failed: %s%n", mFailedFileCount));
      if (mVerbose && mFailedFilesWithReasons != null) {
        mFailedFilesWithReasons.forEach((fileName, reason) ->
            progress.append(format("\t\t%s: %s%n", fileName, reason)));
      }
      return progress.toString();
    }

    private String getJsonReport() {
      try {
        return new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new InternalRuntimeException("Failed to convert LoadProgressReport to JSON", e);
      }
    }
  }

  // metrics
  public static final Counter JOB_LOAD_SUCCESS =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_SUCCESS.getName());
  public static final Counter JOB_LOAD_FAIL =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FAIL.getName());

  public static final Counter LOAD_FILE_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_INODES_COUNT.getName());

  public static final Counter LOAD_FAIL_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FAILURE_COUNT.getName());

  public static final Counter LOAD_FILE_SIZE =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FILE_SIZE.getName());
  public static final Meter LOAD_RATE =
      MetricsSystem.meter(MetricKey.MASTER_JOB_LOAD_RATE.getName());
}
