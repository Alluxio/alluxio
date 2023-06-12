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

import alluxio.AlluxioURI;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.LoadFileRequest;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.job.JobDescription;
import alluxio.master.scheduler.Scheduler;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;
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
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Load job that loads a file or a directory into Alluxio.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 * TODO() as task within this class is running on multithreaded context,
 * make thread unsafe places to be thread safe in future.
 */
@NotThreadSafe
public class DoraLoadJob extends AbstractJob<DoraLoadJob.DoraLoadTask> {
  private static final Logger LOG = LoggerFactory.getLogger(DoraLoadJob.class);
  public static final String TYPE = "load";
  private static final double FAILURE_RATIO_THRESHOLD = 0.05;
  private static final int FAILURE_COUNT_THRESHOLD = 100;
  private static final int RETRY_BLOCK_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_BLOCK_CAPACITY;
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);
  private static final int MAX_FILES_PER_TASK = 100; // TODO(lucy) make it configurable

  /* TODO(lucy) add logic to detect loaded files, as currently each file loaded
     status is on each dora worker, so the decision to load or not delegates to
     worker on getting the load req. */
  // Job configurations
  private final String mLoadRootAlluxioPath;
  private final AlluxioURI mLoadRootAlluxioUri;
  private OptionalLong mBandwidth;
  private boolean mUsePartialListing;
  private boolean mVerificationEnabled;

  // Job states
  private final Queue<String> mRetryFiles = new ArrayDeque<>();
  private final Map<String, String> mFailedFiles = new HashMap<>();
  private final AtomicLong mProcessedFileCount = new AtomicLong();
  private final AtomicLong mProcessedDirectoryCount = new AtomicLong();
  private final AtomicLong mLoadedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mProcessingFileCount = new AtomicLong();
  //including retry, do accurate stats later.
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private Iterator<UfsStatus> mUfsStatusIterator;
  private AtomicBoolean mPreparingTasks = new AtomicBoolean(false);
  private final UnderFileSystem mUfs;
  private boolean mLoadMetadataOnly = false;

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
   *                            file data
   */
  public DoraLoadJob(
      String path,
      Optional<String> user, String jobId, OptionalLong bandwidth,
      boolean usePartialListing,
      boolean verificationEnabled,
      boolean loadMetadataOnly) {
    super(user, jobId, new HashBasedWorkerAssignPolicy());
    mLoadRootAlluxioPath = requireNonNull(path, "path is null");
    mLoadRootAlluxioUri = new AlluxioURI(mLoadRootAlluxioPath);
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    String ufsRoot = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    AlluxioURI ufsRootUri = new AlluxioURI(ufsRoot);
    AlluxioURI ufsSyncRootUri = ufsRootUri.join(path);
    mUfs = UnderFileSystem.Factory.create(
        ufsRoot,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    mLoadMetadataOnly = loadMetadataOnly;
    LOG.info(
        "DoraLoadJob for {} created. {} workers are active",
        path, Preconditions.checkNotNull(Scheduler.getInstance()).getActiveWorkers().size());

    try {
      mUfsStatusIterator = mUfs.listStatusIterable(
          ufsSyncRootUri.toString(), ListOptions.defaults().setRecursive(true), null, 0);
      if (mUfsStatusIterator == null) {
        mUfsStatusIterator = Collections.emptyIterator();
      } else {
        mUfsStatusIterator = Iterators.transform(mUfsStatusIterator, (it) -> {
          it.setUfsFullPath(ufsSyncRootUri.join(it.getName()));
          return it;
        });
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Prepare next set of tasks waiting to be kicked off.
   * it is made sure only one thread should be calling this.
   * @return list of DoraLoadTask
   */
  public List<DoraLoadTask> prepareNextTasks() {
    LOG.debug("Preparing next set of tasks for jobId:{}", mJobId);
    ImmutableList.Builder<UfsStatus> batchBuilder = ImmutableList.builder();
    int i = 0;
    int startRetryListSize = mRetryFiles.size();
    int filesToLoad = 0;
    while (filesToLoad < RETRY_THRESHOLD
        && i++ < startRetryListSize && mRetryFiles.peek() != null) {
      String path = mRetryFiles.poll();
      try {
        UfsStatus uriStatus = mUfs.getStatus(path);
        batchBuilder.add(uriStatus);
        ++filesToLoad;
      } catch (IOException e) {
        // The previous list or get might contain stale file metadata.
        // For example, if a file gets removed before the worker actually loads it,
        // the load will fail and the scheduler will retry.
        // In such case, a FileNotFoundException might be thrown when we attempt to
        // get the file status again, and we simply ignore that file.
        if (!(e instanceof FileNotFoundException)) {
          mRetryFiles.offer(path);
        }
      }
    }
    while (filesToLoad < BATCH_SIZE && mUfsStatusIterator.hasNext()) {
      try {
        UfsStatus ufsStatus = mUfsStatusIterator.next();
        batchBuilder.add(ufsStatus);
        ++filesToLoad;
      } catch (AlluxioRuntimeException e) {
        LOG.warn(format("error getting next task for job %s", this), e);
        if (!e.isRetryable()) {
          failJob(e);
        }
      }
    }

    Map<WorkerInfo, List<DoraLoadTask>> workerToTaskMap = new HashMap<>();
    for (UfsStatus ufsStatus : batchBuilder.build()) {
      // NOTE: active workers may not reflect all workers at start up,
      // but hash based policy will deterministically only among current recognized active workers
      WorkerInfo pickedWorker = mWorkerAssignPolicy.pickAWorker(
          ufsStatus.getUfsFullPath().toString(),
          Scheduler.getInstance().getActiveWorkers().keySet());
      if (pickedWorker == null) {
        mRetryFiles.offer(ufsStatus.getUfsFullPath().toString());
        continue;
      }
      List<DoraLoadTask> tasks = workerToTaskMap.computeIfAbsent(pickedWorker,
          w -> new ArrayList<>());
      DoraLoadTask task;
      if (!tasks.isEmpty()
          && tasks.get(tasks.size() - 1).mFilesToLoad.size() < MAX_FILES_PER_TASK) {
        task = tasks.get(tasks.size() - 1);
      } else {
        task = new DoraLoadTask();
        task.setMyRunningWorker(pickedWorker);
        task.setJob(this);
        tasks.add(task);
      }
      task.mFilesToLoad.add(ufsStatus);
      if (ufsStatus.isFile()) {
        if (!mLoadMetadataOnly) {
          mTotalByteCount.addAndGet(ufsStatus.asUfsFileStatus().getContentLength());
        }
        mProcessingFileCount.addAndGet(1);
      }
    }
    if (workerToTaskMap.isEmpty()) {
      return Collections.unmodifiableList(new ArrayList<>());
    }
    List<DoraLoadTask> tasks = workerToTaskMap.values().stream().flatMap(List::stream)
        .collect(Collectors.toList());
    LOG.debug("prepared tasks:{}", tasks);
    return tasks;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mLoadRootAlluxioPath;
  }

  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(mLoadRootAlluxioPath).setType(TYPE).build();
  }

  /**
   * Get bandwidth.
   * @return the allocated bandwidth
   */
  public OptionalLong getBandwidth() {
    return mBandwidth;
  }

  /**
   * Update bandwidth.
   * @param bandwidth new bandwidth
   */
  public void updateBandwidth(OptionalLong bandwidth) {
    mBandwidth = bandwidth;
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
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED, true);
    JOB_LOAD_SUCCESS.inc();
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
   * @param path the file path
   * @return true
   */
  @VisibleForTesting
  public boolean addFilesToRetry(String path) {
    LOG.debug("Retry file {}", path);
    mRetryFiles.offer(path);
    mTotalFailureCount.incrementAndGet();
    JOB_LOAD_FILE_FAIL.inc();
    return true;
  }

  /**
   * Add failed files.
   * @param fileUfsPath
   * @param message
   * @param code
   */
  @VisibleForTesting
  public void addFileFailure(String fileUfsPath, String message, int code) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the first one.
    mFailedFiles.put(fileUfsPath,
        format("Status code: %s, message: %s", code, message));
    JOB_LOAD_FILE_FAIL.inc();
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
    return mState != JobState.FAILED
        && totalFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) totalFailureCount / mProcessingFileCount.get() <= FAILURE_RATIO_THRESHOLD;
  }

  @Override
  public boolean isCurrentPassDone() {
    return !mUfsStatusIterator.hasNext() && mRetryFiles.isEmpty()
        && mTaskList.isEmpty();
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
        list = prepareNextTasks();
        return Collections.unmodifiableList(list);
      } finally {
        mPreparingTasks.compareAndSet(true, false);
      }
    }
    return list;
  }

  @Override
  public void onTaskSubmitFailure(Task<?> task) {
    if (!(task instanceof DoraLoadTask)) {
      throw new IllegalArgumentException("Task is not a DoraLoadTask: " + task);
    }
    ((DoraLoadTask) task).mFilesToLoad.forEach(
        it -> addFilesToRetry(it.getUfsFullPath().toString()));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("JobId", mJobId)
        .add("Path", mLoadRootAlluxioPath)
        .add("User", mUser)
        .add("Bandwidth", mBandwidth)
        .add("UsePartialListing", mUsePartialListing)
        .add("VerificationEnabled", mVerificationEnabled)
        .add("RetryFiles", mRetryFiles)
        .add("FailedFiles", mFailedFiles)
        .add("StartTime", mStartTime)
        .add("ProcessedFileCount", mProcessedFileCount)
        .add("ProcessedFolderCount", mProcessedDirectoryCount)
        .add("LoadedByteCount", mLoadedByteCount)
        .add("TotalFailureCount", mTotalFailureCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("FileIterator", mUfsStatusIterator)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.LoadJobEntry.Builder jobEntry = alluxio.proto.journal.Job.LoadJobEntry
        .newBuilder()
        .setLoadPath(mLoadRootAlluxioPath)
        .setState(JobState.toProto(mState))
        .setPartialListing(mUsePartialListing)
        .setVerify(mVerificationEnabled)
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
      long totalLoadedBytes = doraLoadTask.getFilesToLoad().stream()
          .map((it) -> (it instanceof UfsFileStatus ? it.asUfsFileStatus().getContentLength() : 0))
          .reduce(Long::sum)
          .orElse(0L);
      // what if timeout ? job needs to proactively check or task needs to be aware
      LoadFileResponse response = doraLoadTask.getResponseFuture().get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.debug(format("Get failure from worker:%s, failed files:%s",
            doraLoadTask.getMyRunningWorker(), response.getFailuresList()));
        for (LoadFileFailure failure : response.getFailuresList()) {
          totalLoadedBytes -= failure.getUfsStatus().getUfsFileStatus().getContentLength();
          if (!isHealthy() || !failure.getRetryable() || !addFilesToRetry(
              failure.getUfsStatus().getUfsFullPath())) {
            addFileFailure(
                failure.getUfsStatus().getUfsFullPath(),
                failure.getMessage(), failure.getCode());
          }
        }
      }
      int totalLoadedInodes = doraLoadTask.getFilesToLoad().size()
          - response.getFailuresList().size();
      int totalLoadedFile =
          (int) (doraLoadTask.getFilesToLoad().stream().filter(UfsStatus::isFile).count()
                        - response.getFailuresList().stream()
              .filter(it -> !it.getUfsStatus().getIsDirectory()).count());
      int totalLoadedDirectory = totalLoadedInodes - totalLoadedFile;
      if (!mLoadMetadataOnly) {
        addLoadedBytes(totalLoadedBytes);
        JOB_LOAD_FILE_SIZE.inc(totalLoadedBytes);
        JOB_LOAD_RATE.mark(totalLoadedBytes);
        JOB_LOAD_RATE.mark(totalLoadedBytes);
      }
      mProcessedFileCount.addAndGet(totalLoadedFile);
      mProcessedDirectoryCount.addAndGet(totalLoadedDirectory);
      JOB_LOAD_FILE_COUNT.inc(totalLoadedFile);
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      LOG.warn("exception when trying to get load response.", e.getCause());
      for (UfsStatus ufsStatus : doraLoadTask.getFilesToLoad()) {
        if (isHealthy()) {
          addFilesToRetry(ufsStatus.getUfsFullPath().toString());
        }
        else {
          AlluxioRuntimeException exception = AlluxioRuntimeException.from(e.getCause());
          addFileFailure(ufsStatus.getUfsFullPath().toString(),
              exception.getMessage(), exception.getStatus().getCode()
              .value());
        }
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("Task get canceled and will retry.", e);
      doraLoadTask.getFilesToLoad().forEach(it -> addFilesToRetry(it.getUfsFullPath().toString()));
      return true;
    }
    catch (InterruptedException e) {
      doraLoadTask.getFilesToLoad().forEach(it -> addFilesToRetry(it.getUfsFullPath().toString()));
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  @Override
  public void updateJob(Job<?> job) {
    if (!(job instanceof DoraLoadJob)) {
      throw new IllegalArgumentException("Job is not a DoraLoadJob: " + job);
    }
    DoraLoadJob targetJob = (DoraLoadJob) job;
    updateBandwidth(targetJob.getBandwidth());
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
   * Dora load task.
   */
  public class DoraLoadTask extends Task<LoadFileResponse> {

    protected List<UfsStatus> mFilesToLoad;

    /**
     * Constructor.
     */
    public DoraLoadTask() {
      super(DoraLoadJob.this, DoraLoadJob.this.mTaskIdGenerator.incrementAndGet());
      super.setPriority(1);
      mFilesToLoad = new ArrayList<>();
    }

    /**
     * Constructor.
     * @param filesToLoad the file to load
     */
    public DoraLoadTask(List<UfsStatus> filesToLoad) {
      super(DoraLoadJob.this, DoraLoadJob.this.mTaskIdGenerator.incrementAndGet());
      super.setPriority(1);
      mFilesToLoad = filesToLoad;
    }

    /**
     * Get files to load of this task.
     * @return list of UfsStatus
     */
    public List<UfsStatus> getFilesToLoad() {
      return mFilesToLoad;
    }

    @Override
    protected ListenableFuture<LoadFileResponse> run(BlockWorkerClient workerClient) {
      LOG.info("Start running task:{} on worker:{}", toString(), getMyRunningWorker());
      LoadFileRequest.Builder loadFileReqBuilder = LoadFileRequest.newBuilder();
      for (UfsStatus ufsStatus : mFilesToLoad) {
        loadFileReqBuilder.addUfsStatus(ufsStatus.toProto());
      }
      UfsReadOptions.Builder ufsReadOptions = UfsReadOptions
          .newBuilder()
          .setTag(mJobId)
          .setPositionShort(false);
      loadFileReqBuilder.setOptions(ufsReadOptions);
      loadFileReqBuilder.setLoadMetadataOnly(mLoadMetadataOnly);
      return workerClient.loadFile(loadFileReqBuilder.build());
    }

    @Override
    public String toString() {
      final StringBuilder filesBuilder = new StringBuilder();
      getFilesToLoad().forEach(f -> {
        filesBuilder.append(f.getUfsFullPath().toString()  + ",");
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
    private final long mProcessedFileCount;
    private final long mProcessedDirectoryCount;
    private final long mLoadedByteCount;
    private final Long mTotalByteCount;
    private final Long mThroughput;
    private final double mFailurePercentage;
    private final AlluxioRuntimeException mFailureReason;
    private final long mFailedFileCount;
    private final Map<String, String> mFailedFilesWithReasons;
    private final boolean mLoadData;

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
      mProcessedFileCount = job.mProcessedFileCount.get();
      mProcessedDirectoryCount = job.mProcessedDirectoryCount.get();
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
              / (mProcessedFileCount + mProcessedDirectoryCount)) * 100;
      mFailureReason = job.mFailedReason.orElse(null);
      mFailedFileCount = job.mFailedFiles.size();
      if (verbose && mFailedFileCount > 0) {
        mFailedFilesWithReasons = job.mFailedFiles;
      } else {
        mFailedFilesWithReasons = null;
      }
      mLoadData = !job.mLoadMetadataOnly;
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
          format("\tSettings:\tbandwidth: %s\tverify: %s%n",
              mBandwidth == null ? "unlimited" : mBandwidth,
              mVerificationEnabled));
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
      progress.append(format("\tLoad data: %s%n", mLoadData));
      progress.append(format("\tFiles Processed: %d%n", mProcessedFileCount));
      progress.append(format("\tDirectories Processed: %d%n", mProcessedDirectoryCount));
      if (mLoadData) {
        progress.append(format("\tBytes Loaded: %s%s%n",
            FormatUtils.getSizeFromBytes(mLoadedByteCount),
            mTotalByteCount == null
                ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
        if (mThroughput != null) {
          progress.append(format("\tThroughput: %s/s%n",
              FormatUtils.getSizeFromBytes(mThroughput)));
        }
        progress.append(format("\tBlock load failure rate: %.2f%%%n", mFailurePercentage));
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

  public static final Counter JOB_LOAD_FILE_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_COUNT.getName());

  public static final Counter JOB_LOAD_FILE_FAIL =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FILE_FAIL.getName());

  public static final Counter JOB_LOAD_FILE_SIZE =
      MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_FILE_SIZE.getName());
  public static final Meter JOB_LOAD_RATE =
      MetricsSystem.meter(MetricKey.MASTER_JOB_LOAD_RATE.getName());
}
