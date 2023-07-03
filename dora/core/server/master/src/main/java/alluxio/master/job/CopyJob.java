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
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.grpc.CopyRequest;
import alluxio.grpc.CopyResponse;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
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
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Copy job that copy a file or a directory from source to destination.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 * TODO() as task within this class is running on multithreaded context,
 * make thread unsafe places to be thread safe in future.
 */
@NotThreadSafe
public class CopyJob extends AbstractJob<CopyJob.CopyTask> {
  private static final Logger LOG = LoggerFactory.getLogger(CopyJob.class);
  public static final String TYPE = "copy";
  private static final double FAILURE_RATIO_THRESHOLD = 0.1;
  private static final int FAILURE_COUNT_THRESHOLD = 100;
  private static final int RETRY_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_CAPACITY;
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);
  public static final Predicate<FileInfo> QUALIFIED_FILE_FILTER = FileInfo::isCompleted;
  private final String mSrc;
  private final String mDst;
  private final boolean mOverwrite;
  private final boolean mCheckContent;
  private OptionalLong mBandwidth;
  private boolean mUsePartialListing;
  private boolean mVerificationEnabled;

  // Job states
  private final LinkedList<Route> mRetryRoutes = new LinkedList<>();
  private final Map<String, String> mFailedFiles = new HashMap<>();
  private final AtomicLong mProcessedFileCount = new AtomicLong();
  private final AtomicLong mCopiedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private final Iterable<FileInfo> mFileIterable;
  private Optional<Iterator<FileInfo>> mFileIterator = Optional.empty();

  /**
   * Constructor.
   *
   * @param src                 file source
   * @param dst                 file destination
   * @param overwrite           whether to overwrite the file
   * @param user                user for authentication
   * @param jobId               job identifier
   * @param bandwidth           bandwidth
   * @param usePartialListing   whether to use partial listing
   * @param verificationEnabled whether to verify the job after loaded
   * @param checkContent        whether to check content
   * @param fileIterable        file iterable
   */
  public CopyJob(String src, String dst, boolean overwrite, Optional<String> user, String jobId,
      OptionalLong bandwidth, boolean usePartialListing, boolean verificationEnabled,
      boolean checkContent, Iterable<FileInfo> fileIterable) {
    super(user, jobId, new RoundRobinWorkerAssignPolicy());
    mSrc = requireNonNull(src, "src is null");
    mDst = requireNonNull(dst, "dst is null");
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    mState = JobState.RUNNING;
    mFileIterable = fileIterable;
    mOverwrite = overwrite;
    mCheckContent = checkContent;
  }

  /**
   * @return source file path
   */
  public String getSrc() {
    return mSrc;
  }

  /**
   * @return the description of the copy job. The path is the "src:dst" format
   */
  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(mSrc + ":" + mDst).setType(TYPE).build();
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
   * Is verification enabled.
   * @return whether verification is enabled
   */
  public boolean isVerificationEnabled() {
    return mVerificationEnabled;
  }

  /**
   * Is verification enabled.
   *
   * @return whether verification is enabled
   */
  @Override
  public boolean needVerification() {
    return mVerificationEnabled && mProcessedFileCount.get() > 0;
  }

  /**
   * Enable verification.
   * @param enableVerification whether to enable verification
   */
  public void setVerificationEnabled(boolean enableVerification) {
    mVerificationEnabled = enableVerification;
  }

  /**
   * Set load state to FAILED with given reason.
   * @param reason failure exception
   */
  @Override
  public void failJob(AlluxioRuntimeException reason) {
    setJobState(JobState.FAILED, true);
    mFailedReason = Optional.of(reason);
    JOB_COPY_FAIL.inc();
    LOG.info("Copy Job {} fails with status: {}", mJobId, this);
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED, true);
    JOB_COPY_SUCCESS.inc();
    LOG.info("Copy Job {} succeeds with status {}", mJobId, this);
  }

  /**
   * Add bytes to total loaded bytes.
   * @param bytes bytes to be added to total
   */
  @VisibleForTesting
  public void addCopiedBytes(long bytes) {
    mCopiedByteCount.addAndGet(bytes);
  }

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    return (new CopyProgressReport(this, verbose)).getReport(format);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CopyJob that = (CopyJob) o;
    return Objects.equal(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getDescription());
  }

  @Override
  public boolean isHealthy() {
    long currentFailureCount = mCurrentFailureCount.get();
    return mState != JobState.FAILED
        && (currentFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) currentFailureCount / mProcessedFileCount.get() <= FAILURE_RATIO_THRESHOLD);
  }

  @Override
  public boolean isCurrentPassDone() {
    return  mFileIterator.isPresent() && !mFileIterator.get().hasNext()
        && mRetryRoutes.isEmpty();
  }

  @Override
  public void initiateVerification() {
    Preconditions.checkState(isCurrentPassDone(), "Previous pass is not finished");
    mFileIterator = Optional.empty();
    mTotalFailureCount.addAndGet(mCurrentFailureCount.get());
    mProcessedFileCount.set(0);
    mCurrentFailureCount.set(0);
    mState = JobState.VERIFYING;
  }

  /**
   * get next load task.
   *
   * @param workers workerInfos
   * @return the next task to run. If there is no task to run, return empty
   */
  public List<CopyTask> getNextTasks(Collection<WorkerInfo> workers) {
    List<CopyTask> tasks = new ArrayList<>();
    List<Route> routes = getNextRoutes(BATCH_SIZE);
    if (routes.isEmpty()) {
      return Collections.unmodifiableList(tasks);
    }
    WorkerInfo workerInfo = mWorkerAssignPolicy.pickAWorker(StringUtil.EMPTY_STRING, workers);
    CopyTask copyTask = new CopyTask(routes);
    copyTask.setMyRunningWorker(workerInfo);
    tasks.add(copyTask);
    return Collections.unmodifiableList(tasks);
  }

  /**
   * Define how to process task that gets rejected when scheduler tried to kick off.
   * For CopyJob
   * @param task
   */
  public void onTaskSubmitFailure(Task<?> task) {
    if (!(task instanceof CopyTask)) {
      throw new IllegalArgumentException("Task is not a CopyTask: " + task);
    }
    ((CopyTask) task).mRoutes.forEach(this::addToRetry);
  }

  /**
   * Get next batch of blocks.
   * @param count number of blocks
   * @return list of blocks
   */
  @VisibleForTesting
  public List<Route> getNextRoutes(int count) {
    FileInfo currentFile;
    if (!mFileIterator.isPresent()) {
      mFileIterator = Optional.of(mFileIterable.iterator());
      if (!mFileIterator.get().hasNext()) {
        return ImmutableList.of();
      }
    }
    ImmutableList.Builder<Route> batchBuilder = ImmutableList.builder();
    int i = 0;
    // retry failed blocks if there's too many failed blocks otherwise wait until no more new block
    if (mRetryRoutes.size() > RETRY_THRESHOLD
        || (!mFileIterator.get().hasNext())) {
      while (i < count && !mRetryRoutes.isEmpty()) {
        batchBuilder.add(requireNonNull(mRetryRoutes.removeFirst()));
        i++;
      }
    }
    for (; i < count; i++) {
      if (!mFileIterator.get().hasNext()) {
        return batchBuilder.build();
      }
      currentFile = mFileIterator.get().next();
      if (!mFailedFiles.containsKey(currentFile.getPath())) {
        mProcessedFileCount.incrementAndGet();
      }
      Route route = buildRoute(currentFile);
      batchBuilder.add(route);
      // would be inaccurate when we initial verification, and we retry un-retryable blocks
      mTotalByteCount.addAndGet(currentFile.getLength());
    }
    return batchBuilder.build();
  }

  /**
   * Add a route to retry later.
   * @param route the route to retry
   * @return whether the block is successfully added
   */
  @VisibleForTesting
  public boolean addToRetry(Route route) {
    if (mRetryRoutes.size() >= RETRY_CAPACITY) {
      return false;
    }
    LOG.debug("Retry route {}", route);
    mRetryRoutes.add(route);
    mCurrentFailureCount.incrementAndGet();
    COPY_FAIL_FILE_COUNT.inc();
    return true;
  }

  /**
   * Add a block to failure summary.
   *
   * @param src   the source path of the file that failed
   * @param message failure message
   * @param code    status code for exception
   */
  @VisibleForTesting
  public void addFailure(String src, String message, int code) {
    mFailedFiles.put(src,
        format("Status code: %s, message: %s", code, message));
    mCurrentFailureCount.incrementAndGet();
    COPY_FAIL_FILE_COUNT.inc();
  }

  private Route buildRoute(FileInfo sourceFile) {
    String relativePath;
    try {
      relativePath = PathUtils.subtractPaths(sourceFile.getPath(), new AlluxioURI(mSrc).getPath());
    } catch (InvalidPathException e) {
      throw new InvalidArgumentRuntimeException("fail to parse source file path", e);
    }
    String dst = PathUtils.concatPath(mDst, relativePath);
    return Route.newBuilder().setSrc(sourceFile.getUfsPath())
                .setDst(dst).setLength(sourceFile.getLength()).build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("JobId", mJobId)
        .add("Src", mSrc)
        .add("Dst", mDst)
        .add("User", mUser)
        .add("Bandwidth", mBandwidth)
        .add("UsePartialListing", mUsePartialListing)
        .add("VerificationEnabled", mVerificationEnabled)
        .add("TotalByteCount", mTotalByteCount)
        .add("CheckContent", mCheckContent)
        .add("Overwrite", mOverwrite)
        .add("RetryRoutes", mRetryRoutes)
        .add("FailedFiles", mFailedFiles)
        .add("StartTime", mStartTime)
        .add("ProcessedFileCount", mProcessedFileCount)
        .add("LoadedByteCount", mCopiedByteCount)
        .add("TotalFailureCount", mTotalFailureCount)
        .add("CurrentFailureCount", mCurrentFailureCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("FileIterator", mFileIterator)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.CopyJobEntry.Builder jobEntry = alluxio.proto.journal.Job.CopyJobEntry
        .newBuilder().setSrc(mSrc).setDst(mDst).setState(JobState.toProto(mState))
        .setPartialListing(mUsePartialListing)
        .setVerify(mVerificationEnabled)
        .setOverwrite(mOverwrite)
        .setCheckContent(mCheckContent)
        .setJobId(mJobId);
    mUser.ifPresent(jobEntry::setUser);
    mBandwidth.ifPresent(jobEntry::setBandwidth);
    mEndTime.ifPresent(jobEntry::setEndTime);
    return Journal.JournalEntry
        .newBuilder()
        .setCopyJob(jobEntry.build())
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
  public boolean processResponse(CopyTask task) {
    try {
      CopyResponse response = task.getResponseFuture().get();
      long totalBytes = task.getRoutes().stream()
          .map(Route::getLength)
          .reduce(Long::sum)
          .orElse(0L);
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.debug(format("Get failure from worker: %s", response.getFailuresList()));
        for (RouteFailure status : response.getFailuresList()) {
          totalBytes -= status.getRoute().getLength();
          if (!isHealthy() || !status.getRetryable() || !addToRetry(
              status.getRoute())) {
            addFailure(status.getRoute().getSrc(), status.getMessage(), status.getCode());
          }
        }
      }
      addCopiedBytes(totalBytes);
      COPY_FILE_COUNT.inc(
          task.getRoutes().size() - response.getFailuresCount());
      COPY_SIZE.inc(totalBytes);
      COPY_RATE.mark(totalBytes);
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      LOG.warn("exception when trying to get load response.", e.getCause());
      for (Route route : task.getRoutes()) {
        if (isHealthy()) {
          addToRetry(route);
        }
        else {
          AlluxioRuntimeException exception = AlluxioRuntimeException.from(e.getCause());
          addFailure(route.getSrc(), exception.getMessage(), exception.getStatus().getCode()
                                                                      .value());
        }
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("Task get canceled and will retry.", e);
      task.getRoutes().forEach(this::addToRetry);
      return true;
    }
    catch (InterruptedException e) {
      task.getRoutes().forEach(this::addToRetry);
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
   * Loads blocks in a UFS through an Alluxio worker.
   */
  public class CopyTask extends Task<CopyResponse> {

    /**
     * @return blocks to load
     */
    public List<Route> getRoutes() {
      return mRoutes;
    }

    private final List<Route> mRoutes;

    /**
     * Creates a new instance of {@link CopyTask}.
     *
     * @param routes pair of source and destination files
     */
    public CopyTask(List<Route> routes) {
      super(CopyJob.this, CopyJob.this.mTaskIdGenerator.incrementAndGet());
      super.setPriority(2);
      mRoutes = routes;
    }

    @Override
    public ListenableFuture<CopyResponse> run(BlockWorkerClient workerClient) {
      CopyRequest.Builder request = CopyRequest
          .newBuilder()
          .addAllRoutes(mRoutes);
      UfsReadOptions.Builder ufsReadOptions = UfsReadOptions
          .newBuilder()
          .setTag(mJobId)
          .setPositionShort(false);

      if (mBandwidth.isPresent()) {
        ufsReadOptions.setBandwidth(mBandwidth.getAsLong());
      }
      mUser.ifPresent(ufsReadOptions::setUser);
      WriteOptions writeOptions = WriteOptions
          .newBuilder()
          .setOverwrite(mOverwrite)
          .setCheckContent(mCheckContent)
          .build();
      return workerClient.copy(request
          .setUfsReadOptions(ufsReadOptions.build())
          .setWriteOptions(writeOptions)
          .build());
    }

    @Override
    public int compareTo(Task o) {
      return 0;
    }
  }

  private static class CopyProgressReport {
    private final boolean mVerbose;
    private final JobState mJobState;
    private final boolean mCheckContent;
    private final long mProcessedFileCount;
    private final long mByteCount;
    private final Long mTotalByteCount;
    private final Long mThroughput;
    private final double mFailurePercentage;
    private final AlluxioRuntimeException mFailureReason;
    private final long mFailedFileCount;
    private final Map<String, String> mFailedFilesWithReasons;

    public CopyProgressReport(CopyJob job, boolean verbose)
    {
      mVerbose = verbose;
      mJobState = job.mState;
      mCheckContent = job.mCheckContent;
      mProcessedFileCount = job.mProcessedFileCount.get();
      mByteCount = job.mCopiedByteCount.get();
      if (!job.mUsePartialListing && job.mFileIterator.isPresent()) {
        mTotalByteCount = job.mTotalByteCount.get();
      }
      else {
        mTotalByteCount = null;
      }
      long duration = job.getDurationInSec();
      if (duration > 0) {
        mThroughput = job.mCopiedByteCount.get() / duration;
      }
      else {
        mThroughput = null;
      }
      long fileCount = job.mProcessedFileCount.get();
      if (fileCount > 0) {
        mFailurePercentage =
            ((double) (job.mTotalFailureCount.get() + job.mCurrentFailureCount.get()) / fileCount)
                * 100;
      }
      else {
        mFailurePercentage = 0;
      }
      mFailureReason = job.mFailedReason.orElse(null);
      mFailedFileCount = job.mFailedFiles.size();
      if (verbose && mFailedFileCount > 0) {
        mFailedFilesWithReasons = job.mFailedFiles;
      } else {
        mFailedFilesWithReasons = Collections.emptyMap();
      }
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
          format("\tSettings:\tcheck-content: %s%n", mCheckContent));
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
      progress.append(format("\tFiles Processed: %d%n", mProcessedFileCount));
      progress.append(format("\tBytes Copied: %s%s%n",
          FormatUtils.getSizeFromBytes(mByteCount),
          mTotalByteCount == null
              ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      if (mThroughput != null) {
        progress.append(format("\tThroughput: %s/s%n",
            FormatUtils.getSizeFromBytes(mThroughput)));
      }
      progress.append(format("\tFiles failure rate: %.2f%%%n", mFailurePercentage));
      progress.append(format("\tFiles Failed: %s%n", mFailedFileCount));
      if (mVerbose && !mFailedFilesWithReasons.isEmpty()) {
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
        throw new InternalRuntimeException("Failed to convert CopyProgressReport to JSON", e);
      }
    }
  }

  // metrics
  public static final Counter JOB_COPY_SUCCESS =
          MetricsSystem.counter(MetricKey.MASTER_JOB_COPY_SUCCESS.getName());
  public static final Counter JOB_COPY_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_COPY_FAIL.getName());
  public static final Counter COPY_FILE_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_JOB_COPY_FILE_COUNT.getName());
  public static final Counter COPY_FAIL_FILE_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_JOB_COPY_FAIL_FILE_COUNT.getName());
  public static final Counter COPY_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_JOB_COPY_SIZE.getName());
  public static final Meter COPY_RATE =
          MetricsSystem.meter(MetricKey.MASTER_JOB_COPY_RATE.getName());
}
