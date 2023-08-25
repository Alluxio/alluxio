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
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.MoveRequest;
import alluxio.grpc.MoveResponse;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Job.FileFilter;
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
import java.util.Date;
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
 * Move job that move a file or a directory from source to destination.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 * TODO() as task within this class is running on multithreaded context,
 * make thread unsafe places to be thread safe in future.
 */
@NotThreadSafe
public class MoveJob extends AbstractJob<MoveJob.MoveTask> {
  private static final Logger LOG = LoggerFactory.getLogger(MoveJob.class);
  public static final String TYPE = "move";
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
  private final AtomicLong mMovedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private final AtomicLong mCurrentSuccessCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private final Iterable<FileInfo> mFileIterable;
  private Optional<Iterator<FileInfo>> mFileIterator = Optional.empty();
  private Optional<FileFilter> mFilter;

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
   * @param verificationEnabled whether to verify the job after moved
   * @param checkContent        whether to check content
   * @param fileIterable        file iterable
   * @param filter              file filter
   */
  public MoveJob(String src, String dst, boolean overwrite, Optional<String> user, String jobId,
                 OptionalLong bandwidth, boolean usePartialListing, boolean verificationEnabled,
                 boolean checkContent, Iterable<FileInfo> fileIterable,
                 Optional<FileFilter> filter) {
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
    mFilter = filter;
  }

  /**
   * @return source file path
   */
  public String getSrc() {
    return mSrc;
  }

  /**
   * @return the description of the move job. The path is the "src:dst" format
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
   * Set move state to FAILED with given reason.
   * @param reason failure exception
   */
  @Override
  public void failJob(AlluxioRuntimeException reason) {
    setJobState(JobState.FAILED, true);
    mFailedReason = Optional.of(reason);
    JOB_MOVE_FAIL.inc();
    LOG.info("Move Job {} fails with status: {}", mJobId, this);
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED, true);
    JOB_MOVE_SUCCESS.inc();
    LOG.info("Move Job {} succeeds with status {}", mJobId, this);
  }

  /**
   * Add bytes to total moved bytes.
   * @param bytes bytes to be added to total
   */
  @VisibleForTesting
  public void addMovedBytes(long bytes) {
    mMovedByteCount.addAndGet(bytes);
  }

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    return (new MoveProgressReport(this, verbose)).getReport(format);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MoveJob that = (MoveJob) o;
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
   * get next move task.
   *
   * @param workers workers candidates
   * @return the next task to run. If there is no task to run, return empty
   */
  public List<MoveTask> getNextTasks(Collection<WorkerInfo> workers) {
    List<MoveTask> tasks = new ArrayList<>();
    Iterator<MoveTask> it = mRetryTaskList.iterator();
    if (it.hasNext()) {
      MoveTask task = it.next();
      LOG.debug("Re-submit retried MoveTask:{} in getNextTasks.", task.getTaskId());
      tasks.add(task);
      it.remove();
      return Collections.unmodifiableList(tasks);
    }
    List<Route> routes = getNextRoutes(BATCH_SIZE);
    if (routes.isEmpty()) {
      return Collections.unmodifiableList(tasks);
    }
    WorkerInfo workerInfo = mWorkerAssignPolicy.pickAWorker(StringUtil.EMPTY_STRING, workers);
    MoveTask moveTask = new MoveTask(routes);
    moveTask.setMyRunningWorker(workerInfo);
    tasks.add(moveTask);
    return Collections.unmodifiableList(tasks);
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
    MOVE_FAIL_FILE_COUNT.inc();
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
    MOVE_FAIL_FILE_COUNT.inc();
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
        .add("MovedByteCount", mMovedByteCount)
        .add("TotalFailureCount", mTotalFailureCount)
        .add("CurrentFailureCount", mCurrentFailureCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("FileIterator", mFileIterator)
        .add("FileFilter", mFilter)
        .add("FileIterable", mFileIterable)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.MoveJobEntry.Builder jobEntry = alluxio.proto.journal.Job.MoveJobEntry
        .newBuilder().setSrc(mSrc).setDst(mDst).setState(JobState.toProto(mState))
        .setPartialListing(mUsePartialListing)
        .setOverwrite(mOverwrite)
        .setCheckContent(mCheckContent)
        .setVerify(mVerificationEnabled)
        .setJobId(mJobId);
    mUser.ifPresent(jobEntry::setUser);
    mBandwidth.ifPresent(jobEntry::setBandwidth);
    mEndTime.ifPresent(jobEntry::setEndTime);
    if (mFilter.isPresent()) {
      FileFilter.Builder builder = FileFilter.newBuilder().setValue(mFilter.get().getValue())
          .setName(mFilter.get().getName());
      if (mFilter.get().hasPattern()) {
        builder.setPattern(mFilter.get().getPattern());
      }
      jobEntry.setFilter(builder.build());
    }
    return Journal.JournalEntry
        .newBuilder()
        .setMoveJob(jobEntry.build())
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
  public boolean processResponse(MoveTask task) {
    try {
      MoveResponse response = task.getResponseFuture().get();
      long totalBytes = task.getRoutes().stream()
          .map(Route::getLength)
          .reduce(Long::sum)
          .orElse(0L);
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.debug(format("Get failure from worker: %s", response.getFailuresList()));
        for (RouteFailure status : response.getFailuresList()) {
          totalBytes -= status.getRoute().getLength();
          LOG.debug(format("Move file %s to %s failure: Status code: %s, message: %s",
                  status.getRoute().getSrc(), status.getRoute().getDst(),
              status.getCode(), status.getMessage()));
          if (!isHealthy() || !status.getRetryable() || !addToRetry(
              status.getRoute())) {
            addFailure(status.getRoute().getSrc(), status.getMessage(), status.getCode());
          }
        }
      }
      addMovedBytes(totalBytes);
      mCurrentSuccessCount.addAndGet(task.getRoutes().size() - response.getFailuresCount());
      MOVE_FILE_COUNT.inc(
          task.getRoutes().size() - response.getFailuresCount());
      MOVE_SIZE.inc(totalBytes);
      MOVE_RATE.mark(totalBytes);
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      LOG.warn("exception when trying to get move response.", e.getCause());
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
   * Moves blocks in a UFS through an Alluxio worker.
   */
  public class MoveTask extends Task<MoveResponse> {

    /**
     * @return blocks to move
     */
    public List<Route> getRoutes() {
      return mRoutes;
    }

    private final List<Route> mRoutes;

    /**
     * Creates a new instance of {@link MoveTask}.
     *
     * @param routes pair of source and destination files
     */
    public MoveTask(List<Route> routes) {
      super(MoveJob.this, MoveJob.this.mTaskIdGenerator.incrementAndGet());
      super.setPriority(2);
      mRoutes = routes;
    }

    @Override
    public ListenableFuture<MoveResponse> run(BlockWorkerClient workerClient) {
      MoveRequest.Builder request = MoveRequest
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
          .setCheckContent(mCheckContent)
          .setOverwrite(mOverwrite)
          .build();
      return workerClient.move(request
          .setUfsReadOptions(ufsReadOptions.build())
          .setWriteOptions(writeOptions)
          .build());
    }
  }

  private static class MoveProgressReport {
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
    private final long mSuccessFileCount;
    private final Map<String, String> mFailedFilesWithReasons;
    private final String mJobId;
    private final long mStartTime;
    private final long mEndTime;

    public MoveProgressReport(MoveJob job, boolean verbose)
    {
      mVerbose = verbose;
      mJobState = job.mState;
      mJobId = job.mJobId;
      mCheckContent = job.mCheckContent;
      mProcessedFileCount = job.mProcessedFileCount.get();
      mByteCount = job.mMovedByteCount.get();
      if (!job.mUsePartialListing && job.mFileIterator.isPresent()) {
        mTotalByteCount = job.mTotalByteCount.get();
      }
      else {
        mTotalByteCount = null;
      }
      long duration = job.getDurationInSec();
      if (duration > 0) {
        mThroughput = job.mMovedByteCount.get() / duration;
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
      mSuccessFileCount = job.mCurrentSuccessCount.get();
      if (verbose && mFailedFileCount > 0) {
        mFailedFilesWithReasons = job.mFailedFiles;
      } else {
        mFailedFilesWithReasons = Collections.emptyMap();
      }
      mStartTime = job.mStartTime;
      if (mJobState == JobState.SUCCEEDED || mJobState == JobState.FAILED) {
        if (job.mEndTime.isPresent()) {
          mEndTime = job.mEndTime.getAsLong();
        } else {
          throw new InternalRuntimeException(
              String.format("No end time in ending state %s", mJobState));
        }
      } else {
        mEndTime = 0;
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
              format("Unknown move progress report format: %s", format));
      }
    }

    private String getTextReport() {
      StringBuilder progress = new StringBuilder();
      progress.append(
          format("\tSettings: \"check-content: %s\"%n", mCheckContent));
      progress.append(format("\tJob Submitted: %s%n", new Date(mStartTime)));
      progress.append(format("\tJob Id: %s%n", mJobId));
      if (mJobState == JobState.SUCCEEDED || mJobState == JobState.FAILED) {
        progress.append(format("\tJob State: %s%s, finished at %s%n", mJobState,
            mFailureReason == null
                ? "" : format(
                " (%s: %s)",
                mFailureReason.getClass().getName(),
                mFailureReason.getMessage()),
            new Date(mEndTime)));
      } else {
        progress.append(format("\tJob State: %s%s%n", mJobState,
            mFailureReason == null
                ? "" : format(
                " (%s: %s)",
                mFailureReason.getClass().getName(),
                mFailureReason.getMessage())));
      }
      if (mVerbose && mFailureReason != null) {
        for (StackTraceElement stack : mFailureReason.getStackTrace()) {
          progress.append(format("\t\t%s%n", stack.toString()));
        }
      }
      progress.append(format("\tFiles qualified%s: %d%s%n",
          mJobState == JobState.RUNNING ? " so far" : "", mProcessedFileCount,
          mTotalByteCount == null
              ? "" : format(", %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      progress.append(format("\tFiles Failed: %s%n", mFailedFileCount));
      progress.append(format("\tFiles Succeeded: %s%n", mSuccessFileCount));
      progress.append(format("\tBytes Moved: %s%n", FormatUtils.getSizeFromBytes(mByteCount)));
      if (mThroughput != null) {
        progress.append(format("\tThroughput: %s/s%n",
            FormatUtils.getSizeFromBytes(mThroughput)));
      }
      progress.append(format("\tFiles failure rate: %.2f%%%n", mFailurePercentage));
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
        throw new InternalRuntimeException("Failed to convert MoveProgressReport to JSON", e);
      }
    }
  }

  // metrics
  public static final Counter JOB_MOVE_SUCCESS =
      MetricsSystem.counter(MetricKey.MASTER_JOB_MOVE_SUCCESS.getName());
  public static final Counter JOB_MOVE_FAIL =
      MetricsSystem.counter(MetricKey.MASTER_JOB_MOVE_FAIL.getName());
  public static final Counter MOVE_FILE_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_MOVE_FILE_COUNT.getName());
  public static final Counter MOVE_FAIL_FILE_COUNT =
      MetricsSystem.counter(MetricKey.MASTER_JOB_MOVE_FAIL_FILE_COUNT.getName());
  public static final Counter MOVE_SIZE =
      MetricsSystem.counter(MetricKey.MASTER_JOB_MOVE_SIZE.getName());
  public static final Meter MOVE_RATE =
      MetricsSystem.meter(MetricKey.MASTER_JOB_MOVE_RATE.getName());
}
