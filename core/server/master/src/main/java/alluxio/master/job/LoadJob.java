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
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadRequest;
import alluxio.grpc.LoadResponse;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.job.JobDescription;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Journal;
import alluxio.scheduler.job.Job;
import alluxio.scheduler.job.JobState;
import alluxio.scheduler.job.Task;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockInfo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Load job that loads a file or a directory into Alluxio.
 * This class should only be manipulated from the scheduler thread in Scheduler
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class LoadJob extends AbstractJob<LoadJob.LoadTask> {
  private static final Logger LOG = LoggerFactory.getLogger(LoadJob.class);
  public static final String TYPE = "load";
  private static final double FAILURE_RATIO_THRESHOLD = 0.05;
  private static final int FAILURE_COUNT_THRESHOLD = 100;
  private static final int RETRY_BLOCK_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_BLOCK_CAPACITY;
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);
  public static final Predicate<FileInfo> QUALIFIED_FILE_FILTER =
      (fileInfo) -> !fileInfo.isFolder() && fileInfo.isCompleted() && fileInfo.isPersisted()
          && fileInfo.getInAlluxioPercentage() != 100;
  // Job configurations
  private final String mPath;

  private OptionalLong mBandwidth;
  private boolean mUsePartialListing;
  private boolean mVerificationEnabled;

  // Job states
  private final LinkedList<Block> mRetryBlocks = new LinkedList<>();
  private final Map<String, String> mFailedFiles = new HashMap<>();

  private final AtomicLong mProcessedFileCount = new AtomicLong();
  private final AtomicLong mLoadedByteCount = new AtomicLong();
  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mTotalBlockCount = new AtomicLong();
  private final AtomicLong mCurrentBlockCount = new AtomicLong();
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private final Iterable<FileInfo> mFileIterable;
  private Optional<Iterator<FileInfo>> mFileIterator = Optional.empty();
  private FileInfo mCurrentFile;
  private Iterator<Long> mBlockIterator = Collections.emptyIterator();

  /**
   * Constructor.
   * @param path file path
   * @param user user for authentication
   * @param bandwidth bandwidth
   * @param fileIterator file iterator
   */
  @VisibleForTesting
  public LoadJob(String path, String user, OptionalLong bandwidth,
      FileIterable fileIterator) {
    this(path, Optional.of(user), UUID.randomUUID().toString(), bandwidth, false, false,
        fileIterator);
  }

  /**
   * Constructor.
   *
   * @param path                file path
   * @param user                user for authentication
   * @param jobId               job identifier
   * @param bandwidth           bandwidth
   * @param usePartialListing   whether to use partial listing
   * @param verificationEnabled whether to verify the job after loaded
   * @param fileIterable        file iterable
   */
  public LoadJob(
      String path,
      Optional<String> user, String jobId, OptionalLong bandwidth,
      boolean usePartialListing,
      boolean verificationEnabled, FileIterable fileIterable) {
    super(user, jobId);
    mPath = requireNonNull(path, "path is null");
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    mFileIterable = fileIterable;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mPath;
  }

  @Override
  public JobDescription getDescription() {
    return JobDescription.newBuilder().setPath(mPath).setType(TYPE).build();
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
    setJobState(JobState.FAILED);
    mFailedReason = Optional.of(reason);
    JOB_LOAD_FAIL.inc();
  }

  @Override
  public void setJobSuccess() {
    setJobState(JobState.SUCCEEDED);
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

  @Override
  public String getProgress(JobProgressReportFormat format, boolean verbose) {
    return (new LoadProgressReport(this, verbose)).getReport(format);
  }

  /**
   * Get the processed block count in the current loading pass.
   * @return current block count
   */
  public long getCurrentBlockCount() {
    return mCurrentBlockCount.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LoadJob that = (LoadJob) o;
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
        && currentFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) currentFailureCount / mCurrentBlockCount.get() <= FAILURE_RATIO_THRESHOLD;
  }

  @Override
  public boolean isCurrentPassDone() {
    return  mFileIterator.isPresent() && !mFileIterator.get().hasNext() && !mBlockIterator.hasNext()
        && mRetryBlocks.isEmpty();
  }

  @Override
  public void initiateVerification() {
    Preconditions.checkState(isCurrentPassDone(), "Previous pass is not finished");
    mFileIterator = Optional.empty();
    mTotalBlockCount.addAndGet(mCurrentBlockCount.get());
    mTotalFailureCount.addAndGet(mCurrentFailureCount.get());
    mCurrentBlockCount.set(0);
    mCurrentFailureCount.set(0);
    mState = JobState.VERIFYING;
  }

  /**
   * get next load task.
   *
   * @param worker blocker to worker
   * @return the next task to run. If there is no task to run, return empty
   */
  @Override
  public Optional<LoadTask> getNextTask(WorkerInfo worker) {
    List<Block> blocks = getNextBatchBlocks(BATCH_SIZE);
    if (blocks.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new LoadTask(blocks));
  }

  /**
   * Get next batch of blocks.
   * @param count number of blocks
   * @return list of blocks
   */
  @VisibleForTesting
  public List<Block> getNextBatchBlocks(int count) {
    if (!mFileIterator.isPresent()) {
      mFileIterator = Optional.of(mFileIterable.iterator());
      if (!mFileIterator
          .get()
          .hasNext()) {
        return ImmutableList.of();
      }
      mCurrentFile = mFileIterator.get().next();
      if (!mFailedFiles.containsKey(mCurrentFile.getPath())) {
        mProcessedFileCount.incrementAndGet();
      }

      mBlockIterator = mCurrentFile.getBlockIds().listIterator();
    }
    ImmutableList.Builder<Block> batchBuilder = ImmutableList.builder();
    int i = 0;
    // retry failed blocks if there's too many failed blocks otherwise wait until no more new block
    if (mRetryBlocks.size() > RETRY_THRESHOLD
        || (!mFileIterator.get().hasNext() && !mBlockIterator.hasNext())) {
      while (i < count && !mRetryBlocks.isEmpty()) {
        batchBuilder.add(requireNonNull(mRetryBlocks.removeFirst()));
        i++;
      }
    }
    for (; i < count; i++) {
      if (!mBlockIterator.hasNext()) {
        if (!mFileIterator.get().hasNext()) {
          return batchBuilder.build();
        }
        mCurrentFile = mFileIterator.get().next();
        if (!mFailedFiles.containsKey(mCurrentFile.getPath())) {
          mProcessedFileCount.incrementAndGet();
        }
        mBlockIterator = mCurrentFile.getBlockIds().listIterator();
      }
      long blockId = mBlockIterator.next();
      BlockInfo blockInfo = mCurrentFile.getFileBlockInfo(blockId).getBlockInfo();
      if (blockInfo.getLocations().isEmpty()) {
        batchBuilder.add(buildBlock(mCurrentFile, blockId));
        mCurrentBlockCount.incrementAndGet();
        // would be inaccurate when we initial verification, and we retry un-retryable blocks
        mTotalByteCount.addAndGet(blockInfo.getLength());
      }
    }
    return batchBuilder.build();
  }

  /**
   * Add a block to retry later.
   * @param block the block that failed to load thus needing retry
   * @return whether the block is successfully added
   */
  @VisibleForTesting
  public boolean addBlockToRetry(Block block) {
    if (mRetryBlocks.size() >= RETRY_BLOCK_CAPACITY) {
      return false;
    }
    LOG.debug("Retry block {}", block);
    mRetryBlocks.add(block);
    mCurrentFailureCount.incrementAndGet();
    JOB_LOAD_BLOCK_FAIL.inc();
    return true;
  }

  /**
   * Add a block to failure summary.
   *
   * @param block   the block that failed to load and cannot be retried
   * @param message failure message
   * @param code    status code for exception
   */
  @VisibleForTesting
  public void addBlockFailure(Block block, String message, int code) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the first one.
    mFailedFiles.put(block.getUfsPath(),
        format("Status code: %s, message: %s", code, message));
    mCurrentFailureCount.incrementAndGet();
    JOB_LOAD_BLOCK_FAIL.inc();
  }

  private static Block buildBlock(FileInfo fileInfo, long blockId) {
    return Block.newBuilder().setBlockId(blockId)
        .setLength(fileInfo.getFileBlockInfo(blockId).getBlockInfo().getLength())
        .setUfsPath(fileInfo.getUfsPath())
        .setMountId(fileInfo.getMountId())
        .setOffsetInFile(fileInfo.getFileBlockInfo(blockId).getOffset())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Path", mPath)
        .add("User", mUser)
        .add("Bandwidth", mBandwidth)
        .add("UsePartialListing", mUsePartialListing)
        .add("VerificationEnabled", mVerificationEnabled)
        .add("RetryBlocks", mRetryBlocks)
        .add("FailedFiles", mFailedFiles)
        .add("StartTime", mStartTime)
        .add("ProcessedFileCount", mProcessedFileCount)
        .add("LoadedByteCount", mLoadedByteCount)
        .add("TotalBlockCount", mTotalBlockCount)
        .add("CurrentBlockCount", mCurrentBlockCount)
        .add("TotalFailureCount", mTotalFailureCount)
        .add("CurrentFailureCount", mCurrentFailureCount)
        .add("State", mState)
        .add("BatchSize", BATCH_SIZE)
        .add("FailedReason", mFailedReason)
        .add("FileIterator", mFileIterator)
        .add("CurrentFile", mCurrentFile)
        .add("BlockIterator", mBlockIterator)
        .add("EndTime", mEndTime)
        .toString();
  }

  @Override
  public Journal.JournalEntry toJournalEntry() {
    alluxio.proto.journal.Job.LoadJobEntry.Builder jobEntry = alluxio.proto.journal.Job.LoadJobEntry
        .newBuilder()
        .setLoadPath(mPath)
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
  public boolean processResponse(LoadTask loadTask) {
    try {
      long totalBytes = loadTask.getBlocks().stream()
          .map(Block::getLength)
          .reduce(Long::sum)
          .orElse(0L);
      LoadResponse response = loadTask.getResponseFuture().get();
      if (response.getStatus() != TaskStatus.SUCCESS) {
        LOG.debug(format("Get failure from worker: %s", response.getBlockStatusList()));
        for (BlockStatus status : response.getBlockStatusList()) {
          totalBytes -= status.getBlock().getLength();
          if (!isHealthy() || !status.getRetryable() || !addBlockToRetry(
              status.getBlock())) {
            addBlockFailure(status.getBlock(), status.getMessage(), status.getCode());
          }
        }
      }
      addLoadedBytes(totalBytes);
      JOB_LOAD_BLOCK_COUNT.inc(
          loadTask.getBlocks().size() - response.getBlockStatusCount());
      JOB_LOAD_BLOCK_SIZE.inc(totalBytes);
      JOB_LOAD_RATE.mark(totalBytes);
      return response.getStatus() != TaskStatus.FAILURE;
    }
    catch (ExecutionException e) {
      LOG.warn("exception when trying to get load response.", e.getCause());
      for (Block block : loadTask.getBlocks()) {
        if (isHealthy()) {
          addBlockToRetry(block);
        }
        else {
          AlluxioRuntimeException exception = AlluxioRuntimeException.from(e.getCause());
          addBlockFailure(block, exception.getMessage(), exception.getStatus().getCode()
                                                                       .value());
        }
      }
      return false;
    }
    catch (CancellationException e) {
      LOG.warn("Task get canceled and will retry.", e);
      loadTask.getBlocks().forEach(this::addBlockToRetry);
      return true;
    }
    catch (InterruptedException e) {
      loadTask.getBlocks().forEach(this::addBlockToRetry);
      Thread.currentThread().interrupt();
      // We don't count InterruptedException as task failure
      return true;
    }
  }

  @Override
  public void updateJob(Job<?> job) {
    if (!(job instanceof LoadJob)) {
      throw new IllegalArgumentException("Job is not a LoadJob: " + job);
    }
    LoadJob targetJob = (LoadJob) job;
    updateBandwidth(targetJob.getBandwidth());
    setVerificationEnabled(targetJob.isVerificationEnabled());
  }

  /**
   * Is verification enabled.
   *
   * @return whether verification is enabled
   */
  @Override
  public boolean needVerification() {
    return mVerificationEnabled && mCurrentBlockCount.get() > 0;
  }

  /**
   * Loads blocks in a UFS through an Alluxio worker.
   */
  public class LoadTask extends Task<LoadResponse> {

    /**
     * @return blocks to load
     */
    public List<Block> getBlocks() {
      return mBlocks;
    }

    private final List<Block> mBlocks;

    /**
     * Creates a new instance of {@link LoadTask}.
     *
     * @param blocks blocks to load
     */
    public LoadTask(List<Block> blocks) {
      mBlocks = blocks;
    }

    @Override
    public ListenableFuture<LoadResponse> run(BlockWorkerClient workerClient) {
      LoadRequest.Builder request1 = LoadRequest
          .newBuilder()
          .addAllBlocks(mBlocks);
      UfsReadOptions.Builder options = UfsReadOptions
          .newBuilder()
          .setTag(mJobId)
          .setPositionShort(false);
      if (mBandwidth.isPresent()) {
        options.setBandwidth(mBandwidth.getAsLong());
      }
      mUser.ifPresent(options::setUser);
      LoadRequest request = request1
          .setOptions(options.build())
          .build();
      return workerClient.load(request);
    }
  }

  private static class LoadProgressReport {
    private final boolean mVerbose;
    private final JobState mJobState;
    private final Long mBandwidth;
    private final boolean mVerificationEnabled;
    private final long mProcessedFileCount;
    private final long mLoadedByteCount;
    private final Long mTotalByteCount;
    private final Long mThroughput;
    private final double mFailurePercentage;
    private final AlluxioRuntimeException mFailureReason;
    private final long mFailedFileCount;
    private final Map<String, String> mFailedFilesWithReasons;

    public LoadProgressReport(LoadJob job, boolean verbose)
    {
      mVerbose = verbose;
      mJobState = job.mState;
      mBandwidth = job.mBandwidth.isPresent() ? job.mBandwidth.getAsLong() : null;
      mVerificationEnabled = job.mVerificationEnabled;
      mProcessedFileCount = job.mProcessedFileCount.get();
      mLoadedByteCount = job.mLoadedByteCount.get();
      if (!job.mUsePartialListing && job.mFileIterator.isPresent()) {
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
      long blockCount = job.mTotalBlockCount.get() + job.mCurrentBlockCount.get();
      if (blockCount > 0) {
        mFailurePercentage =
            ((double) (job.mTotalFailureCount.get() + job.mCurrentFailureCount.get()) / blockCount)
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
        mFailedFilesWithReasons = null;
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
      progress.append(format("\tFiles Processed: %d%n", mProcessedFileCount));
      progress.append(format("\tBytes Loaded: %s%s%n",
          FormatUtils.getSizeFromBytes(mLoadedByteCount),
          mTotalByteCount == null
              ? "" : format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      if (mThroughput != null) {
        progress.append(format("\tThroughput: %s/s%n",
            FormatUtils.getSizeFromBytes(mThroughput)));
      }
      progress.append(format("\tBlock load failure rate: %.2f%%%n", mFailurePercentage));
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
  public static final Counter JOB_LOAD_BLOCK_COUNT =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_COUNT.getName());
  public static final Counter JOB_LOAD_BLOCK_FAIL =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_FAIL.getName());
  public static final Counter JOB_LOAD_BLOCK_SIZE =
          MetricsSystem.counter(MetricKey.MASTER_JOB_LOAD_BLOCK_SIZE.getName());
  public static final Meter JOB_LOAD_RATE =
          MetricsSystem.meter(MetricKey.MASTER_JOB_LOAD_RATE.getName());
}
