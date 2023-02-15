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

import static java.util.Objects.requireNonNull;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.runtime.UnauthenticatedRuntimeException;
import alluxio.grpc.Block;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.LoadProgressReportFormat;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.proto.journal.Job;
import alluxio.proto.journal.Journal;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class should only be manipulated from the scheduler thread in LoadManager
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class LoadJob {
  private static final Logger LOG = LoggerFactory.getLogger(LoadJob.class);
  private static final double FAILURE_RATIO_THRESHOLD = 0.05;
  private static final int FAILURE_COUNT_THRESHOLD = 100;
  private static final int RETRY_BLOCK_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_BLOCK_CAPACITY;
  private static final int BATCH_SIZE = Configuration.getInt(PropertyKey.JOB_BATCH_SIZE);
  // Job configurations
  private final String mPath;
  private final Optional<String> mUser;
  private OptionalLong mBandwidth;
  private boolean mUsePartialListing;
  private boolean mVerificationEnabled;

  // Job states
  private final LinkedList<Block> mRetryBlocks = new LinkedList<>();
  private final Map<String, String> mFailedFiles = new HashMap<>();
  private final long mStartTime;
  private final AtomicLong mProcessedFileCount = new AtomicLong();
  private final AtomicLong mTotalFileCount = new AtomicLong();
  private final AtomicLong mLoadedByteCount = new AtomicLong();

  private final AtomicLong mTotalByteCount = new AtomicLong();
  private final AtomicLong mTotalBlockCount = new AtomicLong();
  private final AtomicLong mCurrentBlockCount = new AtomicLong();
  private final AtomicLong mTotalFailureCount = new AtomicLong();
  private final AtomicLong mCurrentFailureCount = new AtomicLong();
  private final String mJobId;
  private LoadJobState mState;
  private Optional<AlluxioRuntimeException> mFailedReason = Optional.empty();
  private Optional<FileIterator> mFileIterator = Optional.empty();
  private FileInfo mCurrentFile;
  private Iterator<Long> mBlockIterator = Collections.emptyIterator();
  private OptionalLong mEndTime = OptionalLong.empty();

  /**
   * Constructor.
   * @param path file path
   * @param user user for authentication
   * @param bandwidth bandwidth
   */
  @VisibleForTesting
  public LoadJob(String path, String user, OptionalLong bandwidth) {
    this(path, Optional.of(user), UUID.randomUUID().toString(), bandwidth, false, false);
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
   */
  public LoadJob(
      String path,
      Optional<String> user, String jobId, OptionalLong bandwidth,
      boolean usePartialListing,
      boolean verificationEnabled) {
    mPath = requireNonNull(path, "path is null");
    mUser = requireNonNull(user, "user is null");
    mJobId = requireNonNull(jobId, "jobId is null");
    Preconditions.checkArgument(
        !bandwidth.isPresent() || bandwidth.getAsLong() > 0,
        String.format("bandwidth should be greater than 0 if provided, get %s", bandwidth));
    mBandwidth = bandwidth;
    mUsePartialListing = usePartialListing;
    mVerificationEnabled = verificationEnabled;
    mStartTime = System.currentTimeMillis();
    mState = LoadJobState.LOADING;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get user.
   * @return user
   */
  public Optional<String> getUser() {
    return mUser;
  }

  /**
   * Get end time.
   * @return end time
   */
  public OptionalLong getEndTime() {
    return mEndTime;
  }

  /**
   * Get bandwidth.
   * @return the allocated bandwidth
   */
  public OptionalLong getBandwidth() {
    return mBandwidth;
  }

  /**
   * Update end time.
   * @param time time in ms
   */
  public void setEndTime(long time) {
    mEndTime = OptionalLong.of(time);
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
   * Get load status.
   * @return the load job's status
   */
  public LoadJobState getJobState() {
    return mState;
  }

  /**
   * Set load state.
   * @param state new state
   */
  public void setJobState(LoadJobState state) {
    LOG.debug("Change JobState to {} for job {}", state, this);
    mState = state;
    if (!isRunning()) {
      mEndTime = OptionalLong.of(System.currentTimeMillis());
    }
  }

  /**
   * Get uniq tag.
   * @return the tag
   */
  public String getJobId() {
    return mJobId;
  }

  /**
   * Set load state to FAILED with given reason.
   * @param reason failure exception
   */
  public void failJob(AlluxioRuntimeException reason) {
    setJobState(LoadJobState.FAILED);
    mFailedReason = Optional.of(reason);
    LoadManager.JOB_LOAD_FAIL.inc();
  }

  /**
   * Get batch size.
   * @return batch size
   */
  public int getBatchSize() {
    return BATCH_SIZE;
  }

  /**
   * Add bytes to total loaded bytes.
   * @param bytes bytes to be added to total
   */
  public void addLoadedBytes(long bytes) {
    mLoadedByteCount.addAndGet(bytes);
  }

  /**
   * Get load job progress.
   * @param format report format
   * @param verbose whether to include error details in the report
   * @return the load progress report
   */
  public String getProgress(LoadProgressReportFormat format, boolean verbose) {
    return (new LoadProgressReport(this, verbose)).getReport(format);
  }

  /**
   * Get the processed block count in the current loading pass.
   * @return current block count
   */
  public long getCurrentBlockCount() {
    return mCurrentBlockCount.get();
  }

  /**
   * Get the total processed block count for this job.
   * @return total block count
   */
  public long getTotalBlockCount() {
    return mTotalBlockCount.get();
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
    return Objects.equal(mPath, that.mPath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPath);
  }

  /**
   * Check whether the load job is healthy.
   * @return true if the load job is healthy, false if not
   */
  public boolean isHealthy() {
    long currentFailureCount = mCurrentFailureCount.get();
    return mState != LoadJobState.FAILED
        && currentFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) currentFailureCount / mCurrentBlockCount.get() <= FAILURE_RATIO_THRESHOLD;
  }

  /**
   * Check whether the load job is still running.
   * @return true if the load job is running, false if not
   */
  public boolean isRunning() {
    return mState == LoadJobState.LOADING || mState == LoadJobState.VERIFYING;
  }

  /**
   * Check whether the load job is finished.
   * @return true if the load job is finished, false if not
   */
  public boolean isDone() {
    return mState == LoadJobState.SUCCEEDED || mState == LoadJobState.FAILED;
  }

  /**
   * Check whether the current loading pass is finished.
   * @return true if the load job is finished, false if not
   */
  public boolean isCurrentLoadDone() {
    return mFileIterator.isPresent() && !mFileIterator.get().hasNext() && !mBlockIterator.hasNext()
        && mRetryBlocks.isEmpty();
  }

  /**
   * Initiate a verification pass. This will re-list the directory and find
   * any unloaded files / blocks and try to load them again.
   */
  public void initiateVerification() {
    Preconditions.checkState(isCurrentLoadDone(), "Previous pass is not finished");
    mFileIterator = Optional.empty();
    mTotalBlockCount.addAndGet(mCurrentBlockCount.get());
    mTotalFailureCount.addAndGet(mCurrentFailureCount.get());
    mCurrentBlockCount.set(0);
    mCurrentFailureCount.set(0);
    mState = LoadJobState.VERIFYING;
  }

  /**
   * Get next batch of blocks.
   * @param fileSystemMaster file system master to fetch file infos
   * @param count number of blocks
   * @return list of blocks
   */
  public List<Block> getNextBatch(FileSystemMaster fileSystemMaster, int count) {
    if (!mFileIterator.isPresent()) {
      mFileIterator =
          Optional.of(new FileIterator(fileSystemMaster, mPath, mUser, mUsePartialListing));
      if (!mFileIterator.get().hasNext()) {
        return ImmutableList.of();
      }
      mCurrentFile = mFileIterator.get().next();
      mProcessedFileCount.incrementAndGet();
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
        mProcessedFileCount.incrementAndGet();
        mBlockIterator = mCurrentFile.getBlockIds().listIterator();
      }
      long blockId = mBlockIterator.next();
      BlockInfo blockInfo = mCurrentFile.getFileBlockInfo(blockId).getBlockInfo();
      if (blockInfo.getLocations().isEmpty()) {
        batchBuilder.add(buildBlock(mCurrentFile, blockId));
        mCurrentBlockCount.incrementAndGet();
      }
    }
    return batchBuilder.build();
  }

  /**
   * Add a block to retry later.
   * @param block the block that failed to load thus needing retry
   * @return whether the block is successfully added
   */
  public boolean addBlockToRetry(Block block) {
    if (mRetryBlocks.size() >= RETRY_BLOCK_CAPACITY) {
      return false;
    }
    LOG.debug("Retry block {}", block);
    mRetryBlocks.add(block);
    mCurrentFailureCount.incrementAndGet();
    LoadManager.JOB_LOAD_BLOCK_FAIL.inc();
    return true;
  }

  /**
   * Add a block to failure summary.
   *
   * @param block   the block that failed to load and cannot be retried
   * @param message failure message
   * @param code    status code for exception
   */
  public void addBlockFailure(Block block, String message, int code) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the first one.
    mFailedFiles.put(block.getUfsPath(),
        String.format("Status code: %s, message: %s", code, message));
    mCurrentFailureCount.incrementAndGet();
    LoadManager.JOB_LOAD_BLOCK_FAIL.inc();
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

  /**
   * @return journal entry of job
   */
  public Journal.JournalEntry toJournalEntry() {
    Job.LoadJobEntry.Builder jobEntry = Job.LoadJobEntry
        .newBuilder()
        .setLoadPath(mPath)
        .setState(LoadJobState.toProto(mState))
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
   * Get journal entry of the job.
   *
   * @param loadJobEntry journal entry
   * @return journal entry of the job
   */
  public static LoadJob fromJournalEntry(Job.LoadJobEntry loadJobEntry) {
    LoadJob job = new LoadJob(loadJobEntry.getLoadPath(),
        loadJobEntry.hasUser() ? Optional.of(loadJobEntry.getUser()) : Optional.empty(),
        loadJobEntry.getJobId(),
        loadJobEntry.hasBandwidth() ? OptionalLong.of(loadJobEntry.getBandwidth()) :
            OptionalLong.empty(), loadJobEntry.getPartialListing(), loadJobEntry.getVerify());
    job.setJobState(LoadJobState.fromProto(loadJobEntry.getState()));
    if (loadJobEntry.hasEndTime()) {
      job.setEndTime(loadJobEntry.getEndTime());
    }
    return job;
  }

  /**
   * Get duration in seconds.
   * @return job duration in seconds
   */
  @VisibleForTesting
  public long getDurationInSec() {
    return (mEndTime.orElse(System.currentTimeMillis()) - mStartTime) / 1000;
  }

  private class FileIterator implements Iterator<FileInfo> {
    private final ListStatusPOptions.Builder mListOptions =
        ListStatusPOptions.newBuilder().setRecursive(true);
    private static final int PARTIAL_LISTING_BATCH_SIZE = 100;
    private final FileSystemMaster mFileSystemMaster;
    private final String mPath;
    private final Optional<String> mUser;
    private final boolean mUsePartialListing;
    private String mStartAfter = "";
    private List<FileInfo> mFiles;
    private Iterator<FileInfo> mFileInfoIterator;

    public FileIterator(FileSystemMaster fileSystemMaster, String path,
        Optional<String> user, boolean usePartialListing) {
      mFileSystemMaster = requireNonNull(fileSystemMaster, "fileSystemMaster is null");
      mPath = requireNonNull(path, "path is null");
      mUser = requireNonNull(user, "user is null");
      mUsePartialListing = usePartialListing;
      if (usePartialListing) {
        partialListFileInfos();
      } else {
        listFileInfos(ListStatusContext.create(mListOptions));
      }
    }

    @Override
    public boolean hasNext()
    {
      if (mUsePartialListing && !mFileInfoIterator.hasNext()) {
        partialListFileInfos();
      }
      return mFileInfoIterator.hasNext();
    }

    @Override
    public FileInfo next()
    {
      if (mUsePartialListing && !mFileInfoIterator.hasNext()) {
        partialListFileInfos();
      }
      return mFileInfoIterator.next();
    }

    private void partialListFileInfos() {
      if (!mStartAfter.isEmpty()) {
        mListOptions.setDisableAreDescendantsLoadedCheck(true);
      }
      ListStatusContext context = ListStatusContext.create(ListStatusPartialPOptions.newBuilder()
          .setOptions(mListOptions)
          .setBatchSize(PARTIAL_LISTING_BATCH_SIZE)
          .setStartAfter(mStartAfter));
      listFileInfos(context);
      if (mFiles.size() > 0) {
        mStartAfter = mFiles.get(mFiles.size() - 1).getPath();
      }
    }

    private void listFileInfos(ListStatusContext context) {
      try {
        AuthenticatedClientUser.set(mUser.orElse(null));
        mFiles = mFileSystemMaster.listStatus(new AlluxioURI(mPath), context).stream().filter(
            fileInfo -> !fileInfo.isFolder() && fileInfo.isCompleted()
                && fileInfo.getInAlluxioPercentage() != 100).collect(Collectors.toList());
        mFileInfoIterator = mFiles.iterator();
      } catch (FileDoesNotExistException | InvalidPathException e) {
        throw new NotFoundRuntimeException(e);
      } catch (AccessControlException e) {
        throw new UnauthenticatedRuntimeException(e);
      } catch (IOException e) {
        throw AlluxioRuntimeException.from(e);
      } finally {
        AuthenticatedClientUser.remove();
      }
      List<FileInfo> fileInfoStream = mFiles
          .stream().filter(fileInfo -> !mFailedFiles.containsKey(fileInfo.getPath())).collect(
              Collectors.toList());
      mTotalFileCount.addAndGet(fileInfoStream.size());
      mTotalByteCount.addAndGet(fileInfoStream.stream()
          .map(FileInfo::getFileBlockInfos)
          .flatMap(Collection::stream)
          .map(FileBlockInfo::getBlockInfo)
          .filter(blockInfo -> blockInfo.getLocations().isEmpty())
          .map(BlockInfo::getLength)
          .reduce(Long::sum)
          .orElse(0L));
    }
  }

  private static class LoadProgressReport {
    private final boolean mVerbose;
    private final LoadJobState mJobState;
    private final Long mBandwidth;
    private final boolean mVerificationEnabled;
    private final long mProcessedFileCount;
    private final Long mTotalFileCount;
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
      if (job.mFileIterator.isPresent() && !job.mFileIterator.get().mUsePartialListing) {
        mTotalFileCount = job.mTotalFileCount.get();
        mTotalByteCount = job.mTotalByteCount.get();
      }
      else {
        mTotalFileCount = null;
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

    public String getReport(LoadProgressReportFormat format)
    {
      switch (format) {
        case TEXT:
          return getTextReport();
        case JSON:
          return getJsonReport();
        default:
          throw new InvalidArgumentRuntimeException(
              String.format("Unknown load progress report format: %s", format));
      }
    }

    private String getTextReport() {
      StringBuilder progress = new StringBuilder();
      progress.append(
          String.format("\tSettings:\tbandwidth: %s\tverify: %s%n",
              mBandwidth == null ? "unlimited" : mBandwidth,
              mVerificationEnabled));
      progress.append(String.format("\tJob State: %s%s%n", mJobState,
          mFailureReason == null
              ? "" : String.format(
                  " (%s: %s)",
              mFailureReason.getClass().getName(),
              mFailureReason.getMessage())));
      if (mVerbose && mFailureReason != null) {
        for (StackTraceElement stack : mFailureReason.getStackTrace()) {
          progress.append(String.format("\t\t%s%n", stack.toString()));
        }
      }
      progress.append(String.format("\tFiles Processed: %d%s%n", mProcessedFileCount,
          mTotalFileCount == null
              ? "" : String.format(" out of %s", mTotalFileCount)));
      progress.append(String.format("\tBytes Loaded: %s%s%n",
          FormatUtils.getSizeFromBytes(mLoadedByteCount),
          mTotalByteCount == null
              ? "" : String.format(" out of %s", FormatUtils.getSizeFromBytes(mTotalByteCount))));
      if (mThroughput != null) {
        progress.append(String.format("\tThroughput: %s/s%n",
            FormatUtils.getSizeFromBytes(mThroughput)));
      }
      progress.append(String.format("\tBlock load failure rate: %.2f%%%n", mFailurePercentage));
      progress.append(String.format("\tFiles Failed: %s%n", mFailedFileCount));
      if (mVerbose && mFailedFilesWithReasons != null) {
        mFailedFilesWithReasons.forEach((fileName, reason) ->
            progress.append(String.format("\t\t%s: %s%n", fileName, reason)));
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
}
