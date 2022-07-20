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
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioRuntimeException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.NotFoundRuntimeException;
import alluxio.exception.status.UnauthenticatedRuntimeException;
import alluxio.grpc.Block;
import alluxio.grpc.ListStatusPOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.proto.journal.Job;
import alluxio.wire.FileInfo;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;

/**
 * This class should only be manipulated from the scheduler thread in LoadManager
 * thus the state changing functions are not thread safe.
 */
@NotThreadSafe
public class LoadJob {
  private static final double FAILURE_RATIO_THRESHOLD = 0.05;
  private static final int FAILURE_COUNT_THRESHOLD = 100;
  private static final int RETRY_BLOCK_CAPACITY = 1000;
  private static final double RETRY_THRESHOLD = 0.8 * RETRY_BLOCK_CAPACITY;
  private static final ListStatusContext LIST_STATUS_CONTEXT = ListStatusContext.create(
      ListStatusPOptions.newBuilder().setRecursive(true));

  /**
   * Load status.
   */
  public enum LoadStatus
  {
    LOADING,
    VERIFYING,
    STOPPED,
    SUCCEEDED,
    FAILED;

    /**
     * Convert LoadStatus to PJobStatus.
     * @param status load status
     * @return the corresponding PJobStatus
     */
    public static Job.PJobStatus toProto(LoadStatus status) {
      switch (status) {
        case LOADING:
        case VERIFYING:
          return Job.PJobStatus.CREATED;
        case STOPPED:
          return Job.PJobStatus.STOPPED;
        case SUCCEEDED:
          return Job.PJobStatus.SUCCEEDED;
        case FAILED:
          return Job.PJobStatus.FAILED;
        default:
          throw new IllegalArgumentException(String.format("Unknown status %s", status));
      }
    }

    /**
     * Convert PJobStatus to LoadStatus.
     * @param jobStatus protobuf job status
     * @return the corresponding LoadStatus
     */
    public static LoadStatus fromProto(Job.PJobStatus jobStatus) {
      switch (jobStatus) {
        case CREATED:
          return LOADING;
        case STOPPED:
          return STOPPED;
        case SUCCEEDED:
          return SUCCEEDED;
        case FAILED:
          return FAILED;
        default:
          throw new IllegalArgumentException(String.format("Unknown job status %s", jobStatus));
      }
    }
  }

  // Job configurations
  private final String mPath;
  private int mBandwidth;
  private boolean mVerificationEnabled;

  // Job states
  private final LinkedList<Block> mRetryBlocks = new LinkedList<>();
  private final Map<String, Status> mFailedFiles = new HashMap<>();
  private LoadStatus mStatus;
  private Optional<List<FileInfo>> mFiles = Optional.empty();
  private ListIterator<FileInfo> mFileIterator;
  private FileInfo mCurrentFile;
  private ListIterator<Long> mBlockIterator;
  private long mTotalBlockCount;
  private long mCurrentBlockCount;
  private long mTotalFailureCount;
  private long mCurrentFailureCount;

  /**
   * Constructor.
   * @param path file path
   * @param bandwidth bandwidth
   */
  public LoadJob(String path, int bandwidth) {
    this(path, bandwidth, false);
  }

  /**
   * Constructor.
   * @param path file path
   * @param bandwidth bandwidth
   * @param verificationEnabled whether to verify the job after loaded
   */
  public LoadJob(String path, int bandwidth, boolean verificationEnabled) {
    mPath = requireNonNull(path, "path is null");
    Preconditions.checkArgument(
        bandwidth > 0, String.format("bandwidth should be greater than 0, get %d", bandwidth));
    mBandwidth = bandwidth;
    mVerificationEnabled = verificationEnabled;
    mStatus = LoadStatus.LOADING;
  }

  /**
   * Get load file path.
   * @return file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get bandwidth.
   * @return the allocated bandwidth
   */
  public int getBandWidth() {
    return mBandwidth;
  }

  /**
   * Update bandwidth.
   * @param bandwidth new bandwidth
   */
  public void updateBandwidth(int bandwidth) {
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
  public LoadStatus getStatus() {
    return mStatus;
  }

  /**
   * Set load status.
   * @param status new status
   */
  public void setStatus(LoadStatus status) {
    mStatus = status;
  }

  /**
   * Get the processed block count in the current loading pass.
   * @return current block count
   */
  public long getCurrentBlockCount() {
    return mCurrentBlockCount;
  }

  /**
   * Get the total processed block count for this job.
   * @return total block count
   */
  public long getTotalBlockCount() {
    return mTotalBlockCount;
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
    return mStatus != LoadStatus.FAILED
        && mCurrentFailureCount <= FAILURE_COUNT_THRESHOLD
        || (double) mCurrentFailureCount / mCurrentBlockCount <= FAILURE_RATIO_THRESHOLD;
  }

  /**
   * Check whether the load job is still running.
   * @return true if the load job is running, false if not
   */
  public boolean isRunning() {
    return mStatus == LoadStatus.LOADING || mStatus == LoadStatus.VERIFYING;
  }

  /**
   * Check whether the load job is finished.
   * @return true if the load job is finished, false if not
   */
  public boolean isDone() {
    return mStatus == LoadStatus.SUCCEEDED || mStatus == LoadStatus.FAILED;
  }

  /**
   * Check whether the current loading pass is finished.
   * @return true if the load job is finished, false if not
   */
  public boolean isCurrentLoadDone() {
    return mFiles.isPresent() && !mFileIterator.hasNext() && !mBlockIterator.hasNext()
        && mRetryBlocks.isEmpty();
  }

  /**
   * Initiate a verification pass. This will re-list the directory and find
   * any unloaded files / blocks and try to load them again.
   */
  public void initiateVerification() {
    Preconditions.checkState(
        mFiles.isPresent() && !mFileIterator.hasNext() && !mBlockIterator.hasNext(),
        "Previous pass is not finished");
    mFiles = Optional.empty();
    mTotalBlockCount += mCurrentBlockCount;
    mTotalFailureCount += mCurrentFailureCount;
    mCurrentBlockCount = 0;
    mCurrentFailureCount = 0;
    mStatus = LoadStatus.VERIFYING;
  }

  /**
   * Get next batch of blocks.
   * @param fileSystemMaster file system master to fetch file infos
   * @param count number of blocks
   * @return list of blocks
   */
  public List<Block> getNextBatch(FileSystemMaster fileSystemMaster, int count) {
    if (!mFiles.isPresent()) {
      mFiles = Optional.of(listFileInfos(fileSystemMaster));
      if (mFiles.get().isEmpty()) {
        return ImmutableList.of();
      }
      mFileIterator = mFiles.get().listIterator();
      mCurrentFile = mFileIterator.next();
      mBlockIterator = mCurrentFile.getBlockIds().listIterator();
    }

    ImmutableList.Builder<Block> batchBuilder = ImmutableList.builder();
    int i = 0;
    if (mRetryBlocks.size() > RETRY_THRESHOLD
        || (!mFileIterator.hasNext() && !mBlockIterator.hasNext())) {
      while (i < count && !mRetryBlocks.isEmpty()) {
        batchBuilder.add(requireNonNull(mRetryBlocks.removeFirst()));
        i++;
      }
    }
    for (; i < count; i++) {
      if (!mBlockIterator.hasNext()) {
        if (!mFileIterator.hasNext()) {
          return batchBuilder.build();
        }
        mCurrentFile = mFileIterator.next();
        mBlockIterator = mCurrentFile.getBlockIds().listIterator();
      }
      long blockId = mBlockIterator.next();
      if (mCurrentFile.getFileBlockInfo(blockId).getBlockInfo().getLocations().isEmpty()) {
        batchBuilder.add(buildBlock(mCurrentFile, blockId));
        mCurrentBlockCount++;
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
    mRetryBlocks.add(block);
    mCurrentFailureCount++;
    return true;
  }

  /**
   * Add a block to failure summary.
   * @param block the block that failed to load and cannot be retried
   * @param status status for failure
   */
  public void addBlockFailure(Block block, Status status) {
    // When multiple blocks of the same file failed to load, from user's perspective,
    // it's not hugely important what are the reasons for each specific failure,
    // if they are different, so we will just keep the last one.
    mFailedFiles.put(block.getUfsPath(), status);
    mCurrentFailureCount++;
  }

  private List<FileInfo> listFileInfos(FileSystemMaster fileSystemMaster) {
    try {
      return fileSystemMaster.listStatus(new AlluxioURI(mPath), LIST_STATUS_CONTEXT);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      throw new NotFoundRuntimeException(e);
    } catch (AccessControlException e) {
      throw new UnauthenticatedRuntimeException(e);
    } catch (IOException e) {
      throw AlluxioRuntimeException.fromIOException(e);
    }
  }

  private static Block buildBlock(FileInfo fileInfo, long blockId) {
    return Block.newBuilder().setBlockId(blockId)
        .setBlockSize(fileInfo.getBlockSizeBytes())
        .setUfsPath(fileInfo.getUfsPath())
        .setMountId(fileInfo.getMountId())
        .setOffsetInFile(fileInfo.getFileBlockInfo(blockId).getOffset())
        .build();
  }
}
