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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.ReadType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.grpc.CacheRequest;
import alluxio.resource.CloseableResource;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An implementation of {@link FileInStream} for data stored in Alluxio.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 *
 * The internal bookkeeping works as follows:
 *
 * 1. {@link #updateStream()} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #mPosition}.
 * 2. {@link #mPosition} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream()} is called.
 * 3. {@link #updateStream()} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class AlluxioFileInStream extends FileInStream {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFileInStream.class);

  private Supplier<RetryPolicy> mRetryPolicySupplier;
  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;
  private final boolean mPassiveCachingEnabled;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;

  /** Cached block stream for the positioned read API. */
  private BlockInStream mCachedPositionedReadStream;

  /** The last block id for which async cache was triggered. */
  private long mLastBlockIdCached;

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  private Closer mCloser;

  protected AlluxioFileInStream(URIStatus status, InStreamOptions options,
      FileSystemContext context) {
    mCloser = Closer.create();
    // Acquire a resource to block FileSystemContext reinitialization, this needs to be done before
    // using mContext.
    // The resource will be released in close().
    mContext = context;
    mCloser.register(mContext.blockReinit());
    try {
      AlluxioConfiguration conf = mContext.getPathConf(new AlluxioURI(status.getPath()));
      mPassiveCachingEnabled = conf.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
      final Duration blockReadRetryMaxDuration =
          conf.getDuration(PropertyKey.USER_BLOCK_READ_RETRY_MAX_DURATION);
      final Duration blockReadRetrySleepBase =
          conf.getDuration(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MIN);
      final Duration blockReadRetrySleepMax =
          conf.getDuration(PropertyKey.USER_BLOCK_READ_RETRY_SLEEP_MAX);
      mRetryPolicySupplier = () -> RetryUtils.defaultBlockReadRetry(
          blockReadRetryMaxDuration, blockReadRetrySleepBase, blockReadRetrySleepMax);
      mStatus = status;
      mOptions = options;
      mBlockStore = AlluxioBlockStore.create(mContext);
      mLength = mStatus.getLength();
      mBlockSize = mStatus.getBlockSizeBytes();
      mPosition = 0;
      mBlockInStream = null;
      mCachedPositionedReadStream = null;
      mLastBlockIdCached = 0;
    } catch (Throwable t) {
      // If there is any exception, including RuntimeException such as thrown by conf.getBoolean,
      // release the acquired resource, otherwise, FileSystemContext reinitialization will be
      // blocked forever.
      throw CommonUtils.closeAndRethrowRuntimeException(mCloser, t);
    }
  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    RetryPolicy retry = mRetryPolicySupplier.get();
    IOException lastException = null;
    while (retry.attempt()) {
      try {
        updateStream();
        int result = mBlockInStream.read();
        if (result != -1) {
          mPosition++;
        }
        if (mBlockInStream.remaining() == 0) {
          closeBlockInStream(mBlockInStream);
        }
        return result;
      } catch (IOException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    throw lastException;
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= byteBuffer.capacity(),
        PreconditionMessage.ERR_BUFFER_STATE.toString(), byteBuffer.capacity(), off, len);
    if (len == 0) {
      return 0;
    }
    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    RetryPolicy retry = mRetryPolicySupplier.get();
    IOException lastException = null;
    while (bytesLeft > 0 && mPosition != mLength && retry.attempt()) {
      try {
        updateStream();
        int bytesRead = mBlockInStream.read(byteBuffer, currentOffset, bytesLeft);
        if (bytesRead > 0) {
          bytesLeft -= bytesRead;
          currentOffset += bytesRead;
          mPosition += bytesRead;
        }
        retry = mRetryPolicySupplier.get();
        lastException = null;
        if (mBlockInStream.remaining() == 0) {
          closeBlockInStream(mBlockInStream);
        }
      } catch (IOException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream(mBlockInStream);
    closeBlockInStream(mCachedPositionedReadStream);
    mCloser.close();
  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mLength) {
      return -1;
    }

    if (len < mContext.getPathConf(new AlluxioURI(mStatus.getPath()))
        .getBytes(PropertyKey.USER_FILE_SEQUENTIAL_PREAD_THRESHOLD)) {
      mOptions.setPositionShort(true);
    }
    int lenCopy = len;
    RetryPolicy retry = mRetryPolicySupplier.get();
    IOException lastException = null;
    while (len > 0 && retry.attempt()) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      try {
        // Positioned read may be called multiple times for the same block. Caching the in-stream
        // allows us to avoid the block store rpc to open a new stream for each call.
        if (mCachedPositionedReadStream == null) {
          mCachedPositionedReadStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        } else if (mCachedPositionedReadStream.getId() != blockId) {
          closeBlockInStream(mCachedPositionedReadStream);
          mCachedPositionedReadStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        }
        long offset = pos % mBlockSize;
        int bytesRead = mCachedPositionedReadStream.positionedRead(offset, b, off,
            (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
        retry = mRetryPolicySupplier.get();
        lastException = null;
        BlockInStream.BlockInStreamSource source = mCachedPositionedReadStream.getSource();
        if (source != BlockInStream.BlockInStreamSource.NODE_LOCAL
            && source != BlockInStream.BlockInStreamSource.PROCESS_LOCAL) {
          triggerAsyncCaching(mCachedPositionedReadStream);
        }
        if (bytesRead == mBlockSize - offset) {
          mCachedPositionedReadStream.close();
          mCachedPositionedReadStream = null;
        }
      } catch (IOException e) {
        lastException = e;
        if (mCachedPositionedReadStream != null) {
          handleRetryableException(mCachedPositionedReadStream, e);
          mCachedPositionedReadStream = null;
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return lenCopy - len;
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition = pos;
      return;
    }

    long delta = pos - mPosition;
    if (delta <= mBlockInStream.remaining() && delta >= -mBlockInStream.getPos()) { // within block
      mBlockInStream.seek(mBlockInStream.getPos() + delta);
    } else { // close the underlying stream as the new position is no longer in bounds
      closeBlockInStream(mBlockInStream);
    }
    mPosition += delta;
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
    }

    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    long blockId = mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
    BlockInfo blockInfo = mStatus.getBlockInfo(blockId);
    if (blockInfo == null) {
      throw new IOException("No BlockInfo for block(id=" + blockId + ") of file"
          + "(id=" + mStatus.getFileId() + ", path=" + mStatus.getPath() + ")");
    }
    // Create stream
    boolean isBlockInfoOutdated = true;
    // blockInfo is "outdated" when all the locations in that blockInfo are failed workers,
    // if there is at least one location that is not a failed worker, then it's not outdated.
    if (mFailedWorkers.isEmpty() || mFailedWorkers.size() < blockInfo.getLocations().size()) {
      isBlockInfoOutdated = false;
    } else {
      for (BlockLocation location : blockInfo.getLocations()) {
        if (!mFailedWorkers.containsKey(location.getWorkerAddress())) {
          isBlockInfoOutdated = false;
          break;
        }
      }
    }
    if (isBlockInfoOutdated) {
      mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
    } else {
      mBlockInStream = mBlockStore.getInStream(blockInfo, mOptions, mFailedWorkers);
    }
    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    mBlockInStream.seek(offset);
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      BlockInStream.BlockInStreamSource blockSource = stream.getSource();
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
      if (blockSource == BlockInStream.BlockInStreamSource.NODE_LOCAL
          || blockSource == BlockInStream.BlockInStreamSource.PROCESS_LOCAL) {
        return;
      }
      triggerAsyncCaching(stream);
    }
  }

  // Send an async cache request to a worker based on read type and passive cache options.
  // Note that, this is best effort
  @VisibleForTesting
  boolean triggerAsyncCaching(BlockInStream stream) {
    final long blockId = stream.getId();
    final BlockInfo blockInfo = mStatus.getBlockInfo(blockId);
    if (blockInfo == null) {
      return false;
    }
    try {
      boolean cache = ReadType.fromProto(mOptions.getOptions().getReadType()).isCache();
      boolean overReplicated = mStatus.getReplicationMax() > 0
          && blockInfo.getLocations().size() >= mStatus.getReplicationMax();
      cache = cache && !overReplicated;
      // Get relevant information from the stream.
      WorkerNetAddress dataSource = stream.getAddress();
      if (cache && (mLastBlockIdCached != blockId)) {
        // Construct the async cache request
        long blockLength = mOptions.getBlockInfo(blockId).getLength();
        String host = dataSource.getHost();
        // issues#11172: If the worker is in a container, use the container hostname
        // to establish the connection.
        if (!dataSource.getContainerHost().equals("")) {
          LOG.debug("Worker is in a container. Use container host {} instead of physical host {}",
              dataSource.getContainerHost(), host);
          host = dataSource.getContainerHost();
        }
        CacheRequest request =
            CacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
                .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
                .setSourceHost(host).setSourcePort(dataSource.getDataPort())
                .setAsync(true).build();
        if (mPassiveCachingEnabled && mContext.hasProcessLocalWorker()) {
          mContext.getProcessLocalWorker().cache(request);
          mLastBlockIdCached = blockId;
          return true;
        }
        WorkerNetAddress worker;
        if (mPassiveCachingEnabled && mContext.hasNodeLocalWorker()) {
          // send request to local worker
          worker = mContext.getNodeLocalWorker();
        } else { // send request to data source
          worker = dataSource;
        }
        try (CloseableResource<BlockWorkerClient> blockWorker =
                 mContext.acquireBlockWorkerClient(worker)) {
          blockWorker.get().cache(request);
          mLastBlockIdCached = blockId;
        }
      }
      return true;
    } catch (Exception e) {
      LOG.warn("Failed to complete async cache request (best effort) for block {} of file {}: {}",
          stream.getId(), mStatus.getPath(), e.toString());
      return false;
    }
  }

  private void handleRetryableException(BlockInStream stream, IOException e) {
    WorkerNetAddress workerAddress = stream.getAddress();
    LOG.warn("Failed to read block {} of file {} from worker {}. "
        + "This worker will be skipped for future read operations, will retry: {}.",
        stream.getId(), mStatus.getPath(), workerAddress, e.toString());
    try {
      stream.close();
    } catch (Exception ex) {
      // Do not throw doing a best effort close
      LOG.warn("Failed to close input stream for block {} of file {}: {}",
          stream.getId(), mStatus.getPath(), ex.toString());
    }
    // TODO(lu) consider recovering failed workers
    mFailedWorkers.put(workerAddress, System.currentTimeMillis());
  }
}
