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

import alluxio.Configuration;
import alluxio.MetaCache;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.CountingRetry;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
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
public class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);
  private static final int MAX_WORKERS_TO_RETRY =
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_READ_RETRY);

  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

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

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();

    mPosition = 0;
    mBlockInStream = null;
  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    do {
      updateStream();
      try {
        int result = mBlockInStream.read();
        if (result != -1) {
          mPosition++;
        }
        return result;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        handleRetryableException(mBlockInStream, e);
        mBlockInStream = null;
      }
    } while (retry.attemptRetry());
    throw lastException;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }
    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    while (bytesLeft > 0 && mPosition != mLength) {
      updateStream();
      try {
        int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
        if (bytesRead > 0) {
          bytesLeft -= bytesRead;
          currentOffset += bytesRead;
          mPosition += bytesRead;
        }
        retry.reset();
      } catch (UnavailableException | ConnectException | DeadlineExceededException e) {
        if (!retry.attemptRetry()) {
          throw e;
        }
        handleRetryableException(mBlockInStream, e);
        mBlockInStream = null;
      }
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

    int lenCopy = len;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    while (len > 0) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      BlockInStream stream = null;
      stream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
      try {
        long offset = pos % mBlockSize;
        int bytesRead =
            stream.positionedRead(offset, b, off, (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
        retry.reset();
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        //qiniu2 - block may be evicted
        LOG.info("==== file " + mStatus.getPath() + " block " + blockId + " may be evicted, clear local cache");
        MetaCache.invalidateBlockInfoCache(blockId);

        if (!retry.attemptRetry()) {
          throw e;
        }
        handleRetryableException(stream, e);
        stream = null;
      } finally {
        closeBlockInStream(stream);
      }
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
    // Create stream
    mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    mBlockInStream.seek(offset);
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      // Get relevant information from the stream.
      WorkerNetAddress dataSource = stream.getAddress();
      long blockId = stream.getId();
      BlockInStream.BlockInStreamSource blockSource = stream.getSource();
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
      if (blockSource == BlockInStream.BlockInStreamSource.LOCAL) {
        return;
      }

      // Send an async cache request to a worker based on read type and passive cache options.
      boolean cache = mOptions.getOptions().getReadType().isCache();
      boolean passiveCache = Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
      long channelTimeout = Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
      if (cache) {
        WorkerNetAddress worker;
        if (passiveCache && mContext.hasLocalWorker()) { // send request to local worker
          worker = mContext.getLocalWorker();
        } else { // send request to data source
          worker = dataSource;
        }
        try {
          // Construct the async cache request
          long blockLength = mOptions.getBlockInfo(blockId).getLength();
          Protocol.AsyncCacheRequest request =
              Protocol.AsyncCacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
                  .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
                  .setSourceHost(dataSource.getHost()).setSourcePort(dataSource.getDataPort())
                  .build();
          Channel channel = mContext.acquireNettyChannel(worker);
          try {
            NettyRPCContext rpcContext =
                NettyRPCContext.defaults().setChannel(channel).setTimeout(channelTimeout);
            NettyRPC.fireAndForget(rpcContext, new ProtoMessage(request));
          } finally {
            mContext.releaseNettyChannel(worker, channel);
          }
        } catch (Exception e) {
          LOG.warn("Failed to complete async cache request for block {} at worker {}: {}", blockId,
              worker, e.getMessage());
        }
      }
    }
  }

  private void handleRetryableException(BlockInStream stream, IOException e) {
    WorkerNetAddress workerAddress = stream.getAddress();
    LOG.warn("Failed to read block {} from worker {}, will retry: {}",
        stream.getId(), workerAddress, e.getMessage());
    try {
      stream.close();
    } catch (Exception ex) {
      // Do not throw doing a best effort close
      LOG.warn("Failed to close input stream for block {}: {}", stream.getId(), ex.getMessage());
    }

    mFailedWorkers.put(workerAddress, System.currentTimeMillis());
  }
}
