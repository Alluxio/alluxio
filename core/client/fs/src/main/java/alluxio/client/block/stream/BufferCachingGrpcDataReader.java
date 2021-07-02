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

package alluxio.client.block.stream;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that responsible for reading from a specific block.
 *
 * The current implementation cached the block data from the beginning to
 * the largest index being read.
 */
@NotThreadSafe
public class BufferCachingGrpcDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(BufferCachingGrpcDataReader.class);

  private final WorkerNetAddress mAddress;
  private final CloseableResource<BlockWorkerClient> mClient;
  private final long mDataTimeoutMs;
  private final ReadRequest mReadRequest;
  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  /**
   * Count the number of threads that are accessing the same block together.
   * When no thread is accessing this block, the cached data will be GCed.
   */
  private final AtomicInteger mRefCount = new AtomicInteger(0);
  private final AtomicInteger mBufferCount = new AtomicInteger(0);
  @GuardedBy("mBufferLocks")
  private final DataBuffer[] mDataBuffers;
  private final ReentrantReadWriteLock mBufferLocks = new ReentrantReadWriteLock();

  /** The next pos to read. */
  @VisibleForTesting
  protected long mPosToRead;

  /**
   * Creates an instance of {@link BufferCachingGrpcDataReader}.
   *
   * @param address the data server address
   * @param client the block worker client to read data from
   * @param dataTimeoutMs the maximum time to wait for a data response
   * @param readRequest the read request
   * @param stream the underlying gRPC stream to read data
   */
  @VisibleForTesting
  protected BufferCachingGrpcDataReader(WorkerNetAddress address,
      CloseableResource<BlockWorkerClient> client, long dataTimeoutMs,
      ReadRequest readRequest, GrpcBlockingStream<ReadRequest, ReadResponse> stream) {
    mAddress = address;
    mClient = client;
    mDataTimeoutMs = dataTimeoutMs;
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;
    mStream = stream;
    long blockSize = mReadRequest.getLength() + mReadRequest.getOffset();
    long chunkSize = mReadRequest.getChunkSize();
    int buffCount = (int) (blockSize / chunkSize);
    if ((blockSize % chunkSize) != 0) {
      buffCount += 1;
    }
    mDataBuffers = new DataBuffer[buffCount];
  }

  /**
   * Reads a specific chunk from the block.
   *
   * @param index the chunk index
   * @return the chunk data if exists
   */
  @Nullable
  public DataBuffer readChunk(int index) throws IOException {
    if (index >= mDataBuffers.length) {
      return null;
    }

    if (index >= mBufferCount.get()) {
      try (LockResource r1 = new LockResource(mBufferLocks.writeLock())) {
        while (index >= mBufferCount.get()) {
          DataBuffer buffer = readChunk();
          mDataBuffers[mBufferCount.get()] = buffer;
          mBufferCount.incrementAndGet();
        }
      }
    }

    return mDataBuffers[index];
  }

  /**
   * Reads a chunk of data.
   *
   * @return a chunk of data
   */
  @Nullable
  @VisibleForTesting
  protected DataBuffer readChunk() throws IOException {
    Preconditions.checkState(!mClient.get().isShutdown(),
        "Data reader is closed while reading data chunks.");
    DataBuffer buffer = null;
    ReadResponse response = null;
    response = mStream.receive(mDataTimeoutMs);
    if (response == null) {
      return null;
    }
    Preconditions.checkState(response.hasChunk() && response.getChunk().hasData(),
        "response should always contain chunk");

    ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
    buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
    mPosToRead += buffer.readableBytes();
    try {
      mStream.send(mReadRequest.toBuilder().setOffsetReceived(mPosToRead).build());
    } catch (Exception e) {
      // nothing is done as the receipt is sent at best effort
      LOG.debug("Failed to send receipt of data to worker {} for request {}", mAddress,
          mReadRequest, e);
    }
    Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    return buffer;
  }

  /**
   * Closes the {@link BufferCachingGrpcDataReader}.
   */
  public void close() throws IOException {
    try {
      if (mClient.get().isShutdown()) {
        return;
      }
      mStream.close();
      mStream.waitForComplete(mDataTimeoutMs);
    } finally {
      mClient.close();
    }
  }

  /**
   * Increases the reference count and return the current count.
   *
   * @return the incremented count
   */
  public int ref() {
    return mRefCount.incrementAndGet();
  }

  /**
   * Decreases the reference count and return the current count.
   *
   * @return the decremented count
   */
  public int deRef() {
    return mRefCount.decrementAndGet();
  }

  /**
   * @return the current count
   */
  public int getRefCount() {
    return mRefCount.get();
  }

  /**
   * Creates an instance of {@link BufferCachingGrpcDataReader} for block reads.
   *
   * @param context the file system context
   * @param address the worker address
   * @param readRequest the read request
   * @return a new {@link BufferCachingGrpcDataReader}
   */
  public static BufferCachingGrpcDataReader create(FileSystemContext context,
      WorkerNetAddress address, ReadRequest readRequest) throws IOException {
    AlluxioConfiguration alluxioConf = context.getClusterConf();
    int readerBufferSizeMessages = alluxioConf
        .getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
    long dataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_DATA_READ_TIMEOUT);

    CloseableResource<BlockWorkerClient> client = context.acquireBlockWorkerClient(address);

    String desc = "BufferCachingGrpcDataReader";
    if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
      desc = String.format("BufferCachingGrpcDataReader(request=%s,address=%s)",
          readRequest, address);
    }
    GrpcBlockingStream<ReadRequest, ReadResponse> stream = null;
    try {
      // Stream here cannot be GrpcDataMessagingBlockingStream
      // DataBuffer.getReadOnlyByteBuffer is used to clone a copy in SharedDataReader.readChunk.
      // getReadOnlyByteBuffer is not implemented in DataBuffer
      // returned from GrpcDataMessagingBlockingStream.
      stream = new GrpcBlockingStream<>(client.get()::readBlock, readerBufferSizeMessages,
          desc);
      stream.send(readRequest, dataTimeoutMs);
    } catch (Exception e) {
      if (stream != null) {
        stream.close();
      }
      client.close();
      throw e;
    }
    return new BufferCachingGrpcDataReader(address, client, dataTimeoutMs, readRequest, stream);
  }
}

