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
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;
import alluxio.resource.LockResource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that responsible for reading from a specific block.
 *
 * The current implementation cached the block data from the beginning to
 * the largest index being read.
 *
 * It follows GrpcDataReader protocol and takes strong assumption:
 * Parallel read to the same file happens on the same time, so that read request is
 * serialized by kernel
 */
@NotThreadSafe
public final class NaiveCachedGrpcDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NaiveCachedGrpcDataReader.class);

  private final WorkerNetAddress mAddress;
  private final CloseableResource<BlockWorkerClient> mClient;
  private final long mDataTimeoutMs;
  private final ReadRequest mReadRequest;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  private final ReadResponseMarshaller mMarshaller;

  private volatile int mBufferCount = 0;
  private final DataBuffer[] mDataBuffers;
  private final ReentrantReadWriteLock mBufferLocks = new ReentrantReadWriteLock();

  /** The next pos to read. */
  private long mPosToRead;

  /**
   * Count the number of threads that are accessing the same block together.
   * When no thread is accessing this block, the cached data will be GCed.
   */
  private final AtomicInteger mRefCount = new AtomicInteger(0);

  /**
   * Creates an instance of {@link NaiveCachedGrpcDataReader}.
   *
   * @param address the data server address
   * @param client the block worker client to read data from
   * @param dataBuffers the data buffers to cache block data in chunk
   * @param dataTimeoutMs the maximum time to wait for a data response
   * @param readRequest the read request
   * @param stream the underlying gRPC stream to read data
   */
  private NaiveCachedGrpcDataReader(WorkerNetAddress address,
      CloseableResource<BlockWorkerClient> client, DataBuffer[] dataBuffers,
      long dataTimeoutMs, ReadRequest readRequest,
      GrpcBlockingStream<ReadRequest, ReadResponse> stream) {
    mAddress = address;
    mClient = client;
    mDataBuffers = dataBuffers;
    mDataTimeoutMs = dataTimeoutMs;
    mMarshaller = new ReadResponseMarshaller();
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;
    mStream = stream;
  }

  /**
   * Reads a specific chunk from the block.
   *
   * @param index the chunk index
   * @return the chunk data if exists
   */
  public DataBuffer readChunk(int index) throws IOException {
    if (index >= mDataBuffers.length) {
      return null;
    }

    if (index >= mBufferCount) {
      try (LockResource r1 = new LockResource(mBufferLocks.writeLock())) {
        while (index >= mBufferCount) {
          DataBuffer buffer = readChunk();
          mDataBuffers[mBufferCount] = buffer;
          ++mBufferCount;
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
  private DataBuffer readChunk() throws IOException {
    Preconditions.checkState(!mClient.get().isShutdown(),
        "Data reader is closed while reading data chunks.");
    DataBuffer buffer = null;
    ReadResponse response = null;
    response = mStream.receive(mDataTimeoutMs);
    if (response != null) {
      Preconditions.checkState(response.hasChunk() && response.getChunk().hasData(),
          "response should always contain chunk");

      ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
      buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
    } else {
      return null;
    }
    mPosToRead += buffer.readableBytes();
    try {
      mStream.send(mReadRequest.toBuilder().setOffsetReceived(mPosToRead).build());
    } catch (Exception e) {
      // nothing is done as the receipt is sent at best effort
      LOG.debug("Failed to send receipt of data to worker {} for request {}: {}.", mAddress,
          mReadRequest, e.getMessage());
    }
    Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    return buffer;
  }

  /**
   * Closes the {@link NaiveCachedGrpcDataReader}.
   */
  public void close() throws IOException {
    try {
      if (mClient.get().isShutdown()) {
        return;
      }
      mStream.close();
      mStream.waitForComplete(mDataTimeoutMs);
    } finally {
      mMarshaller.close();
      mClient.close();
    }
  }

  /**
   * Increases the reference count and return the current count.
   *
   * @return the current count
   */
  public int ref() {
    return mRefCount.incrementAndGet();
  }

  /**
   * Decreases the reference count and return the current count.
   *
   * @return the current count
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
   * Factory class to create {@link NaiveCachedGrpcDataReader}s.
   */
  public static class Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest mReadRequest;

    /**
     * Creates an instance of {@link GrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequest the read request
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest readRequest) {
      mContext = context;
      mAddress = address;
      mReadRequest = readRequest;
    }

    /**
     * @return a new {@link NaiveCachedGrpcDataReader}
     * @throws IOException
     */
    public NaiveCachedGrpcDataReader create() throws IOException {
      AlluxioConfiguration alluxioConf = mContext.getClusterConf();
      int readerBufferSizeMessages = alluxioConf
          .getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
      long dataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_DATA_TIMEOUT);
      CloseableResource<BlockWorkerClient> client = mContext.acquireBlockWorkerClient(mAddress);
      long blockSize = mReadRequest.getLength() + mReadRequest.getOffset();
      long chunkSize = mReadRequest.getChunkSize();
      int buffCount = (int) (blockSize / chunkSize);
      if ((blockSize % chunkSize) != 0) {
        buffCount += 1;
      }
      DataBuffer[] dataBuffers = new DataBuffer[buffCount];

      GrpcBlockingStream<ReadRequest, ReadResponse> stream;
      try {
        String desc = "NaiveCachedGrpcDataReader";
        if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
          desc = MoreObjects.toStringHelper(this)
              .add("request", mReadRequest)
              .add("address", mAddress)
              .toString();
        }
        // Stream here cannot be GrpcDataMessagingBlockingStream
        // DataBuffer.getReadOnlyByteBuffer is used to clone a copy in SharedDataReader.readChunk.
        // getReadOnlyByteBuffer is not implemented in DataBuffer
        // returned from GrpcDataMessagingBlockingStream.
        stream = new GrpcBlockingStream<>(client.get()::readBlock, readerBufferSizeMessages,
            desc);
        stream.send(mReadRequest, dataTimeoutMs);
      } catch (Exception e) {
        client.close();
        throw e;
      }
      return new NaiveCachedGrpcDataReader(mAddress, client,
          dataBuffers, dataTimeoutMs, mReadRequest, stream);
    }
  }
}

