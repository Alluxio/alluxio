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
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;

import java.util.concurrent.atomic.AtomicInteger;
import alluxio.resource.LockResource;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that streams a region from gRPC data server.
 *
 * Protocol:
 * 1. The client sends a read request (id, offset, length).
 * 2. Once the server receives the request, it streams chunks to the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads chunks from the stream using an iterator.
 * 4. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 5. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public final class NaiveCachedGrpcDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NaiveCachedGrpcDataReader.class);

  private final int mReaderBufferSizeMessages;
  private final long mDataTimeoutMs;
  private final FileSystemContext mContext;
  private final CloseableResource<BlockWorkerClient> mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  private final ReadResponseMarshaller mMarshaller;

  private final DataBuffer[] mDataBuffers; 
  private int mBufferCount = 0;
  private final ReentrantReadWriteLock mBufferLocks = new ReentrantReadWriteLock();

  /** The next pos to read. */
  private long mPosToRead;

  private final AtomicInteger mRefCount = new AtomicInteger(0);

  /**
   * Creates an instance of {@link NaiveCachedGrpcDataReader}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param readRequest the read request
   */
  public NaiveCachedGrpcDataReader(FileSystemContext context, WorkerNetAddress address,
      ReadRequest readRequest) throws IOException {
    mContext = context;
    mAddress = address;
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;
    AlluxioConfiguration alluxioConf = context.getClusterConf();
    mReaderBufferSizeMessages = alluxioConf
        .getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
    mDataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_DATA_TIMEOUT);
    mMarshaller = new ReadResponseMarshaller();
    mClient = mContext.acquireBlockWorkerClient(address);
    long blockSize = readRequest.getLength() + readRequest.getOffset();
    long chunkSize = readRequest.getChunkSize();
    int buffCount = (int)(blockSize / chunkSize);
    if ((blockSize % chunkSize) != 0) {
      buffCount += 1;
    }
    mDataBuffers = new DataBuffer[buffCount];

    try {
      String desc = "GrpcDataReader";
      if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
        desc = MoreObjects.toStringHelper(this)
          .add("request", mReadRequest)
          .add("address", address)
          .toString();
      }
      mStream = new GrpcBlockingStream<>(mClient.get()::readBlock, mReaderBufferSizeMessages,
          desc);
      mStream.send(mReadRequest, mDataTimeoutMs);
    } catch (Exception e) {
      mClient.close();
      throw e;
    }
  }

  public DataBuffer readChunk(int index) throws IOException {
    if (index >= mDataBuffers.length) {
      return null;
    }
    
    try (LockResource r1 = new LockResource(mBufferLocks.writeLock())) {
      while (index >= mBufferCount) {
        DataBuffer buffer = readChunk();
        mDataBuffers[mBufferCount] = buffer;
        ++mBufferCount;
      }
    }

    return mDataBuffers[index];
  }

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

  public int ref() {
    return mRefCount.incrementAndGet();
  }

  public int deRef() {
    return mRefCount.decrementAndGet();
  }

  public int getRefCount() {
    return mRefCount.get();
  }
}

