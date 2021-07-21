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

import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DataMessage;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.ReadResponseMarshaller;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.util.logging.SamplingLogger;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Timer;
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
public final class GrpcDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataReader.class);
  private static final Logger SLOW_CLOSE_LOG = new SamplingLogger(LOG, Constants.MINUTE_MS);

  private final int mReaderBufferSizeMessages;
  private final long mDataTimeoutMs;
  private final boolean mDetailedMetricsEnabled;
  private final FileSystemContext mContext;
  private final CloseableResource<BlockWorkerClient> mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  private final ReadResponseMarshaller mMarshaller;
  private final long mCloseWaitMs;

  /** The next pos to read. */
  private long mPosToRead;

  /**
   * Creates an instance of {@link GrpcDataReader}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param readRequest the read request
   */
  private GrpcDataReader(FileSystemContext context, WorkerNetAddress address,
      ReadRequest readRequest) throws IOException {
    mContext = context;
    mAddress = address;
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;
    AlluxioConfiguration alluxioConf = context.getClusterConf();
    mReaderBufferSizeMessages = alluxioConf
        .getInt(PropertyKey.USER_STREAMING_READER_BUFFER_SIZE_MESSAGES);
    mDataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_DATA_READ_TIMEOUT);
    mDetailedMetricsEnabled = alluxioConf.getBoolean(PropertyKey.USER_BLOCK_READ_METRICS_ENABLED);
    mMarshaller = new ReadResponseMarshaller();
    mClient = mContext.acquireBlockWorkerClient(address);
    mCloseWaitMs = alluxioConf.getMs(PropertyKey.USER_STREAMING_READER_CLOSE_TIMEOUT);

    try {
      if (alluxioConf.getBoolean(PropertyKey.USER_STREAMING_ZEROCOPY_ENABLED)) {
        String desc = "Zero Copy GrpcDataReader";
        if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
          desc = MoreObjects.toStringHelper(this)
              .add("request", mReadRequest)
              .add("address", address)
              .toString();
        }
        mStream = new GrpcDataMessageBlockingStream<>(mClient.get()::readBlock,
            mReaderBufferSizeMessages,
            desc, null, mMarshaller);
      } else {
        String desc = "GrpcDataReader";
        if (LOG.isDebugEnabled()) { // More detailed description when debug logging is enabled
          desc = MoreObjects.toStringHelper(this)
              .add("request", mReadRequest)
              .add("address", address)
              .toString();
        }
        mStream = new GrpcBlockingStream<>(mClient.get()::readBlock, mReaderBufferSizeMessages,
            desc);
      }
      mStream.send(mReadRequest, mDataTimeoutMs);
    } catch (Exception e) {
      mClient.close();
      throw e;
    }
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    if (mDetailedMetricsEnabled) {
      try (Timer.Context ctx = MetricsSystem
          .timer(MetricKey.CLIENT_BLOCK_READ_CHUNK_REMOTE.getName()).time()) {
        return readChunkInternal();
      }
    }
    return readChunkInternal();
  }

  private DataBuffer readChunkInternal() throws IOException {
    Preconditions.checkState(!mClient.get().isShutdown(),
        "Data reader is closed while reading data chunks.");
    DataBuffer buffer = null;
    ReadResponse response = null;
    if (mStream instanceof GrpcDataMessageBlockingStream) {
      DataMessage<ReadResponse, DataBuffer> message =
          ((GrpcDataMessageBlockingStream<ReadRequest, ReadResponse>) mStream)
              .receiveDataMessage(mDataTimeoutMs);
      if (message != null) {
        response = message.getMessage();
        buffer = message.getBuffer();
        if (buffer == null && response.hasChunk() && response.getChunk().hasData()) {
          // falls back to use chunk message for compatibility
          ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
          buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
        }
        Preconditions.checkState(buffer != null, "response should always contain chunk");
      }
    } else {
      response = mStream.receive(mDataTimeoutMs);
      if (response != null) {
        Preconditions.checkState(response.hasChunk() && response.getChunk().hasData(),
            "response should always contain chunk");
        ByteBuffer byteBuffer = response.getChunk().getData().asReadOnlyByteBuffer();
        buffer = new NioDataBuffer(byteBuffer, byteBuffer.remaining());
      }
    }
    if (response == null) {
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

  @Override
  public void close() throws IOException {
    try {
      if (mClient.get().isShutdown()) {
        return;
      }
      mStream.close();

      // When a reader is closed, there is technically nothing the client requires from the server.
      // However, the server does need to cleanup resources for a client close(), including closing
      // or canceling any temp blocks. Therefore, we should wait for some amount of time for the
      // server to finish cleanup, but it should not be very long (since the client is finished
      // with the read). Also, if there is any error when waiting for the complete, it should be
      // ignored since again, the client is completely finished with the read.
      try {
        // Wait a short time for the server to finish the close, and then let the client continue.
        if (mCloseWaitMs > 0) {
          mStream.waitForComplete(mCloseWaitMs);
        }
      } catch (Throwable e) {
        // ignore any errors
        SLOW_CLOSE_LOG.warn(
            "Closing gRPC read stream took longer than {}ms, moving on. blockId: {}, address: {}",
            mCloseWaitMs, mReadRequest.getBlockId(), mAddress);
      }
    } finally {
      mMarshaller.close();
      mClient.close();
    }
  }

  /**
   * Factory class to create {@link GrpcDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest.Builder mReadRequestBuilder;

    /**
     * Creates an instance of {@link GrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestBuilder the builder of read request
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest.Builder readRequestBuilder) {
      mContext = context;
      mAddress = address;
      mReadRequestBuilder = readRequestBuilder;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      return new GrpcDataReader(mContext, mAddress,
          mReadRequestBuilder.setOffset(offset).setLength(len).build());
    }

    @Override
    public void close() throws IOException {}
  }
}

