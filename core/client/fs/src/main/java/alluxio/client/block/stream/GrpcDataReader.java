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
import alluxio.wire.WorkerNetAddress;

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

  private final int mReaderBufferSizeMessages;
  private final long mDataTimeoutMs;
  private final FileSystemContext mContext;
  private final BlockWorkerClient mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;
  private final ReadResponseMarshaller mMarshaller;

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
    AlluxioConfiguration alluxioConf = context.getConf();
    mReaderBufferSizeMessages = alluxioConf
        .getInt(PropertyKey.USER_NETWORK_READER_BUFFER_SIZE_MESSAGES);
    mDataTimeoutMs = alluxioConf.getMs(PropertyKey.USER_NETWORK_DATA_TIMEOUT_MS);
    mMarshaller = new ReadResponseMarshaller();
    mClient = mContext.acquireBlockWorkerClient(address);

    try {
      if (alluxioConf.getBoolean(PropertyKey.USER_NETWORK_ZEROCOPY_ENABLED)) {
        mStream = new GrpcDataMessageBlockingStream<>(mClient::readBlock, mReaderBufferSizeMessages,
            MoreObjects.toStringHelper(this)
                .add("request", mReadRequest)
                .add("address", address)
                .toString(),
            null, mMarshaller);
      } else {
        mStream = new GrpcBlockingStream<>(mClient::readBlock, mReaderBufferSizeMessages,
            MoreObjects.toStringHelper(this)
                .add("request", mReadRequest)
                .add("address", address)
                .toString());
      }
      mStream.send(mReadRequest, mDataTimeoutMs);
    } catch (Exception e) {
      mContext.releaseBlockWorkerClient(address, mClient);
      throw e;
    }
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    Preconditions.checkState(!mClient.isShutdown(),
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
      if (mClient.isShutdown()) {
        return;
      }
      mStream.close();
      mStream.waitForComplete(mDataTimeoutMs);
    } finally {
      mMarshaller.close();
      mContext.releaseBlockWorkerClient(mAddress, mClient);
    }
  }

  /**
   * Factory class to create {@link GrpcDataReader}s.
   */
  public static class Factory implements DataReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final ReadRequest mReadRequestPartial;

    /**
     * Creates an instance of {@link GrpcDataReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestPartial the partial read request
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        ReadRequest readRequestPartial) {
      mContext = context;
      mAddress = address;
      mReadRequestPartial = readRequestPartial;
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      return new GrpcDataReader(mContext, mAddress,
          mReadRequestPartial.toBuilder().setOffset(offset).setLength(len).build());
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}

