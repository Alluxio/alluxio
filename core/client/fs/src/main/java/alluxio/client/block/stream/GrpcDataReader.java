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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
  private final FileSystemContext mContext;
  private final BlockWorkerClient mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final GrpcBlockingStream<ReadRequest, ReadResponse> mStream;

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

    mClient = mContext.acquireBlockWorkerClient(address);
    mStream = new GrpcBlockingStream<>(mClient::readBlock);
    mStream.send(mReadRequest, READ_TIMEOUT_MS);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readChunk() throws IOException {
    Preconditions.checkState(!mClient.isShutdown(),
        "Data reader is closed while reading data chunks.");
    ByteString buf;
    ReadResponse response = mStream.receive(READ_TIMEOUT_MS);
    if (response == null) {
      return null;
    }
    Preconditions.checkState(response.hasChunk(), "response should always contain chunk");
    buf = response.getChunk().getData();
    mPosToRead += buf.size();
    Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    return new DataByteBuffer(buf.asReadOnlyByteBuffer(), buf.size());
  }

  @Override
  public void close() throws IOException {
    if (mClient.isShutdown()) {
      return;
    }
    mStream.close();
    while (mStream.receive(READ_TIMEOUT_MS) != null) {
      // wait until stream is closed from server.
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

