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
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A gRPC data reader that streams a region gRPC a netty data server.
 *
 * Protocol:
 * 1. The client sends a read request (id, offset, length).
 * 2. Once the server receives the request, it streams chunks to the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads chunks from the stream. Reading pauses if the client buffer is full and
 *    resumes if the buffer is not full. If the client can keep up with network speed, the buffer
 *    should have at most one packet.
 * 4. The client stops reading if it receives an empty packet which signifies the end of the stream.
 * 5. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 6. If the client wants to reuse the channel, the client must read all the packets in the channel
 *    before releasing the channel to the channel pool.
 * 7. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public final class GrpcDataReader implements PacketReader {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataReader.class);

  private final FileSystemContext mContext;
  private final BlockWorkerClient mClient;
  private final ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  private final Iterator<ReadResponse> mIterator;
  private final Context.CancellableContext mCancellableContext;

  /** The next pos to read. */
  private long mPosToRead;
  /**
   * This is true only when an empty packet (EOF or CANCELLED) is received. This is only updated
   * by the client thread (not touched by the netty I/O thread).
   */
  private boolean mDone = false;

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link GrpcDataReader}. If this is used to read a block remotely, it
   * requires the block to be locked beforehand and the lock ID is passed to this class.
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
    mCancellableContext = Context.current().withCancellation();
    Context previousContext = mCancellableContext.attach();
    try {
      mIterator = mClient.readBlock(mReadRequest);
    } finally {
      mCancellableContext.detach(previousContext);
    }
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readPacket() throws IOException {
    Preconditions.checkState(!mClosed, "Data reader is closed while reading packets.");
    ByteString buf;
    try {
      if (!mIterator.hasNext()) {
        mDone = true;
        return null;
      }
    } catch (RuntimeException e) {
      close();
      throw new IOException(e.getMessage(), e.getCause());
    }
    ReadResponse response = mIterator.next();
    Preconditions.checkState(response.hasChunk(), "response should either has chunk or code");
    buf = response.getChunk().getData();
    mPosToRead += buf.size();
    Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    return new DataByteBuffer(buf.asReadOnlyByteBuffer(), buf.size());
  }

  @Override
  public void close() {
    if (mClosed || mDone || mClient.isShutdown()) {
      return;
    }
    mCancellableContext.close();
    mClosed = true;
  }

  /**
   * Factory class to create {@link GrpcDataReader}s.
   */
  public static class Factory implements PacketReader.Factory {
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
    public PacketReader create(long offset, long len) throws IOException {
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

