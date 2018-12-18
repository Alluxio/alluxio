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
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.google.common.base.Preconditions;
import io.grpc.Context;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFilePacketReader implements PacketReader {
  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;
  private final long mEnd;
  private final long mPacketSize;
  private long mPos;
  private boolean mClosed;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param reader the file reader to the block path
   * @param offset the offset
   * @param len the length to read
   * @param packetSize the packet size
   */
  private LocalFilePacketReader(LocalFileBlockReader reader, long offset, long len, long packetSize)
      throws IOException {
    mReader = reader;
    Preconditions.checkArgument(packetSize > 0);
    mPos = offset;
    mEnd = Math.min(mReader.getLength(), offset + len);
    mPacketSize = packetSize;
  }

  @Override
  public DataBuffer readPacket() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer = mReader.read(mPos, Math.min(mPacketSize, mEnd - mPos));
    DataBuffer dataBuffer = new DataByteBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    MetricsSystem.counter(ClientMetrics.BYTES_READ_LOCAL).inc(dataBuffer.getLength());
    MetricsSystem.meter(ClientMetrics.BYTES_READ_LOCAL_THROUGHPUT).mark(dataBuffer.getLength());
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.decreaseUsageCount();
  }

  /**
   * Factory class to create {@link LocalFilePacketReader}s.
   */
  @NotThreadSafe
  public static class Factory implements PacketReader.Factory {
    private final BlockWorkerClient mBlockWorker;
    private final long mBlockId;
    private final String mPath;
    private final long mPacketSize;
    private final Context.CancellableContext mCancellableContext;
    private LocalFileBlockReader mReader;
    private boolean mClosed;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context the file system context
     * @param address the worker address
     * @param blockId the block ID
     * @param packetSize the packet size
     * @param options the instream options
     */
    public Factory(FileSystemContext context, WorkerNetAddress address, long blockId,
        long packetSize, InStreamOptions options) throws IOException {
      mBlockId = blockId;
      mPacketSize = packetSize;

      mBlockWorker = context.acquireBlockWorkerClient(address);
      boolean isPromote = ReadType.fromProto(options.getOptions().getReadType()).isPromote();
      OpenLocalBlockRequest request = OpenLocalBlockRequest.newBuilder()
          .setBlockId(mBlockId).setPromote(isPromote).build();
      mCancellableContext = Context.current().withCancellation();
      Context previousContext = mCancellableContext.attach();
      try {
        OpenLocalBlockResponse response = mBlockWorker.openLocalBlock(request);
        mPath = response.getPath();
      } catch (Exception e) {
        mCancellableContext.cancel(e);
        throw e;
      } finally {
        mCancellableContext.detach(previousContext);
      }
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      if (mReader == null) {
        mReader = new LocalFileBlockReader(mPath);
      }
      Preconditions.checkState(mReader.getUsageCount() == 0);
      mReader.increaseUsageCount();
      return new LocalFilePacketReader(mReader, offset, len, mPacketSize);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      if (mReader != null) {
        mReader.close();
      }
      try {
        mCancellableContext.close();
      } finally {
        mBlockWorker.close();
        mClosed = true;
      }
    }
  }
}

