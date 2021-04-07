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

import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link OutputStream} implementation that is based on {@link DataWriter} which
 * streams data chunk by chunk.
 */
@NotThreadSafe
public class BlockOutStream extends OutputStream implements BoundedStream, Cancelable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockOutStream.class);

  private final Closer mCloser;
  /** Length of the stream. If unknown, set to Long.MAX_VALUE. */
  private final long mLength;
  private final WorkerNetAddress mAddress;
  private ByteBuf mCurrentChunk = null;

  private final List<DataWriter> mDataWriters;
  private boolean mClosed;

  /**
   * Constructs a new {@link BlockOutStream} with only one {@link DataWriter}.
   *
   * @param dataWriter the data writer
   * @param length the length of the stream
   * @param address the Alluxio worker address
   */
  public BlockOutStream(DataWriter dataWriter, long length, WorkerNetAddress address) {
    mCloser = Closer.create();
    mLength = length;
    mAddress = address;
    mDataWriters = new ArrayList<>(1);
    mDataWriters.add(dataWriter);
    mCloser.register(dataWriter);
    mClosed = false;
  }

  /**
   * @return the remaining size of the block
   */
  @Override
  public long remaining() {
    long pos = Long.MAX_VALUE;
    for (DataWriter dataWriter : mDataWriters) {
      pos = Math.min(pos, dataWriter.pos());
    }
    return mLength - pos - (mCurrentChunk != null ? mCurrentChunk.readableBytes() : 0);
  }

  /**
   * Creates a new remote block output stream.
   *
   * @param context the file system context
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddresses the worker network addresses
   * @param options the options
   * @return the {@link BlockOutStream} instance created
   */
  public static BlockOutStream createReplicatedBlockOutStream(FileSystemContext context,
      long blockId, long blockSize, java.util.List<WorkerNetAddress> workerNetAddresses,
      OutStreamOptions options) throws IOException {
    List<DataWriter> dataWriters = new ArrayList<>();
    for (WorkerNetAddress address: workerNetAddresses) {
      DataWriter dataWriter =
            DataWriter.Factory.create(context, blockId, blockSize, address, options);
      dataWriters.add(dataWriter);
    }
    return new BlockOutStream(dataWriters, blockSize, workerNetAddresses);
  }

  /**
   * Constructs a new {@link BlockOutStream} with only one {@link DataWriter}.
   *
   * @param dataWriters the data writer
   * @param length the length of the stream
   * @param workerNetAddresses the worker network addresses
   */
  protected BlockOutStream(List<DataWriter> dataWriters, long length,
      java.util.List<WorkerNetAddress> workerNetAddresses) {
    mCloser = Closer.create();
    mLength = length;
    mAddress = workerNetAddresses.get(0);
    mDataWriters = dataWriters;
    for (DataWriter dataWriter : dataWriters) {
      mCloser.register(dataWriter);
    }
    mClosed = false;
  }

  @Override
  public void write(int b) throws IOException {
    Preconditions.checkState(remaining() > 0, PreconditionMessage.ERR_END_OF_BLOCK);
    updateCurrentChunk(false);
    mCurrentChunk.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }

    while (len > 0) {
      updateCurrentChunk(false);
      int toWrite = Math.min(len, mCurrentChunk.writableBytes());
      mCurrentChunk.writeBytes(b, off, toWrite);
      off += toWrite;
      len -= toWrite;
    }
    updateCurrentChunk(false);
  }

  /**
   * Writes the data in the specified byte buf to this output stream.
   *
   * @param buf the buffer
   * @throws IOException
   */
  public void write(io.netty.buffer.ByteBuf buf) throws IOException {
    write(buf, 0, buf.readableBytes());
  }

  /**
   * Writes len bytes from the specified byte buf starting at offset off to this output stream.
   *
   * @param buf the buffer
   * @param off the offset
   * @param len the length
   */
  public void write(io.netty.buffer.ByteBuf buf, int off, int len) throws IOException {
    if (len == 0) {
      return;
    }

    while (len > 0) {
      updateCurrentChunk(false);
      int toWrite = Math.min(len, mCurrentChunk.writableBytes());
      mCurrentChunk.writeBytes(buf, off, toWrite);
      off += toWrite;
      len -= toWrite;
    }
    updateCurrentChunk(false);
  }

  @Override
  public void flush() throws IOException {
    if (mClosed) {
      return;
    }
    updateCurrentChunk(true);
    for (DataWriter dataWriter : mDataWriters) {
      dataWriter.flush();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    releaseCurrentChunk();

    List<Exception> exceptions = new LinkedList<>();
    for (DataWriter dataWriter : mDataWriters) {
      try {
        dataWriter.cancel();
      } catch (IOException e) {
        exceptions.add(e);
      }
    }
    if (exceptions.size() > 0) {
      IOException ex = new IOException("Failed to cancel all block write attempts");
      exceptions.forEach(ex::addSuppressed);
      throw ex;
    }

    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      updateCurrentChunk(true);
    } catch (Throwable t) {
      throw mCloser.rethrow(t);
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  /**
   * @return the worker address for this stream
   */
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * Updates the current chunk.
   *
   * @param lastChunk if the current packet is the last packet
   */
  private void updateCurrentChunk(boolean lastChunk) throws IOException {
    // Early return for the most common case.
    if (mCurrentChunk != null && mCurrentChunk.writableBytes() > 0 && !lastChunk) {
      return;
    }

    if (mCurrentChunk == null) {
      if (!lastChunk) {
        mCurrentChunk = allocateBuffer();
      }
      return;
    }

    if (mCurrentChunk.writableBytes() == 0 || lastChunk) {
      try {
        if (mCurrentChunk.readableBytes() > 0) {
          for (DataWriter dataWriter : mDataWriters) {
            mCurrentChunk.retain();
            dataWriter.writeChunk(mCurrentChunk.duplicate());
          }
        } else {
          Preconditions.checkState(lastChunk);
        }
      } finally {
        // If the packet has bytes to read, we increment its refcount explicitly for every packet
        // writer. So we need to release here. If the packet has no bytes to read, then it has
        // to be the last packet. It needs to be released as well.
        mCurrentChunk.release();
        mCurrentChunk = null;
      }
    }
    if (!lastChunk) {
      mCurrentChunk = allocateBuffer();
    }
  }

  /**
   * Releases the current packet.
   */
  private void releaseCurrentChunk() {
    if (mCurrentChunk != null) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuf allocateBuffer() {
    return PooledByteBufAllocator.DEFAULT.buffer(mDataWriters.get(0).chunkSize());
  }
}
