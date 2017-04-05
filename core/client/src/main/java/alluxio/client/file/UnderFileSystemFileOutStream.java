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
import alluxio.PropertyKey;
import alluxio.client.UnderFileSystemFileWriter;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a streaming API to write to a file in the under file system through an Alluxio
 * worker's data server. This class is based off of
 * {@link alluxio.client.block.BufferedBlockOutStream}.
 */
// TODO(calvin): See if common logic in this class and buffered block out stream can be abstracted
@NotThreadSafe
public final class UnderFileSystemFileOutStream extends OutputStream {
  private final FileSystemContext mContext;
  /** Java heap buffer to buffer writes before flushing them to the worker. */
  private final ByteBuffer mBuffer;
  /** Writer to the worker, currently only implemented through Netty. */
  private final UnderFileSystemFileWriter mWriter;
  /** Address of the worker to write to. */
  private final InetSocketAddress mAddress;
  /** Worker file id referencing the file to write to. */
  private final long mUfsFileId;
  /** Used to manage closeable resources. */
  private final Closer mCloser;
  /** If the stream is closed, this can only go from false to true. */
  private boolean mClosed;
  /** Number of bytes flushed to the worker. */
  private long mFlushedBytes;
  /** Number of bytes written, including unflushed bytes. */
  private long mWrittenBytes;

  /**
   * Factory for creating an {@link UnderFileSystemFileOutStream}.
   */
  public static class Factory {
    private static Factory sInstance;

    /**
     * @return an instance of a factory for creating {@link UnderFileSystemFileOutStream}
     */
    public static synchronized Factory get() {
      if (sInstance == null) {
        sInstance = new Factory();
      }
      return sInstance;
    }

    protected Factory() {} // prevent external instantiation.

    /**
     * @param context the file system context
     * @param address the address of an Alluxio worker
     * @param ufsFileId the file ID of the ufs file to write to
     * @return a new {@link UnderFileSystemFileOutStream}
     * @throws IOException if it fails to create the out stream
     */
    public OutputStream create(FileSystemContext context, InetSocketAddress address, long ufsFileId)
        throws IOException {
      return new alluxio.client.block.stream.UnderFileSystemFileOutStream(context, address,
          ufsFileId);
    }
  }

  /**
   * Constructor for a under file system file output stream.
   *
   * @param context the file system context
   * @param address address of the worker
   * @param ufsFileId the worker specific file id
   */
  private UnderFileSystemFileOutStream(FileSystemContext context, InetSocketAddress address,
      long ufsFileId) {
    mContext = context;
    mBuffer = allocateBuffer();
    mAddress = address;
    mUfsFileId = ufsFileId;
    mFlushedBytes = 0;
    mWrittenBytes = 0;
    mClosed = false;
    mCloser = Closer.create();
    mWriter = mCloser.register(UnderFileSystemFileWriter.Factory.create(mContext));
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      if (mFlushedBytes < mWrittenBytes) {
        flush();
      }
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  @Override
  public void flush() throws IOException {
    if (mBuffer.position() == 0) {
      return;
    }
    writeToWorker(mBuffer.array(), 0, mBuffer.position());
    mBuffer.clear();
  }

  @Override
  public void write(int b) throws IOException {
    checkIfClosed();
    if (mBuffer.position() >= mBuffer.limit()) {
      flush();
    }
    BufferUtils.putIntByteBuffer(mBuffer, b);
    mWrittenBytes++;
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

    // Flush the non-empty buffer if the new write will overflow it.
    if (mBuffer.position() > 0 && mBuffer.position() + len > mBuffer.limit()) {
      flush();
    }

    // If this write is larger than half of buffer limit, then write it out directly
    // to the remote block. Before committing the new writes, need to make sure
    // all bytes in the buffer are written out first, to prevent out-of-order writes.
    // Otherwise, when the write is small, write the data to the buffer.
    if (len > mBuffer.limit() / 2) {
      if (mBuffer.position() > 0) {
        flush();
      }
      unBufferedWrite(b, off, len);
    } else {
      mBuffer.put(b, off, len);
    }

    mWrittenBytes += len;
  }

  /**
   * Convenience method for checking the state of the stream.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed,
        PreconditionMessage.ERR_CLOSED_UNDER_FILE_SYSTEM_FILE_OUT_STREAM);
  }

  /**
   * Writes the data in the byte array directly to the worker. This should only be used for writes
   * which would not be able to be buffered.
   *
   * @param b the data that should be written
   * @param off the offset into the data to start writing from
   * @param len the length to write
   * @throws IOException if the write does not succeed
   */
  private void unBufferedWrite(byte[] b, int off, int len) throws IOException {
    writeToWorker(b, off, len);
  }

  /**
   * Write data to the worker. This will increment flushed bytes.
   *
   * @param b data to write
   * @param off start offset of the data
   * @param len length to write
   * @throws IOException if an error occurs during when writing to the worker
   */
  private void writeToWorker(byte[] b, int off, int len) throws IOException {
    mWriter.write(mAddress, mUfsFileId, mFlushedBytes, b, off, len);
    mFlushedBytes += len;
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuffer allocateBuffer() {
    return ByteBuffer.allocate(
        (int) Configuration.getBytes(PropertyKey.USER_UFS_DELEGATION_WRITE_BUFFER_SIZE_BYTES));
  }
}
