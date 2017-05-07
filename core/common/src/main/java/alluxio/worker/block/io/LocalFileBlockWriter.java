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

package alluxio.worker.block.io;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides write access to a temp block data file locally stored in managed storage.
 */
@NotThreadSafe
public final class LocalFileBlockWriter implements BlockWriter {
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();
  private long mPosition;
  private boolean mClosed;

  /**
   * Constructs a Block writer given the file path of the block.
   *
   * @param path file path of the block
   */
  public LocalFileBlockWriter(String path) {
    mFilePath = Preconditions.checkNotNull(path);
    try {
      mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    }
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public GatheringByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long append(ByteBuffer inputBuf) {
    long bytesWritten;
    try {
      bytesWritten = write(mLocalFileChannel.size(), inputBuf.duplicate());
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    }
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public void transferFrom(ByteBuf buf) {
    try {
      mPosition += buf.readBytes(mLocalFileChannel, buf.readableBytes());
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    }
  }

  @Override
  public long getPosition() {
    return mPosition;
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;

    CommonUtils.close(mCloser);
    mPosition = -1;
  }

  /**
   * Writes data to the block from an input {@link ByteBuffer}.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was written
   */
  private long write(long offset, ByteBuffer inputBuf) {
    int inputBufLength = inputBuf.limit() - inputBuf.position();
    MappedByteBuffer outputBuf;
    try {
      outputBuf = mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    }
    outputBuf.put(inputBuf);
    int bytesWritten = outputBuf.limit();
    BufferUtils.cleanDirectBuffer(outputBuf);
    return bytesWritten;
  }
}
