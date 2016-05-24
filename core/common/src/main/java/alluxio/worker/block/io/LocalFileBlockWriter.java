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

import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

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

  /**
   * Constructs a Block writer given the file path of the block.
   *
   * @param path file path of the block
   * @throws IOException if its file can not be open with "rw" mode
   */
  public LocalFileBlockWriter(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public WritableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    return write(mLocalFileChannel.size(), inputBuf.duplicate());
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Writes data to the block from an input {@link ByteBuffer}.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was written
   * @throws IOException if an I/O error occurs
   */
  private long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit() - inputBuf.position();
    MappedByteBuffer outputBuf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    outputBuf.put(inputBuf);
    int bytesWritten = outputBuf.limit();
    BufferUtils.cleanDirectBuffer(outputBuf);
    return bytesWritten;
  }
}
