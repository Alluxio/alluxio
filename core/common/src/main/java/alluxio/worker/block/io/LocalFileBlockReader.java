/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.io;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides read access to a block data file locally stored in managed storage.
 */
@NotThreadSafe
public final class LocalFileBlockReader implements BlockReader {
  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();
  private final long mFileSize;

  /**
   * Constructs a Block reader given the file path of the block.
   *
   * @param path file path of the block
   * @throws IOException if its file can not be open with "r" mode
   */
  public LocalFileBlockReader(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "r"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
    mFileSize = mLocalFile.length();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long getLength() {
    return mFileSize;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkArgument(offset + length <= mFileSize,
        "offset=%s, length=%s, exceeding fileSize=%s", offset, length, mFileSize);
    // TODO(calvin): May need to make sure length is an int.
    if (length == -1L) {
      length = mFileSize - offset;
    }
    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
