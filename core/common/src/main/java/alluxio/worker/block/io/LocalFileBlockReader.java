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

import alluxio.metrics.MetricsSystem;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides read access to a block data file locally stored in managed storage.
 */
@NotThreadSafe
public class LocalFileBlockReader extends BlockReader {
  private final String mFilePath;
  private final FileInputStream mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();
  private final long mFileSize;
  private boolean mClosed;
  private int mUsageCount = 0;

  /**
   * Constructs a Block reader given the file path of the block.
   *
   * @param path file path of the block
   */
  public LocalFileBlockReader(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path, "path");
    mLocalFile = mCloser.register(new FileInputStream(mFilePath));

    // The file channel is used to get file size
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
    mFileSize = mLocalFileChannel.size();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long getLength() {
    return mFileSize;
  }

  /**
   * increase the file reader usage count.
   */
  public void increaseUsageCount() {
    mUsageCount++;
  }

  /**
   * decrease the file reader usage count.
   */
  public void decreaseUsageCount() {
    Preconditions.checkState(mUsageCount > 0);
    mUsageCount--;
  }

  /**
   * @return the file reader usage count
   */
  public int getUsageCount() {
    return mUsageCount;
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkArgument(offset + length <= mFileSize,
        "offset=%s, length=%s, exceeding fileSize=%s", offset, length, mFileSize);
    MetricsSystem.counter("Client.LocalReaderOps").inc();
    MetricsSystem.counter("Client.LocalReaderRequestBytes").inc(length);
    int sz = (int) length;
    try (Timer.Context timer = MetricsSystem.timer("Client.LocalReaderTimer").time()) {
      byte[] tmpbuf = new byte[sz];
      try (Timer.Context timer2 = MetricsSystem.timer("Client.LocalReaderTimer2").time()) {
        mLocalFile.skip(offset);
        int rd = 0;
        int nread = 0;
        while (rd >= 0 && nread < sz) {
          rd = mLocalFile.read(tmpbuf, nread, sz - nread);
          if (rd >= 0) {
            nread += rd;
          }
        }
        ByteBuffer buffer = ByteBuffer.wrap(tmpbuf, 0, nread);
        return buffer;
      }
    }
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return buf.writeBytes(mLocalFileChannel, buf.writableBytes());
  }

  @Override
  public void close() throws IOException {
    super.close();
    Preconditions.checkState(mUsageCount == 0);
    if (mClosed) {
      return;
    }
    try {
      mCloser.close();
    } finally {
      mClosed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public String getLocation() {
    return mFilePath;
  }
}
