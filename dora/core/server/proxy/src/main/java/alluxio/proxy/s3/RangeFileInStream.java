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

package alluxio.proxy.s3;

import alluxio.client.file.FileInStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class is use {@link FileInStream} underlying, and implement range read.
 */
public class RangeFileInStream extends InputStream {

  private FileInStream mUnderlyingStream;
  private long mUnderlyingLength;
  private long mReadBytes;

  private RangeFileInStream(FileInStream underlyingStream) {
    mUnderlyingStream = underlyingStream;
    mReadBytes = 0;
  }

  @Override
  public int read() throws IOException {
    if (mReadBytes >= mUnderlyingLength) {
      return -1;
    }

    int b = mUnderlyingStream.read();
    if (b != -1) {
      mReadBytes++;
    }
    return b;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (mReadBytes >= mUnderlyingLength) {
      return -1;
    }

    if (mReadBytes + len > mUnderlyingLength) {
      len = (int) (mUnderlyingLength - mReadBytes);
    }

    int n = mUnderlyingStream.read(b, off, len);
    if (n != -1) {
      mReadBytes += n;
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    mUnderlyingStream.close();
  }

  private void seek(long underlyingLength, S3RangeSpec range) throws IOException {
    mUnderlyingStream.seek(range.getOffset(underlyingLength));
    mUnderlyingLength = range.getLength(underlyingLength);
  }

  /**
   * Factory for {@link RangeFileInStream}.
   */
  public static final class Factory {

    /**
     * @param underlyingStream underlying stream
     * @param underlyingLength underlying steam length
     * @param range            range read
     * @return the stream for range read
     * @throws IOException
     */
    public static RangeFileInStream create(FileInStream underlyingStream, long underlyingLength,
                                           S3RangeSpec range) throws IOException {
      RangeFileInStream ris = new RangeFileInStream(underlyingStream);
      ris.seek(underlyingLength, range);
      return ris;
    }
  }
}
