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

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Supports reading from a ufs file directly.
 */
// TODO(lu) make it thread safe
@NotThreadSafe
public class UfsFileInStream extends FileInStream {
  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final InputStream mUfsInStream;
  private final long mLength;
  private long mPosition;

  /**
   * Creates a new {@link UfsFileInStream}.
   *
   * @param stream the embedded input stream
   * @param fileLength the file length
   */
  public UfsFileInStream(InputStream stream, long fileLength) {
    mCloser = Closer.create();
    mUfsInStream = Preconditions.checkNotNull(stream);
    mLength = fileLength;
    mPosition = 0;
    mCloser.register(mUfsInStream);
  }

  @Override
  public int read() throws IOException {
    int res = mUfsInStream.read();
    if (res > 0) {
      mPosition += res;
    }
    return res;
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byte[] byteArray = new byte[byteBuffer.remaining()];
    int totalBytesRead = read(byteArray, off, len);
    if (totalBytesRead == -1) {
      return -1;
    }
    byteBuffer.put(byteArray, off, totalBytesRead);
    mPosition += totalBytesRead;
    return totalBytesRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int res = mUfsInStream.read(b, off, len);
    if (res > 0) {
      mPosition += res;
    }
    return res;
  }

  @Override
  public long skip(long n) throws IOException {
    long res = mUfsInStream.skip(n);
    if (res > 0) {
      mPosition += res;
    }
    return res;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    seek(position);
    return read(buffer, offset, length);
  }

  @Override
  public long getPos() throws IOException {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: %s", pos);
    Preconditions.checkArgument(pos <= mLength,
        "Seek position (%s) exceeds the length of the file (%s)", pos, mLength);
    if (mPosition == pos) {
      return;
    }
    if (mPosition < pos) {
      mPosition += mUfsInStream.skip(pos - mPosition);
    } else {
      mUfsInStream.reset();
      mPosition = mUfsInStream.skip(pos);
    }
    if (mPosition != pos) {
      throw new IOException(String.format("Failed to seek to position %s but at %s",
          pos, mPosition));
    }
  }
}
