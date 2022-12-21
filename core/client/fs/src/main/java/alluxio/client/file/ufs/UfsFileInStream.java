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

package alluxio.client.file.ufs;

import alluxio.Constants;
import alluxio.Seekable;
import alluxio.client.file.FileInStream;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.runtime.InternalRuntimeException;

import com.google.common.base.Preconditions;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Supports reading from a ufs file directly.
 */
@NotThreadSafe
public class UfsFileInStream extends FileInStream {
  // TODO(lu) use stream manager to prevent memory over consumption issue
  private static final int BUFFER_SIZE = 2 * Constants.MB;
  private final long mLength;
  private final Function<Long, InputStream> mFileOpener;
  private Optional<InputStream> mUfsInStream = Optional.empty();
  private long mPosition = 0L;

  /**
   * Creates a new {@link UfsFileInStream}.
   *
   * @param fileOpener the file opener to open an ufs in stream with offset
   * @param fileLength the file length
   */
  public UfsFileInStream(Function<Long, InputStream> fileOpener, long fileLength) {
    mFileOpener = Preconditions.checkNotNull(fileOpener);
    mLength = fileLength;
  }

  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    updateStreamIfNeeded();
    int res = mUfsInStream.get().read();
    if (res == -1) {
      return -1;
    }
    mPosition++;
    return res;
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byte[] byteArray = new byte[len];
    int totalBytesRead = read(byteArray, 0, len);
    if (totalBytesRead <= 0) {
      return totalBytesRead;
    }
    byteBuffer.position(off).limit(off + len);
    byteBuffer.put(byteArray, 0, totalBytesRead);
    return totalBytesRead;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    updateStreamIfNeeded();
    int bytesRead = mUfsInStream.get().read(b, off, len);
    if (bytesRead > 0) {
      mPosition += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    long toBeSkipped = Math.min(n, mLength - mPosition);
    if (!mUfsInStream.isPresent()) {
      mPosition += toBeSkipped;
      return toBeSkipped;
    }
    long skipped = mUfsInStream.get().skip(toBeSkipped);
    if (skipped > 0) {
      mPosition += skipped;
    }
    return skipped;
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
    if (!mUfsInStream.isPresent()) {
      mPosition = pos;
      return;
    }
    if (mUfsInStream.get() instanceof Seekable) {
      ((Seekable) mUfsInStream.get()).seek(pos);
    } else if (mPosition < pos) {
      long skipped = 0;
      do {
        skipped = mUfsInStream.get().skip(pos - mPosition);
        if (skipped > 0) {
          mPosition += skipped;
        }
      } while (mPosition < pos && skipped > 0);
      if (mPosition != pos) {
        throw new InternalRuntimeException(String.format(
            "Failed to use skip to seek to pos %s, current position %s", pos, mPosition));
      }
    } else {
      close();
    }
    mPosition = pos;
  }

  @Override
  public void close() throws IOException {
    if (mUfsInStream.isPresent()) {
      mUfsInStream.get().close();
      mUfsInStream = Optional.empty();
    }
  }

  private void updateStreamIfNeeded() {
    if (mUfsInStream.isPresent()) {
      return;
    }
    InputStream ufsInStream = mFileOpener.apply(mPosition);
    if (mLength - mPosition >= BUFFER_SIZE) {
      ufsInStream = new BufferedInputStream(ufsInStream, BUFFER_SIZE);
    }
    mUfsInStream = Optional.of(ufsInStream);
  }
}
