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

package alluxio.client.file.dora;

import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.client.file.dora.netty.PartialReadException;
import alluxio.file.ReadTargetBuffer;

import com.google.common.base.Throwables;

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implementation of {@link PositionReader} that reads data through network from source.
 */
@ThreadSafe
public class DoraCachePositionReader implements PositionReader {
  private final NettyDataReader mNettyReader;
  private final long mFileLength;
  private final CloseableSupplier<PositionReader> mFallbackReader;
  private volatile boolean mClosed;

  /**
   * @param dataReader reader to read data through network
   * @param length file length
   * @param fallbackReader the position reader to fallback to when errors happen
   */
  // TODO(lu) structure for fallback position read
  public DoraCachePositionReader(NettyDataReader dataReader,
      long length, CloseableSupplier<PositionReader> fallbackReader) {
    mNettyReader = dataReader;
    mFileLength = length;
    mFallbackReader = fallbackReader;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    if (position >= mFileLength) { // at end of file
      return -1;
    }
    try {
      return mNettyReader.read(position, buffer, length);
    } catch (PartialReadException e) {
      int bytesRead = e.getBytesRead();
      if (bytesRead == 0) {
        // we didn't make any progress, throw the exception so that the caller needs to handle that
        Throwables.propagateIfPossible(e.getCause(), IOException.class);
        throw new IOException(e.getCause());
      }
      // otherwise ignore the exception and let the caller decide whether to continue
      return bytesRead;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mNettyReader.close();
    mFallbackReader.close();
  }
}
