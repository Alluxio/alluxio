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

import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implementation of {@link FileInStream} that reads from a dora cache if possible.
 */
@NotThreadSafe
public class PositionReadFileInStream extends FileInStream {
  private final long mLength;
  private long mPos = 0;
  private boolean mClosed;
  public final PositionReader mPositionReader;
  private final PrefetchCache mCache;

  private static class PrefetchCache implements AutoCloseable {
    private final long mFileLength;
    private final EvictingQueue<CallTrace> mCallHistory;
    private int mPrefetchSize = 0;

    private ByteBuf mCache = Unpooled.wrappedBuffer(new byte[0]);
    private long mCacheStartPos = 0;

    PrefetchCache(int prefetchMultiplier, long fileLength) {
      mCallHistory = EvictingQueue.create(prefetchMultiplier);
      mFileLength = fileLength;
    }

    private void update() {
      int consecutiveReadLength = 0;
      long lastReadEnd = -1;
      for (CallTrace trace : mCallHistory) {
        if (trace.mPosition == lastReadEnd) {
          lastReadEnd += trace.mLength;
          consecutiveReadLength += trace.mLength;
        } else {
          lastReadEnd = trace.mPosition + trace.mLength;
          consecutiveReadLength = trace.mLength;
        }
      }
      mPrefetchSize = consecutiveReadLength;
    }

    private void addTrace(long pos, int size) {
      mCallHistory.add(new CallTrace(pos, size));
      update();
    }

    /**
     * Fills the output with bytes from the prefetch cache.
     *
     * @param targetStartPos the position within the file to read from
     * @param outBuffer output buffer
     * @return number of bytes copied from the cache, 0 if the cache does not contain the requested
     *         range of data
     */
    private int fillWithCache(long targetStartPos, ByteBuffer outBuffer) {
      if (mCacheStartPos <= targetStartPos) {
        if (targetStartPos - mCacheStartPos < mCache.readableBytes()) {
          final int posInCache = (int) (targetStartPos - mCacheStartPos);
          final int size = Math.min(outBuffer.remaining(), mCache.readableBytes() - posInCache);
          ByteBuffer slice = outBuffer.slice();
          slice.limit(size);
          mCache.getBytes(posInCache, slice);
          outBuffer.position(outBuffer.position() + size);
          return size;
        } else {
          // the position is beyond the cache end position
          return 0;
        }
      } else {
        // the position is behind the cache start position
        return 0;
      }
    }

    /**
     * Prefetches and caches data from the reader.
     *
     * @param reader reader
     * @param pos position within the file
     * @param minBytesToRead minimum number of bytes to read from the reader
     * @return number of bytes that's been prefetched, 0 if exception occurs
     */
    private int prefetch(PositionReader reader, long pos, int minBytesToRead) {
      int prefetchSize = Math.max(mPrefetchSize, minBytesToRead);
      // cap to remaining file length
      prefetchSize = (int) Math.min(mFileLength - pos, prefetchSize);

      if (mCache.capacity() < prefetchSize) {
        mCache.release();
        mCache = PooledDirectNioByteBuf.allocate(prefetchSize);
        mCacheStartPos = 0;
      }
      mCache.clear();
      try {
        int bytesPrefetched = reader.read(pos, mCache, prefetchSize);
        if (bytesPrefetched > 0) {
          mCache.readerIndex(0).writerIndex(bytesPrefetched);
          mCacheStartPos = pos;
        }
        return bytesPrefetched;
      } catch (IOException ignored) {
        // silence exceptions as we don't care if prefetch fails
        mCache.clear();
        return 0;
      }
    }

    @Override
    public void close() {
      mCache.release();
      mCache = Unpooled.wrappedBuffer(new byte[0]);
      mCacheStartPos = 0;
    }
  }

  private static class CallTrace {
    final long mPosition;
    final int mLength;

    private CallTrace(long pos, int length) {
      mPosition = pos;
      mLength = length;
    }
  }

  /**
   * Constructor.
   * @param reader
   * @param length
   */
  public PositionReadFileInStream(PositionReader reader,
      long length) {
    mPositionReader = reader;
    mLength = length;
    mCache = new PrefetchCache(
        Configuration.getInt(PropertyKey.USER_POSITION_READER_STREAMING_MULTIPLIER), mLength);
  }

  @Override
  public long remaining() {
    return mLength - mPos;
  }

  @VisibleForTesting
  int getBufferedLength() {
    return mCache.mCache.readableBytes();
  }

  @VisibleForTesting
  long getBufferedPosition() {
    return mCache.mCacheStartPos;
  }

  @VisibleForTesting
  int getPrefetchSize() {
    return mCache.mPrefetchSize;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byteBuffer.position(off).limit(off + len);
    mCache.addTrace(mPos, len);
    int totalBytesRead = 0;
    int bytesReadFromCache = mCache.fillWithCache(mPos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    mPos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesPrefetched = mCache.prefetch(mPositionReader, mPos, byteBuffer.remaining());
    if (bytesPrefetched < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    bytesReadFromCache = mCache.fillWithCache(mPos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    mPos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesRead = mPositionReader.read(mPos, byteBuffer, byteBuffer.remaining());
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    totalBytesRead += bytesRead;
    mPos += bytesRead;
    return totalBytesRead;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int len)
      throws IOException {
    long pos = position;
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
    mCache.addTrace(position, len);
    int totalBytesRead = 0;
    int bytesReadFromCache = mCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesPrefetched = mCache.prefetch(mPositionReader, pos, byteBuffer.remaining());
    if (bytesPrefetched < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    bytesReadFromCache = mCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;
    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }
    int bytesRead = mPositionReader.read(pos, byteBuffer, byteBuffer.remaining());
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }
    totalBytesRead += bytesRead;
    pos += bytesRead;
    return totalBytesRead;
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        "Seek position past the end of the read region (block or file).");
    if (pos == mPos) {
      return;
    }
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    seek(mPos + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mPositionReader.close();
    mCache.close();
  }
}
