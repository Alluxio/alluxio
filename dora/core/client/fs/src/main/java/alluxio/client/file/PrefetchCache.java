package alluxio.client.file;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import alluxio.PositionReader;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;
import java.io.IOException;

/**
 * This class represents a prefetch cache for file data. It is used to store and retrieve
 * prefetched data from a file. The cache is implemented as a {@link ByteBuf} and is
 * populated with data read from a {@link PositionReader}.
 * The cache keeps track of the position and size of each read operation that contributed
 * to the cache, and uses this information to determine the amount of data to prefetch in
 * subsequent read operations.
 * The cache is evicted on an LRU (Least Recently Used) basis, and can be closed to release
 * its resources.
 */
public class PrefetchCache implements AutoCloseable {
    private final long mFileLength;
    private final EvictingQueue<CallTrace> mCallHistory;
    private int mPrefetchSize = 0;

    private ByteBuf mCache = Unpooled.wrappedBuffer(new byte[0]);
    private long mCacheStartPos = 0;

    /**
     * Constructs a new PrefetchCache object with the specified prefetch multiplier and file length.
     *
     * @param prefetchMultiplier the multiplier used to determine the prefetch size
     * @param fileLength the length of the file
     */
    PrefetchCache(int prefetchMultiplier, long fileLength) {
        mCallHistory = EvictingQueue.create(prefetchMultiplier);
        mFileLength = fileLength;
    }

    /**
     * Updates the prefetch size based on the call history.
     */
    private void update() {
        int consecutiveReadLength = 0;
        long lastReadEnd = -1;
        for (CallTrace trace : mCallHistory) {
            if (trace.getPosition() == lastReadEnd) {
                lastReadEnd += trace.getLength();
                consecutiveReadLength += trace.getLength();
            } else {
                lastReadEnd = trace.getPosition() + trace.getLength();
                consecutiveReadLength = trace.getLength();
            }
        }
        mPrefetchSize = consecutiveReadLength;
    }

    /**
     * Adds a call trace to the cache's call history.
     *
     * @param pos the position within the file
     * @param size the size of the read operation
     */
    public void addTrace(long pos, int size) {
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
    public int fillWithCache(long targetStartPos, ByteBuffer outBuffer) {
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
     * @param reader the reader to prefetch data from
     * @param pos the position within the file to start the prefetch from
     * @param minBytesToRead minimum number of bytes to read from the reader
     * @return number of bytes that's been prefetched, 0 if exception occurs
     */
    public int prefetch(PositionReader reader, long pos, int minBytesToRead) {
        int prefetchSize = Math.max(mPrefetchSize, minBytesToRead);
        // cap to remaining file length
        prefetchSize = (int) Math.min(mFileLength - pos, prefetchSize);
        // Check if the prefetch size is valid. If not, return 0.
        if (prefetchSize <= 0) {
            return 0;
        }
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

    /**
     * Releases the resources used by the cache.
     */
    @Override
    public void close() {
        mCache.release();
        mCache = Unpooled.wrappedBuffer(new byte[0]);
        mCacheStartPos = 0;
    }

    /**
     * Returns the cache's underlying ByteBuf.
     *
     * @return the cache's ByteBuf
     */
    public ByteBuf getCache() {
        return mCache;
    }

    /**
     * Returns the start position of the cache.
     *
     * @return the start position of the cache
     */
    public long getCacheStartPos() {
        return mCacheStartPos;
    }

    /**
     * Returns the prefetch size of the cache.
     *
     * @return the prefetch size of the cache
     */
    public int getPrefetchSize() {
        return mPrefetchSize;
    }

    @VisibleForTesting
    public void setCache(ByteBuf cache) {
        mCache = cache;
    }

}
