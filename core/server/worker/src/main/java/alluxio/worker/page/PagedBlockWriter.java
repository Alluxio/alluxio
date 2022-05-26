package alluxio.worker.page;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.worker.block.io.BlockWriter;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A paged implementation of BlockWriter interface.
 */
public class PagedBlockWriter extends BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PagedBlockWriter.class);

  private final CacheManager mCacheManager;
  private final long mBlockId;
  private final long mPageSize;
  private long mPosition;

  PagedBlockWriter(CacheManager cacheManager, long blockId, AlluxioConfiguration conf) {
    mCacheManager = cacheManager;
    mBlockId = blockId;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    long bytesWritten = 0;
    while (inputBuf.hasRemaining()) {
      long pageIndex = (mPosition + bytesWritten) / mPageSize;
      PageId pageId = new PageId(String.valueOf(mBlockId), pageIndex);
      int currentPageOffset = (int) (mPosition % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, inputBuf.remaining());
      byte[] page = new byte[bytesLeftInPage];
      inputBuf.get(page);
      if (!mCacheManager.append(pageId, currentPageOffset, page, CacheContext.defaults())) {
        throw new IOException("Append failed for block " + mBlockId);
      }
      bytesWritten += bytesLeftInPage;
    }
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    long bytesWritten = 0;
    while (buf.readableBytes() > 0) {
      long pageIndex = (mPosition + bytesWritten) / mPageSize;
      PageId pageId = new PageId(String.valueOf(mBlockId), pageIndex);
      int currentPageOffset = (int) (mPosition % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, buf.readableBytes());
      byte[] page = new byte[bytesLeftInPage];
      buf.readBytes(page);
      if (!mCacheManager.append(pageId, currentPageOffset, page, CacheContext.defaults())) {
        throw new IOException("Append failed for block " + mBlockId);
      }
      bytesWritten += bytesLeftInPage;
    }
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(DataBuffer buffer) throws IOException {
    ByteBuf bytebuf = null;
    try {
      bytebuf = (ByteBuf) buffer.getNettyOutput();
    } catch (Throwable e) {
      LOG.debug("Failed to get ByteBuf from DataBuffer, write performance may be degraded.");
    }
    if (bytebuf != null) {
      return append(bytebuf);
    }
    return append(buffer.getReadOnlyByteBuffer());
  }

  @Override
  public long getPosition() {
    return mPosition;
  }

  @Override
  public WritableByteChannel getChannel() {
    throw new UnsupportedOperationException();
  }
}
