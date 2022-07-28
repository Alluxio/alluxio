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

package alluxio.worker.page;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.exception.runtime.InternalRuntimeException;
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
  private static final CacheContext TEMP_CACHE_CONTEXT = CacheContext.defaults().setTemporary(true);

  private final CacheManager mCacheManager;
  private final String mBlockId;
  private final long mPageSize;
  private long mPosition;

  PagedBlockWriter(CacheManager cacheManager, long blockId, long pageSize) {
    mCacheManager = cacheManager;
    mBlockId = String.valueOf(blockId);
    mPageSize = pageSize;
  }

  @Override
  public long append(ByteBuffer inputBuf) {
    long bytesWritten = 0;
    while (inputBuf.hasRemaining()) {
      PageId pageId = getPageId(bytesWritten);
      int currentPageOffset = getCurrentPageOffset(bytesWritten);
      int bytesLeftInPage = getBytesLeftInPage(currentPageOffset, inputBuf.remaining());
      byte[] page = new byte[bytesLeftInPage];
      inputBuf.get(page);
      if (!mCacheManager.append(pageId, currentPageOffset, page, TEMP_CACHE_CONTEXT)) {
        throw new InternalRuntimeException("Append failed for block " + mBlockId);
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
      PageId pageId = getPageId(bytesWritten);
      int currentPageOffset = getCurrentPageOffset(bytesWritten);
      int bytesLeftInPage = getBytesLeftInPage(currentPageOffset, buf.readableBytes());
      byte[] page = new byte[bytesLeftInPage];
      buf.readBytes(page);
      if (!mCacheManager.append(pageId, currentPageOffset, page, TEMP_CACHE_CONTEXT)) {
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

  private PageId getPageId(long bytesWritten) {
    long pageIndex = (mPosition + bytesWritten) / mPageSize;
    return new BlockPageId(mBlockId, pageIndex);
  }

  private int getCurrentPageOffset(long bytesWritten) {
    return (int) ((mPosition + bytesWritten) % mPageSize);
  }

  private int getBytesLeftInPage(int currentPageOffset, int remaining) {
    return (int) Math.min(mPageSize - currentPageOffset, remaining);
  }
}
