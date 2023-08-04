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

package alluxio.worker.dora;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.grpc.WritePType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A paged implementation of BlockWriter interface.
 */
public class PagedFileWriter extends BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PagedFileWriter.class);
  private final CacheContext mTempCacheContext;

  private final CacheManager mCacheManager;
  private final String mFileId;
  private final long mPageSize;
  private long mPosition;

  private final PagedDoraWorker mWorker;

  private final String mUfsPath;

  PagedFileWriter(PagedDoraWorker worker, String ufsPath,
                  CacheManager cacheManager, String fileId, long pageSize) {
    mWorker = Preconditions.checkNotNull(worker);
    mTempCacheContext = CacheContext.defaults().setTemporary(true);
    mCacheManager = Preconditions.checkNotNull(cacheManager);
    mFileId = Preconditions.checkNotNull(fileId);
    mPageSize = pageSize;
    mUfsPath = Preconditions.checkNotNull(ufsPath);
  }

  @Override
  public void abort() {
    mCacheManager.deleteTempFile(mFileId);
  }

  @Override
  public void commitFile() {
    mCacheManager.commitFile(mFileId);
  }

  @Override
  public long append(ByteBuffer inputBuf) {
    try {
      return append(Unpooled.wrappedBuffer(inputBuf));
    } catch (IOException e) {
      LOG.error("Failed to append ByteBuffer. ", e);
      return -1;
    }
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    long bytesWritten = 0;
    LOG.debug("Writing @" + mPosition + "len=" + buf.readableBytes());
    DoraOpenFileHandleContainer openFileHandleContainer = mWorker.getOpenFileHandleContainer();
    OpenFileHandle handle = openFileHandleContainer.find(mUfsPath);

    while (buf.readableBytes() > 0) {
      PageId pageId = getPageId(bytesWritten);
      int currentPageOffset = getCurrentPageOffset(bytesWritten);
      int bytesLeftInPage = getBytesLeftInPage(currentPageOffset, buf.readableBytes());
      byte[] page = new byte[bytesLeftInPage];
      buf.readBytes(page);
      if (handle != null && handle.getOptions().hasWriteType()
          && handle.getOptions().getWriteType() != WritePType.THROUGH) {
        // Don't write to local paging cache for THROUGH
        if (!mCacheManager.append(pageId, currentPageOffset, page, mTempCacheContext)) {
          throw new IOException("Append failed for file " + mFileId);
        }
      }
      // Now writes data to UFS.
      if (handle != null) {
        OutputStream outputStream = handle.getOutStream();
        if (outputStream != null) {
          outputStream.write(page);
        }
      } else {
        throw new IOException("Cannot write data to UFS for " + mUfsPath + " @" + mPosition);
      }
      bytesWritten += bytesLeftInPage;
    }

    // data is written to local cache and UFS. Update Position.
    mPosition += bytesWritten;
    LOG.debug("after write " + bytesWritten + " bytes. New pos = " + mPosition);
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
    return new PageId(mFileId, pageIndex);
  }

  private int getCurrentPageOffset(long bytesWritten) {
    return (int) ((mPosition + bytesWritten) % mPageSize);
  }

  private int getBytesLeftInPage(int currentPageOffset, int remaining) {
    return (int) Math.min(mPageSize - currentPageOffset, remaining);
  }
}
