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

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.io.BlockReader;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class PagedBlockReader extends BlockReader {

  private final long mPageSize;
  private final CacheManager mCacheManager;
  private final UfsManager mUfsManager;
  private final AlluxioConfiguration mConf;
  private final long mBlockId;
  private final Protocol.OpenUfsBlockOptions mUfsBlockOptions;

  public PagedBlockReader(CacheManager cacheManager, UfsManager ufsManager,
                          AlluxioConfiguration conf,
                          long blockId, Protocol.OpenUfsBlockOptions ufsBlockOptions) {

    mCacheManager = cacheManager;
    mUfsManager = ufsManager;
    mConf = conf;
    mBlockId = blockId;
    mUfsBlockOptions = ufsBlockOptions;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    byte[] buf = new byte[(int) length];
    long bytesRead = 0;
    while (bytesRead < length) {
      long pos = offset + bytesRead;
      long pageIndex = pos / mPageSize;
      PageId pageId = new PageId(String.valueOf(mBlockId), pageIndex);
      int currentPageOffset = (int) (pos % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, length - bytesRead);
      int bytesReadFromCache = mCacheManager.get(
          pageId, currentPageOffset, bytesLeftInPage, buf, (int) bytesRead);
      if (bytesReadFromCache > 0) {
        bytesRead += bytesReadFromCache;
      } else {
        byte[] page = readExternalPage(pos);
      }
    }

    return ByteBuffer.wrap(buf);
  }

  private byte[] readExternalPage(long pos) {

  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public ReadableByteChannel getChannel() {
    return null;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    return 0;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public String getLocation() {
    return null;
  }
}
