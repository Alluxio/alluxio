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

package alluxio.worker.http;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.NettyBufTargetBuffer;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * {@link PagedService} is used for providing page related RESTful API service.
 */
public class PagedService {

  private final CacheManager mCacheManager;

  private final long mPageSize;

  /**
   * {@link PagedService} is used for providing page related RESTful API service.
   * @param cacheManager The interface for managing cached pages
   */
  @Inject
  public PagedService(CacheManager cacheManager) {
    mCacheManager = cacheManager;
    mPageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
  }

  /**
   * Get page bytes given fileId, pageIndex, and channel which is used for allocating ByteBuf.
   * @param fileId the file ID
   * @param pageIndex the page index
   * @param channel the Netty channel which is used for allocating ByteBuf
   * @return the ByteBuf object that wraps page bytes
   */
  public ByteBuf getPage(String fileId, long pageIndex, Channel channel) {
    ByteBuf byteBuf = channel.alloc().buffer((int) mPageSize);
    NettyBufTargetBuffer targetBuffer = new NettyBufTargetBuffer(byteBuf);
    PageId pageId = new PageId(fileId, pageIndex);
    // TODO(JiamingMai): load the page from UFS if it doesn't exist, but this requires AlluxioURI
    // instead of the given fileId
    int bytesRead = mCacheManager.get(pageId, 0, targetBuffer, CacheContext.defaults());
    return targetBuffer.getTargetBuffer();
  }
  // TODO(JiamingMai): do we need to implement a method for reading file directly?
}
