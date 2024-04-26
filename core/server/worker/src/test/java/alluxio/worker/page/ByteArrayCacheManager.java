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
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.file.ReadTargetBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Implementation of cache manager that stores cached data in byte arrays in memory.
 */
class ByteArrayCacheManager implements CacheManager {
  private final Map<PageId, byte[]> mPages;

  /**
   * Metrics for test validation.
   */
  long mPagesServed = 0;
  long mPagesCached = 0;

  ByteArrayCacheManager() {
    mPages = new HashMap<>();
  }

  @Override
  public void commitFile(String fileId) {
    throw new UnsupportedOperationException("commitFile method is unsupported. ");
  }

  @Override
  public boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext) {
    byte[] data = new byte[page.remaining()];
    page.get(data);
    mPages.put(pageId, data);
    mPagesCached++;
    return true;
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer target,
      CacheContext cacheContext) {
    if (!mPages.containsKey(pageId)) {
      return 0;
    }
    mPagesServed++;
    target.writeBytes(mPages.get(pageId), pageOffset, bytesToRead);
    return bytesToRead;
  }

  @Override
  public int getAndLoad(PageId pageId, int pageOffset, int bytesToRead,
      ReadTargetBuffer buffer, CacheContext cacheContext,
      Supplier<byte[]> externalDataSupplier) {
    int bytesRead = get(pageId, pageOffset,
        bytesToRead, buffer, cacheContext);
    if (bytesRead > 0) {
      return bytesRead;
    }
    byte[] page = externalDataSupplier.get();
    if (page.length == 0) {
      return 0;
    }
    buffer.writeBytes(page, pageOffset, bytesToRead);
    put(pageId, page, cacheContext);
    return bytesToRead;
  }

  @Override
  public boolean delete(PageId pageId) {
    return mPages.remove(pageId) != null;
  }

  @Override
  public State state() {
    return State.READ_WRITE;
  }

  @Override
  public boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext) {
    return false;
  }

  @Override
  public void deleteFile(String fileId) {
    mPages.keySet().removeIf(pageId -> pageId.getFileId().equals(fileId));
  }

  @Override
  public void deleteTempFile(String fileId) {
    mPages.keySet().removeIf(pageId -> pageId.getFileId().equals(fileId));
  }

  @Override
  public Optional<CacheUsage> getUsage() {
    return Optional.of(new Usage());
  }

  @Override
  public Optional<DataFileChannel> getDataFileChannel(PageId pageId, int pageOffset,
      int bytesToRead, CacheContext cacheContext) {
    return Optional.empty();
  }

  class Usage implements CacheUsage {
    @Override
    public Optional<CacheUsage> partitionedBy(PartitionDescriptor<?> partition) {
      return Optional.empty();
    }

    @Override
    public long used() {
      return mPages.values().stream().mapToInt(page -> page.length).sum();
    }

    @Override
    public long available() {
      return Integer.MAX_VALUE;
    }

    @Override
    public long capacity() {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public void close() {
    // no-op
  }
}
