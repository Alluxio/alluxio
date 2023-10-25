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

package alluxio.client.file.cache;

import alluxio.client.file.CacheContext;
import alluxio.exception.PageNotFoundException;
import alluxio.file.ReadTargetBuffer;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataFileChannel;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A wrapper class of CacheManager without throwing unchecked exceptions.
 */
public class NoExceptionCacheManager implements CacheManager {
  private static final Logger LOG = LoggerFactory.getLogger(NoExceptionCacheManager.class);

  private final CacheManager mCacheManager;

  /**
   * @param cacheManager delegated cache manager
   */
  public NoExceptionCacheManager(CacheManager cacheManager) {
    mCacheManager = cacheManager;
  }

  @Override
  public void commitFile(String fileId) {
    try {
      mCacheManager.commitFile(fileId);
    } catch (Exception e) {
      LOG.error("Failed to commit file {}", fileId, e);
    }
  }

  @Override
  public boolean put(PageId pageId, byte[] page) {
    try {
      return mCacheManager.put(pageId, page);
    } catch (Exception e) {
      LOG.error("Failed to put page {}", pageId, e);
      Metrics.PUT_ERRORS.inc();
      return false;
    }
  }

  @Override
  public boolean put(PageId pageId, ByteBuffer page, CacheContext cacheContext) {
    try {
      return mCacheManager.put(pageId, page, cacheContext);
    } catch (Exception e) {
      LOG.error("Failed to put page {}, cacheContext {}", pageId, cacheContext, e);
      Metrics.PUT_ERRORS.inc();
      return false;
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, ReadTargetBuffer buffer,
                 CacheContext cacheContext) {
    int originalOffset = buffer.offset();
    try {
      int bytesRead =  mCacheManager.get(pageId, pageOffset, buffer, cacheContext);
      if (bytesRead == -1) {
        buffer.offset(originalOffset);
      }
      return bytesRead;
    } catch (Exception e) {
      LOG.error("Failed to get page {}", pageId, e);
      Metrics.GET_ERRORS.inc();
      //In case any error in cache manager, revert the offset change in the buffer
      buffer.offset(originalOffset);
      return -1;
    }
  }

  @Override
  public int get(PageId pageId, int bytesToRead, byte[] buffer, int offsetInBuffer) {
    try {
      return mCacheManager.get(pageId, bytesToRead, buffer, offsetInBuffer);
    } catch (Exception e) {
      LOG.error("Failed to get page {}", pageId, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer) {
    try {
      return mCacheManager.get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer);
    } catch (Exception e) {
      LOG.error("Failed to get page {}, offset {}", pageId, pageOffset, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer,
      int offsetInBuffer, CacheContext cacheContext) {
    try {
      return mCacheManager
          .get(pageId, pageOffset, bytesToRead, buffer, offsetInBuffer, cacheContext);
    } catch (Exception e) {
      LOG.error("Failed to get page {}, offset {} cacheContext {}", pageId, pageOffset,
          cacheContext, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
  }

  @Override
  public int get(PageId pageId, int pageOffset, int bytesToRead, ReadTargetBuffer buffer,
      CacheContext cacheContext) {
    try {
      return mCacheManager
          .get(pageId, pageOffset, bytesToRead, buffer, cacheContext);
    } catch (Exception e) {
      LOG.error("Failed to get page {}, offset {} cacheContext {}", pageId, pageOffset,
          cacheContext, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
  }

  @Override
  public int getAndLoad(PageId pageId, int pageOffset, int bytesToRead,
      ReadTargetBuffer buffer, CacheContext cacheContext,
      Supplier<byte[]> externalDataSupplier) {
    try {
      return mCacheManager.getAndLoad(pageId, pageOffset, bytesToRead,
          buffer, cacheContext, externalDataSupplier);
    } catch (Exception e) {
      LOG.error("Failed to get and load page {}, offset {} cacheContext {}", pageId, pageOffset,
          cacheContext, e);
      Metrics.GET_ERRORS.inc();
      return -1;
    }
  }

  @Override
  public boolean delete(PageId pageId) {
    try {
      return mCacheManager.delete(pageId);
    } catch (Exception e) {
      LOG.error("Failed to delete page {}", pageId, e);
      Metrics.DELETE_ERRORS.inc();
      return false;
    }
  }

  @Override
  public Optional<DataFileChannel> getDataFileChannel(PageId pageId, int pageOffset,
      int bytesToRead, CacheContext cacheContext) {
    try {
      return mCacheManager.getDataFileChannel(pageId, pageOffset, bytesToRead, cacheContext);
    } catch (Exception e) {
      if (e instanceof PageNotFoundException) {
        // In cold read, this may be expected behavior
        LOG.debug("Failed to getDataFileChannel of page {}", pageId, e);
      } else {
        LOG.error("Failed to getDataFileChannel of page {}", pageId, e);
      }
      Metrics.GET_ERRORS.inc();
      return Optional.empty();
    }
  }

  @Override
  public State state() {
    return mCacheManager.state();
  }

  @Override
  public boolean append(PageId pageId, int appendAt, byte[] page, CacheContext cacheContext) {
    return mCacheManager.append(pageId, appendAt, page, cacheContext);
  }

  @Override
  public void close() throws Exception {
    try {
      mCacheManager.close();
    } catch (Exception e) {
      LOG.error("Failed to close CacheManager", e);
    }
  }

  @Override
  public List<PageId> getCachedPageIdsByFileId(String fileId, long fileLength) {
    return mCacheManager.getCachedPageIdsByFileId(fileId, fileLength);
  }

  @Override
  public boolean hasPageUnsafe(PageId pageId) {
    return mCacheManager.hasPageUnsafe(pageId);
  }

  @Override
  public void deleteFile(String fileId) {
    try {
      mCacheManager.deleteFile(fileId);
    } catch (Exception e) {
      LOG.error("Failed to deleteFile for {}", fileId, e);
    }
  }

  @Override
  public void deleteTempFile(String fileId) {
    try {
      mCacheManager.deleteTempFile(fileId);
    } catch (Exception e) {
      LOG.error("Failed to deleteFile for {}", fileId, e);
    }
  }

  @Override
  public Optional<CacheUsage> getUsage() {
    return mCacheManager.getUsage();
  }

  private static final class Metrics {
    // Note that only counter/guage can be added here.
    // Both meter and timer need to be used inline
    // because new meter and timer will be created after {@link MetricsSystem.resetAllMetrics()}
    /** Errors when deleting pages. */
    private static final Counter DELETE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_DELETE_ERRORS.getName());
    /** Errors when getting pages. */
    private static final Counter GET_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_GET_ERRORS.getName());
    /** Errors when adding pages. */
    private static final Counter PUT_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_PUT_ERRORS.getName());

    private Metrics() {} // prevent instantiation
  }
}
