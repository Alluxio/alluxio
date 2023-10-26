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

import alluxio.AlluxioURI;
import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.file.CacheContext;
import alluxio.client.file.DelegatingFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.CacheFilter;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.metrics.MetricsSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileSystem.class);
  private final CacheManager mCacheManager;
  private final CacheFilter mCacheFilter;
  private final AlluxioConfiguration mConf;

  /**
   * @param cacheManage cache manager
   * @param fs a FileSystem instance to query on local cache miss
   * @param conf the configuration, only respected for the first call
   */
  public LocalCacheFileSystem(CacheManager cacheManage, FileSystem fs, AlluxioConfiguration conf) {
    super(fs);
    // Handle metrics
    MetricsSystem.startSinks(conf.getString(PropertyKey.METRICS_CONF_FILE));
    mCacheManager = Preconditions.checkNotNull(cacheManage, "cacheManager");
    mConf = Preconditions.checkNotNull(conf, "conf");
    mCacheFilter = CacheFilter.create(conf);
    LocalCacheFileInStream.registerMetrics();
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mDelegatedFileSystem.getConf();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    if (mCacheManager == null || mCacheManager.state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openFile(path, options);
    }
    return openFile(mDelegatedFileSystem.getStatus(path), options);
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws IOException, AlluxioException {
    if (mCacheManager == null || mCacheManager.state() == CacheManager.State.NOT_IN_USE
        || !mCacheFilter.needsCache(status)) {
      return mDelegatedFileSystem.openFile(status, options);
    }
    return new LocalCacheFileInStream(status,
        uriStatus -> mDelegatedFileSystem.openFile(status, options), mCacheManager, mConf,
        Optional.empty());
  }

  @Override
  public PositionReader openPositionRead(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException {
    if (mCacheManager == null || mCacheManager.state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openPositionRead(path, options);
    }
    try {
      return openPositionRead(mDelegatedFileSystem.getStatus(path), options);
    } catch (IOException | AlluxioException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  @Override
  public PositionReader openPositionRead(URIStatus status, OpenFilePOptions options) {
    if (mCacheManager == null || mCacheManager.state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openPositionRead(status, options);
    }
    return LocalCachePositionReader.create(mConf, mCacheManager,
        new CloseableSupplier<>(() -> mDelegatedFileSystem.openPositionRead(status, options)),
        status, mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE),
        status.getCacheContext() == null ? CacheContext.defaults() : status.getCacheContext());
  }
}
