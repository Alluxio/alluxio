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

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.ThreadUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * FileSystem implementation with the capability of caching metadata of paths.
 */
@ThreadSafe
public class MetadataCachingFileSystem extends DelegatingFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCachingFileSystem.class);
  private static final int THREAD_KEEPALIVE_SECOND = 60;
  private static final int THREAD_TERMINATION_TIMEOUT_MS = 10000;
  private static final URIStatus NOT_FOUND_STATUS = new URIStatus(
      new FileInfo().setCompleted(true));

  private final FileSystemContext mFsContext;
  private final MetadataCache mMetadataCache;
  private final ExecutorService mAccessTimeUpdater;
  private final boolean mDisableUpdateFileAccessTime;

  /**
   * @param fileSystem the file system
   * @param context the fs context
   */
  public MetadataCachingFileSystem(FileSystem fileSystem, FileSystemContext context) {
    super(fileSystem);
    mFsContext = context;
    int maxSize = mFsContext.getClusterConf().getInt(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
    Preconditions.checkArgument(maxSize != 0,
        "%s should not be zero to enable metadata caching file system",
        PropertyKey.USER_METADATA_CACHE_MAX_SIZE.getName());

    mMetadataCache = mFsContext.getClusterConf()
        .isSet(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME)
        ? new MetadataCache(maxSize, mFsContext.getClusterConf()
            .getMs(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME))
        : new MetadataCache(maxSize);
    int masterClientThreads = mFsContext.getClusterConf()
        .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX);
    mDisableUpdateFileAccessTime = mFsContext.getClusterConf()
        .getBoolean(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED);
    // At a time point, there are at most the same number of concurrent master clients that
    // asynchronously update access time.
    mAccessTimeUpdater = new ThreadPoolExecutor(0, masterClientThreads, THREAD_KEEPALIVE_SECOND,
        TimeUnit.SECONDS, new SynchronousQueue<>());
    MetricsSystem.registerCachedGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.CLIENT_META_DATA_CACHE_SIZE.getName()),
        mMetadataCache::size);
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    mMetadataCache.invalidate(path.getParent());
    mMetadataCache.invalidate(path);
    mDelegatedFileSystem.createDirectory(path, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws IOException, AlluxioException {
    mMetadataCache.invalidate(path.getParent());
    mMetadataCache.invalidate(path);
    return mDelegatedFileSystem.createFile(path, options);
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws IOException,
      AlluxioException {
    mMetadataCache.invalidate(path.getParent());
    mMetadataCache.invalidate(path);
    mDelegatedFileSystem.delete(path, options);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws IOException, AlluxioException {
    mMetadataCache.invalidate(src.getParent());
    mMetadataCache.invalidate(src);
    mMetadataCache.invalidate(dst.getParent());
    mMetadataCache.invalidate(dst);
    mDelegatedFileSystem.rename(src, dst, options);
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws IOException, AlluxioException {
    return mDelegatedFileSystem.getBlockLocations(getStatus(path));
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    URIStatus status = mMetadataCache.get(path);
    if (status == null || !status.isCompleted()) {
      try {
        status = mDelegatedFileSystem.getStatus(path, options);
        mMetadataCache.put(path, status);
      } catch (FileDoesNotExistException e) {
        mMetadataCache.put(path, NOT_FOUND_STATUS);
        throw e;
      }
    } else if (status == NOT_FOUND_STATUS) {
      throw new FileDoesNotExistException("Path \"" + path.getPath() + "\" does not exist.");
    } else if (options.getUpdateTimestamps()) {
      // Asynchronously send an RPC to master to update the access time.
      // Otherwise, if we need to synchronously send RPC to master to do this,
      // caching the status does not bring any benefit.
      asyncUpdateFileAccessTime(path);
    }
    return status;
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
      Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.getRecursive()) {
      // Do not cache results of recursive list status,
      // because some results might be cached multiple times.
      // Otherwise, needs more complicated logic inside the cache,
      // that might not worth the effort of caching.
      mDelegatedFileSystem.iterateStatus(path, options, action);
      return;
    }

    List<URIStatus> cachedStatuses = mMetadataCache.listStatus(path);
    if (cachedStatuses == null) {
      List<URIStatus> statuses = new ArrayList<>();
      mDelegatedFileSystem.iterateStatus(path, options, status -> {
        statuses.add(status);
        action.accept(status);
      });
      mMetadataCache.put(path, statuses);
      return;
    }
    cachedStatuses.forEach(action);
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.getRecursive()) {
      // Do not cache results of recursive list status,
      // because some results might be cached multiple times.
      // Otherwise, needs more complicated logic inside the cache,
      // that might not worth the effort of caching.
      return mDelegatedFileSystem.listStatus(path, options);
    }

    List<URIStatus> statuses = mMetadataCache.listStatus(path);
    if (statuses == null) {
      statuses = mDelegatedFileSystem.listStatus(path, options);
      mMetadataCache.put(path, statuses);
    }
    return statuses;
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    URIStatus status = getStatus(path,
        FileSystemOptionsUtils.getStatusDefaults(mFsContext.getClusterConf()).toBuilder()
            .setAccessMode(Bits.READ)
            .setUpdateTimestamps(options.getUpdateLastAccessTime())
            .build());
    return mDelegatedFileSystem.openFile(status, options);
  }

  /**
   * Asynchronously update file's last access time.
   *
   * @param path the path to the file
   */
  @VisibleForTesting
  public void asyncUpdateFileAccessTime(AlluxioURI path) {
    if (mDisableUpdateFileAccessTime) {
      return;
    }
    try {
      mAccessTimeUpdater.submit(() -> {
        try {
          AlluxioConfiguration conf = mFsContext.getClusterConf();
          GetStatusPOptions getStatusOptions = FileSystemOptionsUtils
              .getStatusDefaults(conf).toBuilder()
              .setAccessMode(Bits.READ)
              .setUpdateTimestamps(true)
              .build();
          super.getStatus(path, getStatusOptions);
        } catch (IOException | AlluxioException e) {
          LOG.error("Failed to update access time for " + path, e);
        }
      });
    } catch (RejectedExecutionException e) {
      LOG.warn("Failed to submit a task to update access time for {}: {}", path, e.toString());
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!mDelegatedFileSystem.isClosed()) {
      ThreadUtils.shutdownAndAwaitTermination(mAccessTimeUpdater, THREAD_TERMINATION_TIMEOUT_MS);
      mDelegatedFileSystem.close();
    }
  }

  /**
   * Best efforts to drops metadata cache of a given uri,
   * all its ancestors and descendants.
   *
   * @param uri the uri need to drop metadata cache
   */
  public void dropMetadataCache(AlluxioURI uri) {
    dropMetadataCacheDescendants(uri.getPath());
    dropMetadataCacheAncestors(uri);
  }

  /**
   * Best efforts to drop metadata cache of a given uri
   * and all its ancestors.
   *
   * @param uri the uri need to drop metadata cache
   */
  private void dropMetadataCacheAncestors(AlluxioURI uri) {
    mMetadataCache.invalidate(uri);
    LOG.debug("Invalidated metadata cache for path {}", uri);
    if (!uri.isRoot()) {
      AlluxioURI parentUri = uri.getParent();
      if (parentUri != null) {
        dropMetadataCacheAncestors(parentUri);
      }
    }
  }

  /**
   * Best efforts to drop metadata cache of a given uri
   * and all its descendants.
   *
   * @param path the path need to drop metadata cache
   */
  private void dropMetadataCacheDescendants(String path) {
    List<URIStatus> children = mMetadataCache.listStatus(path);
    if (children != null) {
      for (URIStatus child : children) {
        dropMetadataCacheDescendants(child.getPath());
      }
    }
    mMetadataCache.invalidate(path);
    LOG.debug("Invalidated metadata cache for path {}", path);
  }

  /**
   * Drop all metadata cache.
   */
  public void dropMetadataCacheAll() {
    if (mMetadataCache.size() > 0) {
      mMetadataCache.invalidateAll();
      LOG.debug("Invalidated all metadata cache");
    }
  }

  /**
   * @return metadata cache size
   */
  public long getMetadataCacheSize() {
    return mMetadataCache.size();
  }
}
