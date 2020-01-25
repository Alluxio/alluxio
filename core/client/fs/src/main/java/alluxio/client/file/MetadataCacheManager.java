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
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.Bits;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FileSystemOptions;
import alluxio.util.ThreadUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Cache Manager that can cache metadata of paths.
 */
public class MetadataCacheManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheManager.class);
  private static final int THREAD_KEEPALIVE_SECOND = 60;
  private static final int THREAD_TERMINATION_TIMEOUT_MS = 10000;
  private final FileSystemContext mFsContext;
  private final MetadataCache mMetadataCache;
  private final ExecutorService mAccessTimeUpdater;
  private final FileSystem mFileSystem;

  /**
   * @param fs delegated file system
   * @param context the fs context
   */
  public MetadataCacheManager(FileSystem fs, FileSystemContext context) {
    mFileSystem = fs;
    mFsContext = context;
    int maxSize = mFsContext.getClusterConf().getInt(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
    long expirationTimeMs = mFsContext.getClusterConf()
        .getMs(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME);
    mMetadataCache = new MetadataCache(maxSize, expirationTimeMs);
    int masterClientThreads = mFsContext.getClusterConf()
        .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX);
    // At a time point, there are at most the same number of concurrent master clients that
    // asynchronously update access time.
    mAccessTimeUpdater = new ThreadPoolExecutor(0, masterClientThreads, THREAD_KEEPALIVE_SECOND,
        TimeUnit.SECONDS, new SynchronousQueue<>());
  }

  /**
   * Gets the {@link URIStatus} object that represents the metadata of an Alluxio path.
   *
   * @param path the path to obtain information about
   * @param options options to associate with this operation
   * @return the {@link URIStatus} of the file
   * @throws FileDoesNotExistException if the path does not exist
   */
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    URIStatus status = mMetadataCache.get(path);
    if (status == null) {
      status = mFileSystem.getStatus(path, options);
      mMetadataCache.put(path, status);
    } else if (options.getUpdateTimestamps()) {
      // Asynchronously send an RPC to master to update the access time.
      // Otherwise, if we need to synchronously send RPC to master to do this,
      // caching the status does not bring any benefit.
      asyncUpdateFileAccessTime(path);
    }
    return status;
  }

  /**
   * If the path is a directory, returns the {@link URIStatus} of all the direct entries in it.
   * Otherwise returns a list with a single {@link URIStatus} element for the file.
   *
   * @param path the path to list information about
   * @param options options to associate with this operation
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws IOException
   */
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.getRecursive()) {
      // Do not cache results of recursive list status,
      // because some results might be cached multiple times.
      // Otherwise, needs more complicated logic inside the cache,
      // that might not worth the effort of caching.
      return mFileSystem.listStatus(path, options);
    }

    List<URIStatus> statuses = mMetadataCache.listStatus(path);
    if (statuses == null) {
      statuses = mFileSystem.listStatus(path, options);
      mMetadataCache.put(path, statuses);
    }
    return statuses;
  }

  /**
   * Asynchronously update file's last access time.
   *
   * @param path the path to the file
   */
  @VisibleForTesting
  public void asyncUpdateFileAccessTime(AlluxioURI path) {
    try {
      mAccessTimeUpdater.submit(() -> {
        try {
          AlluxioConfiguration conf = mFsContext.getPathConf(path);
          GetStatusPOptions getStatusOptions = FileSystemOptions.getStatusDefaults(conf).toBuilder()
              .setAccessMode(Bits.READ)
              .setUpdateTimestamps(true)
              .build();
          mFileSystem.getStatus(path, getStatusOptions);
        } catch (IOException | AlluxioException e) {
          LOG.error("Failed to update access time for " + path, e);
        }
      });
    } catch (RejectedExecutionException e) {
      LOG.warn("Failed to submit a task to update access time for {}: {}", path, e.toString());
    }
  }

  @Override
  public void close() {
    ThreadUtils.shutdownAndAwaitTermination(mAccessTimeUpdater, THREAD_TERMINATION_TIMEOUT_MS);
  }
}
