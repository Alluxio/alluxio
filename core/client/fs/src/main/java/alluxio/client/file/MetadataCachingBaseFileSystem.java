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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * FileSystem implementation with the capability of caching metadata of paths.
 */
@ThreadSafe
public class MetadataCachingBaseFileSystem extends BaseFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);
  private static final int THREAD_KEEPALIVE_SECOND = 60;
  private static final int THREAD_TERMINATION_TIMEOUT_MS = 10000;

  private final MetadataCache mMetadataCache;
  private final ExecutorService mAccessTimeUpdater;

  /**
   * @param context the fs context
   */
  public MetadataCachingBaseFileSystem(FileSystemContext context) {
    super(context);

    int maxSize = mFsContext.getClusterConf().getInt(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
    long expirationTimeMs = mFsContext.getClusterConf()
        .getMs(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME);
    mMetadataCache = new MetadataCache(maxSize, expirationTimeMs);
    int masterClientThreads = mFsContext.getClusterConf()
        .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);
    // At a time point, there are at most the same number of concurrent master clients that
    // asynchronously update access time.
    mAccessTimeUpdater = new ThreadPoolExecutor(0, masterClientThreads, THREAD_KEEPALIVE_SECOND,
        TimeUnit.SECONDS, new SynchronousQueue<>());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    URIStatus status = mMetadataCache.get(path);
    if (status == null) {
      status = super.getStatus(path, options);
      mMetadataCache.put(path, status);
    } else if (options.getUpdateTimestamps()) {
      // Asynchronously send an RPC to master to update the access time.
      // Otherwise, if we need to synchronously send RPC to master to do this,
      // caching the status does not bring any benefit.
      asyncUpdateFileAccessTime(path);
    }
    return status;
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    List<URIStatus> statuses = super.listStatus(path, options);
    for (URIStatus status : statuses) {
      mMetadataCache.put(status.getPath(), status);
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
    mAccessTimeUpdater.submit(() -> {
      try {
        AlluxioConfiguration conf = mFsContext.getPathConf(path);
        GetStatusPOptions getStatusOptions = FileSystemOptions.getStatusDefaults(conf).toBuilder()
            .setAccessMode(Bits.READ)
            .setUpdateTimestamps(true)
            .build();
        super.getStatus(path, getStatusOptions);
      } catch (IOException | AlluxioException e) {
        LOG.error("Failed to update access time for file " + path, e);
      }
    });
  }

  @Override
  public synchronized void close() throws IOException {
    if (!mClosed) {
      ThreadUtils.shutdownAndAwaitTermination(mAccessTimeUpdater, THREAD_TERMINATION_TIMEOUT_MS);
      super.close();
    }
  }
}
