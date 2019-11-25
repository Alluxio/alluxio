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
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cache for metadata of files.
 */
public final class MetadataCache {
  private final BaseFileSystem mFs;
  private final Cache<String, URIStatus> mCache;

  /**
   * @param fs the fs client
   * @param maxSize the max size of the cache
   * @param expirationTimeMs the expiration time (in milliseconds) of the cached item
   */
  public MetadataCache(BaseFileSystem fs, int maxSize, long expirationTimeMs) {
    mFs = fs;
    mCache = CacheBuilder.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(expirationTimeMs, TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * If file status is cached, return the cached status.
   * Otherwise, issue an RPC to master to get and cache the status.
   *
   * @param file the file
   * @param options the options
   * @return the file status
   */
  public URIStatus getStatus(AlluxioURI file, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    try {
      return mCache.get(file.getPath(), () -> mFs.getStatusThroughRPC(file, options));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof FileDoesNotExistException) {
        throw (FileDoesNotExistException) e.getCause();
      }
      if (e.getCause() instanceof AlluxioException) {
        throw (AlluxioException) e.getCause();
      }
      throw new IOException(e.getCause());
    }
  }

  /**
   * Issues an RPC to master to list the status of the directory, and cache the results.
   *
   * @param directory the directory
   * @param options the options
   * @return the list of statuses
   */
  public List<URIStatus> listStatus(AlluxioURI directory, ListStatusPOptions options)
      throws IOException, AlluxioException {
    List<URIStatus> statuses = mFs.listStatusThroughRPC(directory, options);
    for (URIStatus status : statuses) {
      mCache.put(status.getPath(), status);
    }
    return statuses;
  }
}
