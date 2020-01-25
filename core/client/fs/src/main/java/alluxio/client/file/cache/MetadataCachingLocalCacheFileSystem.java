/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.MetadataCacheManager;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.List;

/**
 * FileSystem implementation with the capability of caching metadata of paths.
 */
public class MetadataCachingLocalCacheFileSystem extends LocalCacheFileSystem {
  private final MetadataCacheManager mMetadataCacheManager;
  private final FileSystemContext mFsContext;
  private final Closer mCloser;

  /**
   * @param fs delegated file system
   * @param context the fs context
   */
  public MetadataCachingLocalCacheFileSystem(FileSystem fs, FileSystemContext context) {
    super(fs);
    mCloser = Closer.create();
    mFsContext = context;
    mMetadataCacheManager = new MetadataCacheManager(fs, context);
    mCloser.register(fs);
    mCloser.register(mMetadataCacheManager);
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemUtils.checkUri(mFsContext, path);
    return mMetadataCacheManager.getStatus(path, options);
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemUtils.checkUri(mFsContext, path);
    return mMetadataCacheManager.listStatus(path, options);
  }

  @Override
  public synchronized void close() throws IOException {
    mCloser.close();
  }
}
