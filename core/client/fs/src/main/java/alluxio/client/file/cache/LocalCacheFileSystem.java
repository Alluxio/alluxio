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
import alluxio.client.file.DelegatingFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.OpenFilePOptions;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {
  private final FileSystemContext mFsContext;

  /**
   * @param fs a FileSystem instance to query on local cache miss
   * @param fsContext file system context
   */
  public LocalCacheFileSystem(FileSystem fs, FileSystemContext fsContext) {
    super(fs);
    mFsContext = fsContext;
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) {
    // TODO(calvin): We should add another API to reduce the cost of openFile
    return new LocalCacheFileInStream(path, options, mDelegatedFileSystem,
        mFsContext.getCacheManager());
  }
}
