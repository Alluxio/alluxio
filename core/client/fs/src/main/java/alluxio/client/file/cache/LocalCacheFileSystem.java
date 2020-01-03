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
import alluxio.grpc.OpenFilePOptions;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {

  private final LocalCacheManager mLocalCacheManager;

  /**
   * @param fs a FileSystem instance to query on local cache miss
   */
  public LocalCacheFileSystem(FileSystem fs) {
    super(fs);
    // needs to be moved outside FileSystem constructor
    mLocalCacheManager = new LocalCacheManager();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) {
    return new LocalCacheFileInStream(path, options, mDelegatedFileSystem, mLocalCacheManager);
  }
}
