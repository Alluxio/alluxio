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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.OpenFilePOptions;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {
  private static CacheManager sCacheManager;

  private final AlluxioConfiguration mConf;

  /**
   * @param fs a FileSystem instance to query on local cache miss
   * @param conf the configuration, only respected for the first call
   */
  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "write to static is made threadsafe")
  public LocalCacheFileSystem(FileSystem fs, AlluxioConfiguration conf) {
    super(fs);
    // TODO(feng): support multiple cache managers
    if (sCacheManager == null) {
      synchronized (LocalCacheFileSystem.class) {
        if (sCacheManager == null) {
          sCacheManager = CacheManager.create(conf);
        }
      }
    }
    mConf = conf;
  }

  @Override
  public AlluxioConfiguration getConf() {
    if (mConf.getBoolean(PropertyKey.USER_LOCAL_CACHE_LIBRARY)) {
      return mConf;
    }
    return mDelegatedFileSystem.getConf();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options) {
    // TODO(calvin): We should add another API to reduce the cost of openFile
    return new LocalCacheFileInStream(path, options, mDelegatedFileSystem, sCacheManager);
  }
}
