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
import alluxio.Constants;
import alluxio.client.file.DelegatingFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.util.logging.SamplingLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {
  private static final Logger SAMPLING_LOGGER = new SamplingLogger(
      LoggerFactory.getLogger(LocalCacheFileSystem.class), 30 * Constants.MINUTE_MS);
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
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    // TODO(calvin): We should add another API to reduce the cost of openFile
    try {
      return new LocalCacheFileInStream(path, options, mDelegatedFileSystem,
          mFsContext.getCacheManager());
    } catch (IOException e) {
      SAMPLING_LOGGER.warn("Failed to create LocalCacheFileInStream {}", e.getMessage());
      return mDelegatedFileSystem.openFile(path, options);
    }
  }
}
