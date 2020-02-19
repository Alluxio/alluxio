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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.grpc.OpenFilePOptions;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link FileInStream} that reads from a local cache if possible.
 */
@NotThreadSafe
public class DryRunLocalCacheFileInStream extends LocalCacheFileInStream {

  /**
   * Constructor when only path information is available.
   *
   * @param path path of the file
   * @param options read options
   * @param externalFs the external file system if a cache miss occurs
   * @param cacheManager local cache manager
   */
  public DryRunLocalCacheFileInStream(AlluxioURI path, OpenFilePOptions options,
      FileSystem externalFs, CacheManager cacheManager) {
    super(path, options, externalFs, cacheManager);
  }

  /**
   * Constructor when the {@link URIStatus} is already available.
   *
   * @param status file status
   * @param options read options
   * @param externalFs the external file system if a cache miss occurs
   * @param cacheManager local cache manager
   */
  public DryRunLocalCacheFileInStream(URIStatus status, OpenFilePOptions options,
      FileSystem externalFs, CacheManager cacheManager) {
    super(status, options, externalFs, cacheManager);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = getExternalFileInStream(getPos()).read(b, off, len);
    super.read(b, off, len);
    return bytesRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    int bytesRead = getExternalFileInStream(getPos()).positionedRead(pos, b, off, len);
    super.positionedRead(pos, b, off, len);
    return bytesRead;
  }

  @Override
  protected void readPage(byte[] buffer, int offset, ReadableByteChannel page, int bytesLeft) {
    // read nothing in dry run mode
  }

  @Override
  protected void copyPage(byte[] page, int pageOffset, byte[] buffer, int bufferOffset,
      int length) {
    // read nothing in dry run mode
  }

  @Override
  protected void readExternalPage(long pageStart, int pageSize, byte[] buffer) {
    // read nothing in dry run mode
  }
}
