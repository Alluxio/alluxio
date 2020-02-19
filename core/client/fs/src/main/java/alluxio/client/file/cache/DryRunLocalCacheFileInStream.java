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
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.io.BufferUtils;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.io.Closer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of {@link FileInStream} that reads from a local cache if possible.
 */
@NotThreadSafe
public class DryRunLocalCacheFileInStream extends LocalCacheFileInStream {
  public DryRunLocalCacheFileInStream(AlluxioURI path, OpenFilePOptions options,
      FileSystem externalFs, CacheManager cacheManager) {
    super(path, options, externalFs, cacheManager);
  }

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
