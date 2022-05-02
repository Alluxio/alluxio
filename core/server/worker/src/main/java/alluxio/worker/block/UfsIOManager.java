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

package alluxio.worker.block;

import alluxio.AlluxioURI;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Control UFS IO.
 */
public class UfsIOManager {
  private final ConcurrentMap<BytesReadMetricKey, Counter>
      mUfsBytesReadMetrics =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();
  private final UfsManager.UfsClient mUfsClient;
  private final UfsInputStreamCache mUfsInstreamCache;

  public UfsIOManager(UfsManager.UfsClient ufsClient) {
    mUfsClient = ufsClient;
    mUfsInstreamCache = new UfsInputStreamCache();
  }

  public ByteBuffer read(UnderFileSystemBlockMeta blockMeta, long offset, long length,
      boolean positionShort, Protocol.OpenUfsBlockOptions options) throws IOException {
    UnderFileSystem ufs = mUfsClient.acquireUfsResource().get();
    InputStream inStream = mUfsInstreamCache.acquire(ufs, blockMeta.getUnderFileSystemPath(),
        IdUtils.fileIdFromBlockId(blockMeta.getBlockId()),
        OpenOptions.defaults().setOffset(blockMeta.getOffset() + offset)
            .setPositionShort(positionShort));
    long bytesToRead = length;
    if (bytesToRead <= 0) {
      return ByteBuffer.allocate(0);
    }
    byte[] data = new byte[(int) bytesToRead];
    int bytesRead = 0;
    Preconditions.checkNotNull(inStream, "inStream");
    while (bytesRead < bytesToRead) {
      int read;
      try {
        read = inStream.read(data, bytesRead, (int) (bytesToRead - bytesRead));
      } catch (IOException e) {
        throw AlluxioStatusException.fromIOException(e);
      }
      if (read == -1) {
        break;
      }
      bytesRead += read;
    }
    mUfsInstreamCache.release(inStream);
    Counter ufsBytesRead = mUfsBytesReadMetrics.computeIfAbsent(
        new BytesReadMetricKey(mUfsClient.getUfsMountPointUri(), options.getUser()),
        key -> key.getUser() == null
            ? MetricsSystem.counterWithTags(
            MetricKey.WORKER_BYTES_READ_UFS.getName(),
            MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(),
            MetricInfo.TAG_UFS, MetricsSystem.escape(key.getUri()))
            : MetricsSystem.counterWithTags(
            MetricKey.WORKER_BYTES_READ_UFS.getName(),
            MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(),
            MetricInfo.TAG_UFS, MetricsSystem.escape(key.getUri()),
            MetricInfo.TAG_USER, key.getUser()));
    Meter ufsBytesReadThroughput = mUfsBytesReadThroughputMetrics.computeIfAbsent(
        mUfsClient.getUfsMountPointUri(),
        uri -> MetricsSystem.meterWithTags(
            MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
            MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(),
            MetricInfo.TAG_UFS,
            MetricsSystem.escape(uri)));

    ufsBytesRead.inc(bytesRead);
    ufsBytesReadThroughput.mark(bytesRead);
    return ByteBuffer.wrap(data, 0, bytesRead);
  }

}
