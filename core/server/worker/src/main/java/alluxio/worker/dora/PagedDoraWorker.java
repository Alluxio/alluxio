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

package alluxio.worker.dora;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.Server;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.FileId;
import alluxio.underfs.PagedUfsReader;
import alluxio.underfs.UfsInputStreamCache;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.DoraWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.page.UfsBlockReadOptions;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Page store based dora worker.
 */
public class PagedDoraWorker implements DoraWorker {
  // for now Dora Worker does not support Alluxio <-> UFS mapping,
  // and assumes all UFS paths belong to the same UFS.
  private static final int MOUNT_POINT = 1;
  private final Closer mResourceCloser = Closer.create();
  private final AtomicReference<Long> mWorkerId;
  private final CacheManager mCacheManager;
  private final DoraUfsManager mUfsManager;
  private final UfsInputStreamCache mUfsStreamCache;
  private final long mPageSize;
  private final AlluxioConfiguration mConf;

  /**
   * Constructor.
   * @param workerId
   */
  public PagedDoraWorker(AtomicReference<Long> workerId, AlluxioConfiguration conf) {
    mWorkerId = workerId;
    mConf = conf;
    mUfsManager = mResourceCloser.register(new DoraUfsManager());
    mUfsStreamCache = new UfsInputStreamCache();
    mPageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    try {
      mCacheManager = CacheManager.Factory.create(Configuration.global());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return Collections.emptySet();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public void start(WorkerNetAddress options) throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void close() throws IOException {
    mResourceCloser.close();
    try {
      mCacheManager.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  @Override
  public FileInfo getFileInfo(String fileId) throws IOException {
    return new FileInfo();
  }

  @Override
  public BlockReader createFileReader(String fileId, long offset, boolean positionShort,
      Protocol.OpenUfsBlockOptions options) throws IOException {
    UfsManager.UfsClient ufsClient;
    try {
      ufsClient = mUfsManager.get(MOUNT_POINT);
    } catch (NotFoundException e) {
      mUfsManager.addMount(MOUNT_POINT, new AlluxioURI(options.getUfsPath()),
          UnderFileSystemConfiguration.defaults(mConf));
      try {
        ufsClient = mUfsManager.get(MOUNT_POINT);
      } catch (NotFoundException e2) {
        throw new RuntimeException(
            String.format("Failed to get mount point for %s", options.getUfsPath()), e2);
      }
    }

    FileId id = FileId.of(fileId);
    final long fileSize = options.getBlockSize();
    return new PagedFileReader(mConf, mCacheManager,
        new PagedUfsReader(mConf, ufsClient, mUfsStreamCache, id,
            fileSize, offset, UfsBlockReadOptions.fromProto(options), mPageSize),
        id,
        fileSize,
        offset,
        /* posShort */ false,
        mPageSize,
        options);
  }

  @Override
  public void cleanupSession(long sessionId) {
  }
}
