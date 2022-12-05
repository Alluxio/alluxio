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

import alluxio.Server;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.DoraWorker;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.io.BlockReader;

import com.google.common.io.Closer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Page store based dora worker.
 */
public class PagedDoraWorker implements DoraWorker {
  private final Closer mResourceCloser = Closer.create();
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final AtomicReference<Long> mWorkerId;

  /**
   * Constructor.
   * @param blockMasterClientPool
   * @param workerId
   */
  public PagedDoraWorker(BlockMasterClientPool blockMasterClientPool,
      AtomicReference<Long> workerId) {
    mBlockMasterClientPool = mResourceCloser.register(blockMasterClientPool);
    mWorkerId = workerId;
    mBlockMasterClientPool.acquire(); //todo: remove this line
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return null;
  }

  @Override
  public void start(WorkerNetAddress options) throws IOException {
  }

  @Override
  public void stop() throws IOException {
  }

  @Override
  public void close() throws IOException {
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
    return null;
  }

  @Override
  public void cleanupSession(long sessionId) {
  }
}
