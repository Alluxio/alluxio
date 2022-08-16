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

import alluxio.Server;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.Configuration;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A block worker mock for testing.
 */
public class NoopBlockWorker implements BlockWorker {

  @Override
  public AtomicReference<Long> getWorkerId() {
    return null;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws IOException {
    // noop
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
    // noop
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) {
    // noop
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions) {
    return null;
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return null;
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return null;
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return null;
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return null;
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    return null;
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws IOException {
    // noop
  }

  @Override
  public void removeAllBlocks()
          throws IOException {
    // noop
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes) {
    // noop
  }

  @Override
  public void asyncCache(AsyncCacheRequest request) {
    // noop
  }

  @Override
  public void cache(CacheRequest request) {
    // noop
  }

  @Override
  public CompletableFuture<List<BlockStatus>> load(List<Block> blocks, String tag,
      OptionalLong bandwidth) {
    return null;
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    // noop
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return null;
  }

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    return null;
  }

  @Override
  public void clearMetrics() {
    // noop
  }

  @Override
  public Configuration getConfiguration(GetConfigurationPOptions options) {
    return null;
  }

  @Override
  public List<String> getWhiteList() {
    return null;
  }

  @Override
  public BlockStore getBlockStore() {
    throw new UnsupportedOperationException();
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
    // noop
  }

  @Override
  public void stop() throws IOException {
    // noop
  }

  @Override
  public void close() throws IOException {
    // noop
  }

  @Override
  public void cleanupSession(long sessionId) {
    // noop
  }
}
