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
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * A block worker mock for testing.
 */
public class NoopBlockWorker implements BlockWorker {

  @Override
  public AtomicReference<Long> getWorkerId() {
    return null;
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    // noop
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    // noop
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    // noop
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) throws IOException {
    // noop
  }

  @Override
  public String createBlock(long sessionId, long blockId, String tierAlias, String medium,
      long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    return null;
  }

  @Override
  public void createBlockRemote(long sessionId, long blockId, String tierAlias, String medium,
      long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    // noop
  }

  @Nullable
  @Override
  public TempBlockMeta getTempBlockMeta(long sessionId, long blockId) {
    return null;
  }

  @Override
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
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
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return null;
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return null;
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return false;
  }

  @Override
  public long lockBlock(long sessionId, long blockId) {
    return 0;
  }

  @Override
  public void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // noop
  }

  @Override
  public void moveBlockToMedium(long sessionId, long blockId, String mediumType)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // noop
  }

  @Override
  public String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return null;
  }

  @Override
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return null;
  }

  @Override
  public BlockReader readUfsBlock(long sessionId, long blockId, long offset, boolean positionShort)
      throws BlockDoesNotExistException, IOException {
    return null;
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    // noop
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    // noop
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    // noop
  }

  @Override
  public boolean unlockBlock(long sessionId, long blockId) {
    return false;
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
  public boolean openUfsBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    return false;
  }

  @Override
  public void closeUfsBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, IOException, WorkerOutOfSpaceException {
    // noop
  }

  @Override
  public void clearMetrics() {
    // noop
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
