/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.heartbeat.HeartbeatExecutor;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Session client sends periodical heartbeats to the worker it is talking to. If it fails to do so,
 * the worker may withdraw the space granted to the particular session.
 */
@ThreadSafe
final class BlockWorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  private final BlockWorkerClient mBlockWorkerClient;

  public BlockWorkerClientHeartbeatExecutor(BlockWorkerClient blockWorkerClient) {
    mBlockWorkerClient = Preconditions.checkNotNull(blockWorkerClient);
  }

  @Override
  public void heartbeat() {
    mBlockWorkerClient.periodicHeartbeat();
  }

  @Override
  public void close() {
    // Not responsible for cleaning up blockWorkerClient
  }
}
