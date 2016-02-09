/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
