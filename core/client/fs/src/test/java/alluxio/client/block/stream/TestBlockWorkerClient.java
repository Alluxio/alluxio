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

package alluxio.client.block.stream;

import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.resource.LockBlockResource;
import alluxio.retry.RetryPolicy;
import alluxio.wire.WorkerNetAddress;

/**
 * A mock {@link BlockWorkerClient} which returns local host for the data server address.
 */
public class TestBlockWorkerClient implements BlockWorkerClient {
  private static final String HOSTNAME = "localhost";
  private static final int PORT = 29998;

  @Override
  public void accessBlock(long blockId) {}

  @Override
  public void cacheBlock(long blockId) {}

  @Override
  public void cancelBlock(long blockId) {}

  @Override
  public long getSessionId() {
    return 0;
  }

  @Override
  public WorkerNetAddress getWorkerNetAddress() {
    return new WorkerNetAddress().setHost(HOSTNAME).setDataPort(PORT);
  }

  @Override
  public LockBlockResource lockBlock(long blockId, LockBlockOptions options) {
    return null;
  }

  @Override
  public LockBlockResource lockUfsBlock(long blockId, LockBlockOptions options) {
    return null;
  }

  @Override
  public boolean promoteBlock(long blockId) {
    return false;
  }

  @Override
  public void removeBlock(long blockId) {}

  @Override
  public String requestBlockLocation(long blockId, long initialBytes, int tier) {
    return null;
  }

  @Override
  public boolean requestSpace(long blockId, long requestBytes) {
    return false;
  }

  @Override
  public boolean unlockBlock(long blockId) {
    return false;
  }

  @Override
  public void sessionHeartbeat(RetryPolicy retryPolicy) {}

  @Override
  public void close() {}
}
