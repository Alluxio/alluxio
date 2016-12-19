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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.network.connection.ThriftClientPool;
import alluxio.thrift.BlockWorkerClientService;

import org.apache.thrift.protocol.TProtocol;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Class for managing a pool of {@link BlockWorkerClientService.Client}.
 */
@ThreadSafe
public final class BlockWorkerThriftClientPool
    extends ThriftClientPool<BlockWorkerClientService.Client> {
  /**
   * Creates an instance of {@link BlockWorkerThriftClientPool}.
   *
   * @param subject the parent subject, set to null if not present
   * @param address the worke address
   * @param maxCapacity the maximum capacity of the pool
   * @param gcThresholdMs the threshold to GC clients in the pool
   */
  public BlockWorkerThriftClientPool(Subject subject, InetSocketAddress address, int maxCapacity,
      long gcThresholdMs) {
    super(subject, Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME,
        Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION, address, maxCapacity, gcThresholdMs);
  }

  @Override
  protected BlockWorkerClientService.Client createThriftClient(TProtocol protocol) {
    return new BlockWorkerClientService.Client(protocol);
  }

  @Override
  protected String getServiceNameForLogging() {
    return "blockWorker";
  }
}
