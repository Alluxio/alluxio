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

package alluxio.worker;

import alluxio.Process;
import alluxio.conf.ServerConfiguration;
import alluxio.network.TieredIdentityFactory;
import alluxio.underfs.UfsManager;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A worker in the Alluxio system.
 */
public interface WorkerProcess extends Process {
  /**
   * Factory for creating {@link WorkerProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link WorkerProcess}
     */
    public static WorkerProcess create() {
      return create(TieredIdentityFactory.localIdentity(ServerConfiguration.global()));
    }

    /**
     * @param tieredIdentity tiered identity for the worker process
     * @return a new instance of {@link WorkerProcess}
     */
    public static WorkerProcess create(TieredIdentity tieredIdentity) {
      return new AlluxioWorkerProcess(tieredIdentity);
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @return the connect information for this worker
   */
  WorkerNetAddress getAddress();

  /**
   * @return the block worker for this Alluxio worker
   */
  UfsManager getUfsManager();

  /**
   * @return the worker's data service bind host (used by unit test only)
   */
  String getDataBindHost();

  /**
   * @return the worker's data service port (used by unit test only)
   */
  int getDataLocalPort();

  /**
   * @return the worker's data service domain socket path if available or "" if not available
   */
  String getDataDomainSocketPath();

  /**
   * @return this worker's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the worker web service bind host (used by unit test only)
   */
  String getWebBindHost();

  /**
   * @return the worker web service port (used by unit test only)
   */
  int getWebLocalPort();

  /**
   * @param clazz the class of the worker to get
   * @param <T> the type of the worker to get

   * @return the given worker
   */
  <T extends Worker> T getWorker(Class<T> clazz);
}
