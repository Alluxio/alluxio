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

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Server;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A master in the Alluxio system.
 */
public interface AlluxioMasterService extends Server {
  /**
   * Factory for creating {@link AlluxioMasterService}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link AlluxioMasterService}
     */
    public static AlluxioMasterService create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioMaster();
      }
      return new DefaultAlluxioMaster();
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get

   * @return the given master
   */
  <T> T getMaster(Class<T> clazz);

  /**
   * @return this master's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the master in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();

  /**
   * Waits until the master is ready to server requests.
   */
  void waitForReady();
}
