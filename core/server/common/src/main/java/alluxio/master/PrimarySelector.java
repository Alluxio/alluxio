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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Interface for a class which can determine whether the local master is the primary.
 */
public interface PrimarySelector {

  /**
   * Factory for creating primary selectors.
   */
  final class Factory {
    /**
     * @return a primary selector based on zookeeper configuration
     */
    public static PrimarySelector createZkPrimarySelector() {
      String zkAddress = Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      String zkElectionPath = Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH);
      return new PrimarySelectorClient(zkAddress, zkElectionPath, zkLeaderPath);
    }

    private Factory() {} // Not intended for instantiation.
  }

  /**
   * Starts the primary selector. The primary selector must be started before {@link #isPrimary()}
   * may be queried.
   *
   * @param localAddress the address of the local master
   */
  void start(InetSocketAddress localAddress) throws IOException;

  /**
   * Stops the primary selector.
   */
  void stop() throws IOException;

  /**
   * @return whether the local master is the primary
   */
  boolean isPrimary();
}
