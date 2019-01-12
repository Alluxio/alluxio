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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.interfaces.Scoped;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * Interface for a class which can determine whether the local master is the primary.
 */
public interface PrimarySelector {

  /**
   * The state for the primary selector.
   */
  enum State {
    /** The current process is primary. */
    PRIMARY,
    /** The current process is secondary. */
    SECONDARY,
  }

  /**
   * Factory for creating primary selectors.
   */
  final class Factory {
    /**
     * @return a primary selector based on zookeeper configuration
     */
    public static PrimarySelector createZkPrimarySelector() {
      String zkAddress = ServerConfiguration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      String zkElectionPath = ServerConfiguration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH);
      String zkLeaderPath = ServerConfiguration.get(PropertyKey.ZOOKEEPER_LEADER_PATH);
      return new PrimarySelectorClient(zkAddress, zkElectionPath, zkLeaderPath);
    }

    /**
     * @return a job master primary selector based on zookeeper configuration
     */
    public static PrimarySelector createZkJobPrimarySelector() {
      String zkAddress = ServerConfiguration.get(PropertyKey.ZOOKEEPER_ADDRESS);
      String zkElectionPath = ServerConfiguration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH);
      String zkLeaderPath = ServerConfiguration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH);
      return new PrimarySelectorClient(zkAddress, zkElectionPath, zkLeaderPath);
    }

    private Factory() {} // Not intended for instantiation.
  }

  /**
   * Starts the primary selector.
   *
   * @param localAddress the address of the local master
   */
  void start(InetSocketAddress localAddress) throws IOException;

  /**
   * Stops the primary selector.
   */
  void stop() throws IOException;

  /**
   * @return the current state
   */
  State getState();

  /**
   * Registers a listener to be executed whenever the selector's state updates.
   *
   * The listener will be executed synchronously in the state update thread, so it should run
   * quickly.
   *
   * @param listener the listener
   * @return an object which will unregister the listener when closed
   */
  Scoped onStateChange(Consumer<State> listener);

  /**
   * Blocks until the primary selector enters the specified state.
   *
   * @param state the state to wait for
   */
  void waitForState(State state) throws InterruptedException;
}
