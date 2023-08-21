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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.NodeState;
import alluxio.master.journal.ufs.UfsJournalMultiMasterPrimarySelector;
import alluxio.util.interfaces.Scoped;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * Interface for a class which can determine whether the local master is the primary.
 */
public interface PrimarySelector {
  /**
   * Starts the primary selector.
   *
   * @param localAddress the address of the local master
   */
  void start(InetSocketAddress localAddress);

  /**
   * Stops the primary selector.
   */
  void stop();

  /**
   * @return the current state
   */
  NodeState getState();

  /**
   * @return the current state without the lock acquired
   */
  NodeState getStateUnsafe();

  /**
   * Registers a listener to be executed whenever the selector's state updates.
   *
   * The listener will be executed synchronously in the state update thread, so it should run
   * quickly.
   *
   * @param listener the listener
   * @return an object which will unregister the listener when closed
   */
  Scoped onStateChange(Consumer<NodeState> listener);

  /**
   * Blocks until the primary selector enters the specified state.
   *
   * @param state the state to wait for
   */
  void waitForState(NodeState state) throws InterruptedException;
}
