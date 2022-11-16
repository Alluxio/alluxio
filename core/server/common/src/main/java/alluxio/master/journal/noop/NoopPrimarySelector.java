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

package alluxio.master.journal.noop;

import alluxio.grpc.NodeState;
import alluxio.master.PrimarySelector;
import alluxio.util.interfaces.Scoped;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

/**
 * Noop primary selector.
 */
public class NoopPrimarySelector implements PrimarySelector {
  @Override
  public void start(InetSocketAddress localAddress) {}

  @Override
  public void stop() {}

  @Override
  public NodeState getState() {
    return NodeState.STANDBY;
  }

  @Override
  public NodeState getStateUnsafe() {
    return NodeState.STANDBY;
  }

  @Override
  public Scoped onStateChange(Consumer<NodeState> listener) {
    return () -> { };
  }

  @Override
  public void waitForState(NodeState state) {}
}
