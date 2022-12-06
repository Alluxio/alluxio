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

package alluxio.master.service;

/**
 * Defines a simple start/promote/demote/stop interface for interacting with simple Alluxio
 * master components such as the web server, metrics server, rpc server, etc...
 */
public interface SimpleService {
  /**
   * Starts the service. Leaves the service in {@link alluxio.grpc.NodeState#STANDBY} state.
   * Leaves the service in the same state as {@link #demote()}.
   * Can only be called once.
   */
  void start();

  /**
   * Promotes the service to {@link alluxio.grpc.NodeState#PRIMARY} state.
   * Can only be called on a started service (i.e. {@link #start()} must precede this method).
   * Can only be called in {@link alluxio.grpc.NodeState#STANDBY} state.
   * Can be called multiple times.
   */
  void promote();

  /**
   * Demotes the service back to {@link alluxio.grpc.NodeState#STANDBY} state.
   * Can only be called on a started service (i.e. {@link #start()} must precede this method).
   * Can only be called in {@link alluxio.grpc.NodeState#PRIMARY} state (i.e. {@link #promote()}
   * must precede this method).
   * Can be called multiple times.
   */
  void demote();

  /**
   * Stops the service altogether and cleans up any state left.
   * If {@link #start()} has not been called on this service this method should be a noop.
   * Can only be called once.
   */
  void stop();
}
