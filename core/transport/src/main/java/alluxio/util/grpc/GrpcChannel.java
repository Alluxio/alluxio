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

package alluxio.util.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

import java.util.concurrent.TimeUnit;

/**
 * A simple wrapper around the {@link Channel} class in grpc. Outside of this module, this
 * class should be used to replace references to {@link Channel} for dependency management.
 * Note: This class is intended for internal use only.
 */
public final class GrpcChannel extends Channel {
  ManagedChannel mChannel;

  /**
   * Create a new instance of {@link GrpcChannel}.
   *
   * @param channel the grpc channel to wrap
   */
  public GrpcChannel(ManagedChannel channel) {
    mChannel = channel;
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    return mChannel.newCall(methodDescriptor, callOptions);
  }

  @Override
  public String authority() {
    return mChannel.authority();
  }

  /**
   * Shuts down channel immediately.
   */
  public void shutdown() {
    mChannel.shutdownNow();
  }

  /**
   * Wait for channel to complete shutdown.
   * @param timeout maximum time to wait before return
   * @param unit unit of the timeout
   * @return whether channel is terminated
   */
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return mChannel.awaitTermination(timeout, unit);
  }

  /**
   * Return the state of the connection.
   * @param requestConnection if set to true, the channel will establish a new connection if it is
   *                          not currently connected
   * @return the state of the connection
   */
  public ConnectivityState getState(boolean requestConnection) {
    return mChannel.getState(requestConnection);
  }
}
