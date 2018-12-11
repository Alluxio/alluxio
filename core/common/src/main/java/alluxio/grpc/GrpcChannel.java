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

package alluxio.grpc;

import alluxio.security.authentication.AuthType;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

import javax.security.auth.Subject;
import java.util.concurrent.TimeUnit;

/**
 * An authenticated gRPC channel.
 * This channel can communicate with servers of type {@link GrpcServer}.
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
    boolean terminated = false;
    while(!terminated) {
      try {
        terminated = mChannel.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
      }
    }
  }
}
