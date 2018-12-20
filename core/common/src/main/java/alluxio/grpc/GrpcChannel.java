/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;

/**
 * An authenticated gRPC channel. This channel can communicate with servers of type
 * {@link GrpcServer}.
 */
public final class GrpcChannel extends Channel {
  private final GrpcManagedChannelPool.ChannelKey mChannelKey;
  private final Channel mChannel;
  private boolean mChannelReleased;

  /**
   * Create a new instance of {@link GrpcChannel}.
   *
   * @param channel the grpc channel to wrap
   */
  public GrpcChannel(GrpcManagedChannelPool.ChannelKey channelKey, Channel channel) {
    mChannelKey = channelKey;
    mChannel = channel;
    mChannelReleased = false;
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
   * Shuts down the channel.
   */
  public void shutdown() {
    GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(mChannelKey);
    mChannelReleased = true;
  }

  /**
   * @return {@code true} if the channel has been shut down
   */
  public boolean isShutdown(){
    return mChannelReleased;
  }
}
