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
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * An authenticated gRPC channel. This channel can communicate with servers of type
 * {@link GrpcServer}.
 */
public final class GrpcChannel extends Channel {
  private final GrpcManagedChannelPool.ChannelKey mChannelKey;
  private final Channel mChannel;
  private boolean mChannelReleased;
  private boolean mChannelHealthy = true;

  /**
   * Create a new instance of {@link GrpcChannel}.
   *
   * @param channel the grpc channel to wrap
   */
  public GrpcChannel(GrpcManagedChannelPool.ChannelKey channelKey, Channel channel) {
    mChannelKey = channelKey;
    mChannel = ClientInterceptors.intercept(channel, new ChannelResponseTracker((this)));
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
    if(!mChannelReleased) {
      GrpcManagedChannelPool.INSTANCE().releaseManagedChannel(mChannelKey);
    }
    mChannelReleased = true;
  }

  /**
   * @return {@code true} if the channel has been shut down
   */
  public boolean isShutdown(){
    return mChannelReleased;
  }

  /**
   * @return {@code true} if channel is healthy
   */
  public boolean isHealthy() {
    return mChannelHealthy;
  }

  /**
   * An interceptor that is used to track server calls and invalidate the channel status. Upon
   * receiving Unauthenticated or Unavailable code from the server it invalidates the channel by
   * marking it unhealthy for channel owner to be able to detect and re-authenticate or re-create
   * the channel.
   */
  private class ChannelResponseTracker implements ClientInterceptor {
    private GrpcChannel mGrpcChannel;

    public ChannelResponseTracker(GrpcChannel grpcChannel) {
      mGrpcChannel = grpcChannel;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          // Put channel Id to headers.
          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
              responseListener) {
            @Override
            public void onClose(io.grpc.Status status, Metadata trailers) {
              if (status == Status.UNAUTHENTICATED || status == Status.UNAVAILABLE) {
                mGrpcChannel.mChannelHealthy = false;
              }
              super.onClose(status, trailers);
            }
          }, headers);
        }
      };
    }
  }
}
