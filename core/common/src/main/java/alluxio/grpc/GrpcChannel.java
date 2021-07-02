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

import alluxio.security.authentication.AuthenticatedChannelClientDriver;

import com.google.common.base.MoreObjects;
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

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An authenticated gRPC channel. This channel can communicate with servers of type
 * {@link GrpcServer}.
 */
@NotThreadSafe
public final class GrpcChannel extends Channel {
  private final GrpcConnection mConnection;

  /** The channel. */
  private Channel mTrackedChannel;
  /** Interceptor for tracking responses on the channel. */
  private ChannelResponseTracker mResponseTracker;

  /** Client-side authentication driver. */
  private AuthenticatedChannelClientDriver mAuthDriver;

  /** Used to prevent double release of the channel. */
  private boolean mChannelReleased = false;

  /**
   * Create a new instance of {@link GrpcChannel}.
   *
   * @param connection the grpc connection
   * @param authDriver nullable client-side authentication driver
   */
  public GrpcChannel(GrpcConnection connection,
      @Nullable AuthenticatedChannelClientDriver authDriver) {
    mConnection = connection;
    mAuthDriver = authDriver;

    // Intercept with response tracking interceptor for monitoring call health.
    mResponseTracker = new ChannelResponseTracker();
    mConnection.interceptChannel(mResponseTracker);
    mTrackedChannel = mConnection.getChannel();
  }

  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    return mTrackedChannel.newCall(methodDescriptor, callOptions);
  }

  @Override
  public String authority() {
    return mTrackedChannel.authority();
  }

  /**
   * Intercepts the channel with given interceptor.
   * @param interceptor interceptor
   */
  public void intercept(ClientInterceptor interceptor) {
    mTrackedChannel = ClientInterceptors.intercept(mTrackedChannel, interceptor);
  }

  /**
   * Shuts down the channel.
   *
   * Shutdown should be synchronized as it could be called concurrently due to:
   *  - Authentication long polling
   *  - gRPC messaging stream.
   */
  public synchronized void shutdown() {
    if (mAuthDriver != null) {
      // Close authenticated session with server.
      mAuthDriver.close();
      mAuthDriver = null;
    }
    if (!mChannelReleased) {
      try {
        mConnection.close();
      } catch (Exception e) {
        // TODO(ggezer): Don't throw once stabilized, just trace.
        throw new RuntimeException("Failed to release the connection.", e);
      }
      mChannelReleased = true;
    }
  }

  /**
   * @return {@code true} if the channel has been shut down
   */
  public boolean isShutdown() {
    return mChannelReleased;
  }

  /**
   * @return {@code true} if channel is healthy
   */
  public boolean isHealthy() {
    boolean healthy = mResponseTracker.isChannelHealthy();
    if (mAuthDriver != null) {
      healthy &= mAuthDriver.isAuthenticated();
    }
    return healthy;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ChannelKey", mConnection.getChannelKey())
        .add("ChannelHealthy", isHealthy())
        .add("ChannelReleased", mChannelReleased)
        .toString();
  }

  /**
   * @return a short identifier for the channel
   */
  public String toStringShort() {
    return mConnection.getChannelKey().toStringShort();
  }

  /**
   * An interceptor that is used to track server calls and invalidate the channel status. Upon
   * receiving Unauthenticated or Unavailable code from the server it invalidates the channel by
   * marking it unhealthy for channel owner to be able to detect and re-authenticate or re-create
   * the channel.
   */
  private class ChannelResponseTracker implements ClientInterceptor {
    private boolean mChannelHealthy = true;

    public boolean isChannelHealthy() {
      return mChannelHealthy;
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
                mChannelHealthy = false;
              }
              super.onClose(status, trailers);
            }
          }, headers);
        }
      };
    }
  }
}
