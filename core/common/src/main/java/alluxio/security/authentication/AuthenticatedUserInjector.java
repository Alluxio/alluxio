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

package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.UUID;

/**
 * Server side interceptor for injecting authenticated user into TLS. This interceptor requires
 * {@link ChannelIdInjector} to have been injected the channel id from which the particular RPC is
 * being made.
 */
public class AuthenticatedUserInjector implements ServerInterceptor {

  private AuthenticationServer mAuthenticationServer;

  /**
   * Creates {@link AuthenticationServer} with given authentication server.
   * @param authenticationServer the authentication server
   */
  public AuthenticatedUserInjector(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {
      /**
       * onHalfClose is called on the same thread that calls the service handler.
       */
      @Override
      public void onHalfClose() {
        // Try to fetch channel Id from the metadata.
        UUID channelId = headers.get(ChannelIdInjector.sClientIdKey);
        if (channelId != null) {
          try {
            // Fetch authenticated username for this channel and set it.
            String userName = mAuthenticationServer.getUserNameForChannel(channelId);
            if (userName != null) {
              AuthenticatedClientUser.set(userName);
            }
          } catch (UnauthenticatedException e) {
            call.close(Status.UNAUTHENTICATED, headers);
          }
        }
        super.onHalfClose();
      }
    };
  }
}
