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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.UUID;

/**
 * Server side interceptor for setting authenticated user in {@link AuthenticatedClientUser}. This
 * interceptor requires {@link ChannelIdInjector} to have injected the channel id from which the
 * particular RPC is being made.
 */
@ThreadSafe
public final class AuthenticatedUserInjector implements ServerInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedUserInjector.class);

  private final AuthenticationServer mAuthenticationServer;

  /**
   * Creates {@link AuthenticationServer} with given authentication server.
   *
   * @param authenticationServer the authentication server
   */
  public AuthenticatedUserInjector(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    /**
     * For streaming calls, below will make sure authenticated user is injected prior to creating
     * the stream. If the call gets closed during authentication, the listener we return below
     * will not continue.
     */
    authenticateCall(call, headers);

    /**
     * For non-streaming calls to server, below listener will be invoked in the same thread that is
     * serving the call.
     */
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {
      @Override
      public void onHalfClose() {
        if (authenticateCall(call, headers)) {
          super.onHalfClose();
        }
      }
    };
  }

  /**
   * Authenticates given call against auth-server state.
   * Fails the call if it's not originating from an authenticated client channel.
   * It sets thread-local authentication information for the call with the user information
   * that is kept on auth-server.
   *
   * @return {@code true} if call was authenticated successfully
   */
  private <ReqT, RespT> boolean authenticateCall(ServerCall<ReqT, RespT> call, Metadata headers) {
    // Fail validation for cancelled server calls.
    if (call.isCancelled()) {
      LOG.debug("Server call has been cancelled: {}",
          call.getMethodDescriptor().getFullMethodName());
      return false;
    }

    // Try to fetch channel Id from the metadata.
    UUID channelId = headers.get(ChannelIdInjector.S_CLIENT_ID_KEY);
    boolean callAuthenticated = false;
    if (channelId != null) {
      try {
        // Fetch authenticated username for this channel and set it.
        AuthenticatedUserInfo userInfo = mAuthenticationServer.getUserInfoForChannel(channelId);
        LOG.debug("Acquiring credentials for service-method: {} on channel: {}",
            call.getMethodDescriptor().getFullMethodName(), channelId);
        if (userInfo != null) {
          AuthenticatedClientUser.set(userInfo.getAuthorizedUserName());
          AuthenticatedClientUser.setConnectionUser(userInfo.getConnectionUserName());
          AuthenticatedClientUser.setAuthMethod(userInfo.getAuthMethod());
        } else {
          AuthenticatedClientUser.remove();
        }
        callAuthenticated = true;
      } catch (UnauthenticatedException e) {
        String message = String.format("Channel: %s is not authenticated for call: %s",
            channelId.toString(), call.getMethodDescriptor().getFullMethodName());
        closeQuietly(call, Status.UNAUTHENTICATED.withDescription(message), headers);
      }
    } else {
      String message = String.format("Channel Id is missing for call: %s.",
          call.getMethodDescriptor().getFullMethodName());
      closeQuietly(call, Status.UNAUTHENTICATED.withDescription(message), headers);
    }
    return callAuthenticated;
  }

  /**
   * Closes the call while blanketing runtime exceptions. This is mostly to avoid dumping "already
   * closed" exceptions to logs.
   *
   * @param call call to close
   * @param status status to close the call with
   * @param headers headers to close the call with
   */
  private <ReqT, RespT> void closeQuietly(ServerCall<ReqT, RespT> call, Status status,
      Metadata headers) {
    try {
      LOG.debug("Closing the call:{} with Status:{}",
          call.getMethodDescriptor().getFullMethodName(), status);
      call.close(status, headers);
    } catch (RuntimeException exc) {
      LOG.debug("Error while closing the call:{} with Status:{}: {}",
          call.getMethodDescriptor().getFullMethodName(), status, exc);
    }
  }
}
