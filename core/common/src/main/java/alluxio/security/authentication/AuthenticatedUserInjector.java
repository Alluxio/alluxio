package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioVersionServiceGrpc;
import alluxio.grpc.SaslAuthenticationServiceGrpc;

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

  public AuthenticatedUserInjector(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  /**
   * PS: Authentication and Versioning services don't require channel to be authenticated.
   *
   * @param methodName name of the RPC method being intercepted.
   * @return true if the method that is being called need to be intercepted.
   */
  private boolean shouldIntercept(String methodName) {
    return !(methodName.startsWith(SaslAuthenticationServiceGrpc.SERVICE_NAME)
        || methodName.startsWith(AlluxioVersionServiceGrpc.SERVICE_NAME));
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (!shouldIntercept(call.getMethodDescriptor().getFullMethodName())) {
      return next.startCall(call, headers);
    } else {
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
}
