package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import alluxio.grpc.AlluxioVersionServiceGrpc;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.UUID;

public class AuthenticatedUserInjector implements ServerInterceptor {

  private AuthenticationServer mAuthenticationServer;

  public AuthenticatedUserInjector(AuthenticationServer authenticationServer) {
    mAuthenticationServer = authenticationServer;
  }

  private boolean IsWhiteListed(String methodName) {
    return methodName.contains(AlluxioSaslClientServiceGrpc.SERVICE_NAME)
        || methodName.contains(AlluxioVersionServiceGrpc.SERVICE_NAME);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (IsWhiteListed(call.getMethodDescriptor().getFullMethodName())) {
      return next.startCall(call, headers);
    } else {
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
          next.startCall(call, headers)) {
        @Override
        public void onHalfClose() {
          UUID clientId = headers.get(ClientIdInjector.sClientIdKey);
          if (clientId != null) {
            try {
              String userName = mAuthenticationServer.getUserNameForClient(clientId);
              AuthenticatedClientUser.set(userName);
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
