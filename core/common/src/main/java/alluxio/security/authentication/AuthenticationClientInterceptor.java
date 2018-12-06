package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.AlluxioSaslClientServiceGrpc;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.UUID;

public class AuthenticationClientInterceptor implements ServerInterceptor {

  private AuthenticatedClientRegistry mClientRegistry;

  public AuthenticationClientInterceptor(AuthenticatedClientRegistry clientRegistry) {
    mClientRegistry = clientRegistry;
  }

  private boolean IsWhiteListed(String methodName) {
    return methodName.contains(AlluxioSaslClientServiceGrpc.SERVICE_NAME);
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
              String userName = mClientRegistry.getUserNameForClient(clientId);
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
