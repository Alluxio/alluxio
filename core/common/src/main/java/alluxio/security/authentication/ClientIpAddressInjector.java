package alluxio.security.authentication;

import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class ClientIpAddressInjector implements ServerInterceptor {

  /**
   * A {@link ThreadLocal} variable to maintain the client's IP address along with a specific
   * thread.
   */
  private static ThreadLocal<String> sIpAddressThreadLocal = new ThreadLocal<>();

  private boolean IsWhiteListed(String methodName) {
    return !methodName.startsWith(FileSystemMasterClientServiceGrpc.SERVICE_NAME);
  }

  /**
   * @return IP address of the gRPC client that is making the call.
   */
  public static String getIpAddress() {
    return sIpAddressThreadLocal.get();
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
          sIpAddressThreadLocal
              .set(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString());
          super.onHalfClose();
        }
      };
    }
  }
}

