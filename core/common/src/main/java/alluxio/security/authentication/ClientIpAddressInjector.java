package alluxio.security.authentication;

import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * Server side interceptor that is used to put remote client's IP Address to TLS.
 */
public class ClientIpAddressInjector implements ServerInterceptor {

  /**
   * A {@link ThreadLocal} variable to maintain the client's IP address along with a specific
   * thread.
   */
  private static ThreadLocal<String> sIpAddressThreadLocal = new ThreadLocal<>();

  /**
   * @return IP address of the gRPC client that is making the call.
   */
  public static String getIpAddress() {
    return sIpAddressThreadLocal.get();
  }

  private boolean IsWhiteListed(String methodName) {
    return !methodName.startsWith(FileSystemMasterClientServiceGrpc.SERVICE_NAME);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    if (IsWhiteListed(call.getMethodDescriptor().getFullMethodName())) {
      return next.startCall(call, headers);
    } else {
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
          next.startCall(call, headers)) {
        /**
         * onHalfClose is called on the same thread that calls the service handler.
         */
        @Override
        public void onHalfClose() {
          String remoteIpAddress =
              call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
          sIpAddressThreadLocal.set(remoteIpAddress);
          super.onHalfClose();
        }
      };
    }
  }
}

