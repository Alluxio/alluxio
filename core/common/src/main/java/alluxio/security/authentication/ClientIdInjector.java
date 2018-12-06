package alluxio.security.authentication;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.UUID;

public class ClientIdInjector implements ClientInterceptor {
  public static Metadata.Key<UUID> sClientIdKey =
      Metadata.Key.of("client-id", new Metadata.AsciiMarshaller<UUID>() {
        @Override
        public String toAsciiString(UUID value) {
          return value.toString();
        }

        @Override
        public UUID parseAsciiString(String serialized) {
          return UUID.fromString(serialized);
        }
      });

  private UUID mClientId;

  public ClientIdInjector(UUID clientId) {
    mClientId = clientId;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        /* put custom header */
        headers.put(sClientIdKey, mClientId);
        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
            responseListener) {
          @Override
          public void onHeaders(Metadata headers) {
            super.onHeaders(headers);
          }
        }, headers);
      }
    };
  }
}
