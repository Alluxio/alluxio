package alluxio.hub.manager.rpc.interceptor;

import alluxio.hub.proto.HubAuthentication;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client side interceptor that is used to augment outgoing metadata with the API and secret keys
 * for the channel that the RPC is being called on.
 */
@ThreadSafe
public class HubAuthenticationInterceptor implements ClientInterceptor {

  /** Metadata key for the API key. */
  public static final Metadata.Key<String> METADATA_API_KEY =
      Metadata.Key.of("hub-api-key", Metadata.ASCII_STRING_MARSHALLER);

  /** Metadata key for the Secret key. */
  public static final Metadata.Key<String> METADATA_SECRET_KEY =
      Metadata.Key.of("hub-secret-key", Metadata.ASCII_STRING_MARSHALLER);

  private final HubAuthentication mHubAuthentication;

  /**
   * Creates the client-side interceptor that augments outgoing metadata with API and secret keys
   * for authentication.
   * @param hubAuthentication API and secret keys used by the Hub Manager for authentication
   */
  public HubAuthenticationInterceptor(HubAuthentication hubAuthentication) {
    mHubAuthentication = hubAuthentication;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // Put API and secret keys in header.
        headers.put(METADATA_API_KEY, mHubAuthentication.getApiKey());
        headers.put(METADATA_SECRET_KEY, mHubAuthentication.getSecretKey());
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
