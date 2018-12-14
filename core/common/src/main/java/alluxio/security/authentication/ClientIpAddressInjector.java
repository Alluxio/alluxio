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
   * @return IP address of the gRPC client that is making the call
   */
  public static String getIpAddress() {
    return sIpAddressThreadLocal.get();
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
        String remoteIpAddress =
            call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
        sIpAddressThreadLocal.set(remoteIpAddress);
        super.onHalfClose();
      }
    };
  }
}

