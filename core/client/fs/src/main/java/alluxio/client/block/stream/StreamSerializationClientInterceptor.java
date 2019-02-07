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

package alluxio.client.block.stream;

import alluxio.grpc.GrpcSerializationUtils;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;

/**
 * Serialization interceptor for client.
 */
public class StreamSerializationClientInterceptor implements ClientInterceptor {
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel channel) {
    MethodDescriptor<ReqT, RespT> overriddenMethodDescriptor =
        callOptions.getOption(GrpcSerializationUtils.OVERRIDDEN_METHOD_DESCRIPTOR);
    if (overriddenMethodDescriptor != null) {
      method = overriddenMethodDescriptor;
    }
    return channel.newCall(method, callOptions);
  }
}
