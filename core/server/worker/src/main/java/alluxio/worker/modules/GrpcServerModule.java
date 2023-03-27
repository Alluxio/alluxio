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

package alluxio.worker.modules;

import alluxio.conf.Configuration;
import alluxio.util.network.NetworkAddressUtils;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import javax.inject.Named;

/**
 * Grpc worker module.
 */
public class GrpcServerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(InetSocketAddress.class).annotatedWith(Names.named("GrpcConnectAddress"))
        .toInstance(NetworkAddressUtils.getConnectAddress(
            NetworkAddressUtils.ServiceType.WORKER_RPC,
            Configuration.global()));
  }

  /**
   * @return Grpc bind address
   */
  @Provides
  @Named("GrpcBindAddress")
  public InetSocketAddress provideGrpcBindAddress() {
    // Random port binding.
    int bindPort;
    InetSocketAddress configuredBindAddress =
        NetworkAddressUtils.getBindAddress(NetworkAddressUtils.ServiceType.WORKER_RPC,
            Configuration.global());
    if (configuredBindAddress.getPort() == 0) {
      ServerSocket bindSocket;
      try {
        bindSocket = new ServerSocket(0);
        bindPort = bindSocket.getLocalPort();
        // Socket opened for auto bind.
        // Close it.
        bindSocket.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      bindPort = configuredBindAddress.getPort();
    }
    return new InetSocketAddress(configuredBindAddress.getHostName(), bindPort);
  }
}
