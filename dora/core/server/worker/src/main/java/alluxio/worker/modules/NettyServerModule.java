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
import alluxio.worker.netty.NettyDataServer;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import javax.inject.Named;

/**
 * Grpc worker module.
 */
public class NettyServerModule extends AbstractModule {

  private final boolean mNettyDataTransmissionEnable;

  /**
   * The constructor of NettyServerModule.
   *
   * @param isNettyDataTransmissionEnable whether the netty data transmission is enabled or not
   */
  public NettyServerModule(boolean isNettyDataTransmissionEnable) {
    mNettyDataTransmissionEnable = isNettyDataTransmissionEnable;
  }

  @Override
  protected void configure() {
    if (!mNettyDataTransmissionEnable) {
      bind(NettyDataServer.class).toProvider(() -> null);
    }
  }

  /**
   * @return Grpc bind address
   */
  @Provides
  @Named("NettyBindAddress")
  public InetSocketAddress provideNettyBindAddress() {
    // Random port binding.
    int bindPort;
    InetSocketAddress configuredBindAddress =
        NetworkAddressUtils.getBindAddress(NetworkAddressUtils.ServiceType.WORKER_DATA,
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

  /**
   * @return s3 bind address
   */
  @Provides
  @Named("S3BindAddress")
  public InetSocketAddress provideS3BindAddress() {
    // bind http port
    InetSocketAddress httpBindAddress = NetworkAddressUtils.getBindAddress(
        NetworkAddressUtils.ServiceType.WORKER_REST,
        Configuration.global());
    return httpBindAddress;
  }
}
