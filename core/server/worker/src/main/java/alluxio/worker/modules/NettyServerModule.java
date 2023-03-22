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

  private final boolean mIsNettyDataTransmissionEnable;

  /**
   * The constructor of NettyServerModule.
   *
   * @param isNettyDataTransmissionEnable whether the netty data transmission enable or not
   */
  public NettyServerModule(boolean isNettyDataTransmissionEnable) {
    mIsNettyDataTransmissionEnable = isNettyDataTransmissionEnable;
  }

  @Override
  protected void configure() {
    if (!mIsNettyDataTransmissionEnable) {
      bind(NettyDataServer.class).toProvider(() -> null);
    }
  }

  /**
   * @return Grpc bind address
   */
  @Provides
  @Named("NettyBindAddress")
  public InetSocketAddress provideGrpcBindAddress() {
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
}
