package alluxio.worker.modules;

import alluxio.conf.Configuration;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import alluxio.worker.netty.NettyDataServer;
import javax.inject.Named;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * Grpc worker module.
 */
public class NettyServerModule extends AbstractModule {

  private final boolean mIsNettyDataTransmissionEnable;

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
