package alluxio.worker.modules;

import alluxio.conf.Configuration;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import javax.inject.Named;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

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

    // the following objects are required when using dora
    bind(DoraWorker.class).to(PagedDoraWorker.class).in(Scopes.SINGLETON);
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
