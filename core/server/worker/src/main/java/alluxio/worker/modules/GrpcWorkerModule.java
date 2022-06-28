package alluxio.worker.modules;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.DataServer;
import alluxio.worker.WorkerRegistry;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.grpc.BlockWorkerClientServiceHandler;
import alluxio.worker.grpc.GrpcDataServer;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.UUID;
import javax.inject.Named;

/**
 * Grpc worker module.
 */
public class GrpcWorkerModule extends AbstractModule {
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
