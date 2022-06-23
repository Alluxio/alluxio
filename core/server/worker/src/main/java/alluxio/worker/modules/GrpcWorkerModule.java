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

public class GrpcWorkerModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcWorkerModule.class);

  @Override
  protected void configure() {
    bind(InetSocketAddress.class).annotatedWith(Names.named("GrpcConnectAddress"))
        .toInstance(NetworkAddressUtils.getConnectAddress(
            NetworkAddressUtils.ServiceType.WORKER_RPC,
            Configuration.global()));
  }

  @Provides
  @Named("RemoteDataServer")
  public Provider<DataServer> provideRemoteDataServer(
      WorkerRegistry workerRegistry,
      UfsManager ufsManager,
      @Named("GrpcConnectAddress") InetSocketAddress connectAddress,
      @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress,
      Provider<BlockWorkerClientServiceHandler> blockWorkerClientServiceHandlerProvider
  ) {
    return () -> {
      BlockWorkerClientServiceHandler blockWorkerClientServiceHandler =
          new BlockWorkerClientServiceHandler(
              //TODO(beinan):inject BlockWorker abstraction
              (DefaultBlockWorker) workerRegistry.get(BlockWorker.class),
              ufsManager,
              false);
      return new GrpcDataServer(
          connectAddress.getHostName(), gRpcBindAddress, blockWorkerClientServiceHandler);
    };
  }

  @Provides
  @Named("DomainSocketDataServer")
  public Provider<DataServer> provideDomainSocketDataServer(
      WorkerRegistry workerRegistry,
      UfsManager ufsManager,
      @Named("GrpcConnectAddress") InetSocketAddress connectAddress,
      @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress,
      Provider<BlockWorkerClientServiceHandler> blockWorkerClientServiceHandlerProvider
  ) {
    return () -> {
      String domainSocketPath =
          Configuration.getString(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
      if (Configuration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
        domainSocketPath =
            PathUtils.concatPath(domainSocketPath, UUID.randomUUID().toString());
      }
      LOG.info("Domain socket data server is enabled at {}.", domainSocketPath);
      BlockWorkerClientServiceHandler blockWorkerClientServiceHandler =
          new BlockWorkerClientServiceHandler(
              //TODO(beinan):inject BlockWorker abstraction
              (DefaultBlockWorker) workerRegistry.get(BlockWorker.class),
              ufsManager,
              true);
      GrpcDataServer domainSocketDataServer = new GrpcDataServer(connectAddress.getHostName(),
          new DomainSocketAddress(domainSocketPath), blockWorkerClientServiceHandler);
      // Share domain socket so that clients can access it.
      try {
        FileUtils.changeLocalFileToFullPermission(domainSocketPath);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return domainSocketDataServer;
    };
  }

  @Provides
  @Named("GrpcBindAddress")
  public InetSocketAddress provideGrpcBindAddress() {
    // Random port binding.
    int bindPort;
    InetSocketAddress configuredBindAddress =
        NetworkAddressUtils.getBindAddress(NetworkAddressUtils.ServiceType.WORKER_RPC,
            Configuration.global());
    if (configuredBindAddress.getPort() == 0) {
      ServerSocket bindSocket = null;
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
