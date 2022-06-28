package alluxio.worker;

import static java.util.Objects.requireNonNull;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsManager;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.grpc.BlockWorkerClientServiceHandler;
import alluxio.worker.grpc.GrpcDataServer;

import com.google.inject.Inject;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import javax.inject.Named;

/**
 * Factory for data server.
 */
public class DataServerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerFactory.class);

  private final UfsManager mUfsManager;
  private InetSocketAddress mConnectAddress;
  private InetSocketAddress mGRpcBindAddress;

  @Inject
  DataServerFactory(UfsManager ufsManager,
      @Named("GrpcConnectAddress") InetSocketAddress connectAddress,
      @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress) {
    mUfsManager = requireNonNull(ufsManager);
    mConnectAddress = requireNonNull(connectAddress);
    mGRpcBindAddress = requireNonNull(gRpcBindAddress);
  }

  DataServer createRemoteDataServer(DefaultBlockWorker worker) {
    BlockWorkerClientServiceHandler blockWorkerClientServiceHandler =
        new BlockWorkerClientServiceHandler(
            //TODO(beinan):inject BlockWorker abstraction
            worker,
            mUfsManager,
            false);
    return new GrpcDataServer(
        mConnectAddress.getHostName(), mGRpcBindAddress, blockWorkerClientServiceHandler);
  }

  DataServer createDomainSocketDataServer(DefaultBlockWorker worker) {
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
            worker,
            mUfsManager,
            true);
    GrpcDataServer domainSocketDataServer = new GrpcDataServer(mConnectAddress.getHostName(),
        new DomainSocketAddress(domainSocketPath), blockWorkerClientServiceHandler);
    // Share domain socket so that clients can access it.
    try {
      FileUtils.changeLocalFileToFullPermission(domainSocketPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return domainSocketDataServer;
  }
}
