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

package alluxio.dora.worker;

import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.util.io.FileUtils;
import alluxio.dora.util.io.PathUtils;
import alluxio.dora.worker.block.DefaultBlockWorker;
import alluxio.dora.worker.dora.DoraWorker;
import alluxio.dora.worker.grpc.BlockWorkerClientServiceHandler;
import alluxio.dora.worker.grpc.DoraWorkerClientServiceHandler;
import alluxio.dora.worker.grpc.GrpcDataServer;
import com.google.inject.Inject;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.net.InetSocketAddress;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Factory for data server.
 */
public class AlluxioDataServerFactory implements DataServerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioDataServerFactory.class);

  private final UfsManager mUfsManager;
  private InetSocketAddress mConnectAddress;
  private InetSocketAddress mGRpcBindAddress;

  @Inject
  AlluxioDataServerFactory(UfsManager ufsManager,
                           @Named("GrpcConnectAddress") InetSocketAddress connectAddress,
                           @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress) {
    mUfsManager = requireNonNull(ufsManager);
    mConnectAddress = requireNonNull(connectAddress);
    mGRpcBindAddress = requireNonNull(gRpcBindAddress);
  }

  public DataServer createRemoteGrpcDataServer(DataWorker dataWorker) {
    BlockWorkerGrpc.BlockWorkerImplBase blockWorkerService;
    if (dataWorker instanceof DoraWorker) {
      blockWorkerService =
          new DoraWorkerClientServiceHandler((DoraWorker) dataWorker);
    } else {
      blockWorkerService =
          new BlockWorkerClientServiceHandler(
              //TODO(beinan): inject BlockWorker abstraction
              (DefaultBlockWorker) dataWorker,
              mUfsManager,
              false);
    }
    return new GrpcDataServer(
        mConnectAddress.getHostName(), mGRpcBindAddress, blockWorkerService);
  }

  public DataServer createDomainSocketDataServer(DataWorker worker) {
    String domainSocketPath =
        Configuration.getString(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
    if (Configuration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
      domainSocketPath =
          PathUtils.concatPath(domainSocketPath, UUID.randomUUID().toString());
    }
    LOG.info("Domain socket data server is enabled at {}.", domainSocketPath);
    BlockWorkerGrpc.BlockWorkerImplBase blockWorkerService;
    if (worker instanceof DoraWorker) {
      blockWorkerService =
          new DoraWorkerClientServiceHandler((DoraWorker) worker);
    } else {
      blockWorkerService =
          new BlockWorkerClientServiceHandler(
            //TODO(beinan):inject BlockWorker abstraction
            (DefaultBlockWorker) worker,
            mUfsManager,
            true);
    }
    GrpcDataServer domainSocketDataServer = new GrpcDataServer(mConnectAddress.getHostName(),
        new DomainSocketAddress(domainSocketPath), blockWorkerService);
    // Share domain socket so that clients can access it.
    FileUtils.changeLocalFileToFullPermission(domainSocketPath);
    return domainSocketDataServer;
  }

  /**
   * Get gRPC bind address.
   *
   * @return the InetSocketAddress object with gRPC bind address
   */
  public InetSocketAddress getGRpcBindAddress() {
    return mGRpcBindAddress;
  }

  /**
   * Get gRPC connect address.
   *
   * @return the InetSocketAddress object with gRPC connect address
   */
  public InetSocketAddress getConnectAddress() {
    return mConnectAddress;
  }
}
