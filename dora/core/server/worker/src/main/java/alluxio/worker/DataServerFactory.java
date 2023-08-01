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

package alluxio.worker;

import static java.util.Objects.requireNonNull;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.underfs.UfsManager;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.grpc.DoraWorkerClientServiceHandler;
import alluxio.worker.grpc.GrpcDataServer;

import com.google.inject.Inject;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.UUID;
import javax.inject.Named;

/**
 * Factory for data server.
 */
@SuppressFBWarnings("BC_UNCONFIRMED_CAST")
public class DataServerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerFactory.class);

  private final UfsManager mUfsManager;
  private InetSocketAddress mConnectAddress;
  private InetSocketAddress mGRpcBindAddress;

  @Inject
  protected DataServerFactory(UfsManager ufsManager,
                    @Named("GrpcConnectAddress") InetSocketAddress connectAddress,
                    @Named("GrpcBindAddress") InetSocketAddress gRpcBindAddress) {
    mUfsManager = requireNonNull(ufsManager);
    mConnectAddress = requireNonNull(connectAddress);
    mGRpcBindAddress = requireNonNull(gRpcBindAddress);
  }

  /**
   * @param dataWorker the dora worker
   * @return the remoteGrpcServer
   */
  public DataServer createRemoteGrpcDataServer(DataWorker dataWorker) {
    BlockWorkerGrpc.BlockWorkerImplBase blockWorkerService;
    if (dataWorker instanceof DoraWorker) {
      blockWorkerService =
          new DoraWorkerClientServiceHandler((DoraWorker) dataWorker);
    } else {
      throw new UnsupportedOperationException(dataWorker.getClass().getCanonicalName()
          + " is no longer supported in Alluxio 3.x");
    }
    return new GrpcDataServer(
        mConnectAddress.getHostName(), mGRpcBindAddress, blockWorkerService);
  }

  /**
   * @param worker the dora worker
   * @return the domain socket data server
   */
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
      throw new UnsupportedOperationException(worker.getClass().getCanonicalName()
          + " is no longer supported in Alluxio 3.x");
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
