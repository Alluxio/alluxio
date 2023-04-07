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

import static java.util.Objects.requireNonNull;

import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.worker.block.DefaultBlockWorker;
import alluxio.dora.worker.grpc.BlockWorkerClientServiceHandler;
import alluxio.dora.worker.grpc.DoraWorkerClientServiceHandler;
import alluxio.dora.worker.grpc.GrpcDataServer;
import alluxio.dora.grpc.BlockWorkerGrpc;
import alluxio.dora.underfs.UfsManager;
import alluxio.dora.util.io.FileUtils;
import alluxio.dora.util.io.PathUtils;
import alluxio.dora.worker.dora.DoraWorker;

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
public interface DataServerFactory {
  DataServer createRemoteGrpcDataServer(DataWorker dataWorker);

  DataServer createDomainSocketDataServer(DataWorker worker);

  /**
   * Get gRPC bind address.
   *
   * @return the InetSocketAddress object with gRPC bind address
   */
  InetSocketAddress getGRpcBindAddress();

  /**
   * Get gRPC connect address.
   *
   * @return the InetSocketAddress object with gRPC connect address
   */
  InetSocketAddress getConnectAddress();
}
