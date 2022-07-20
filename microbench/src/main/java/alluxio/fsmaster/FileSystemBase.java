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

package alluxio.fsmaster;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.GetConfigHashPOptions;
import alluxio.grpc.GetConfigHashPResponse;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GetServiceVersionPResponse;
import alluxio.grpc.GetStatusPRequest;
import alluxio.grpc.GetStatusPResponse;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.master.meta.PathProperties;
import alluxio.security.authentication.AuthType;
import alluxio.wire.ConfigHash;

import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class FileSystemBase {
  ServerServiceDefinition mFsMasterClientService = ServerInterceptors.intercept(
      new FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceImplBase() {
        private final GetStatusPResponse mResponse = GetStatusPResponse.getDefaultInstance();

        @Override
        public void getStatus(GetStatusPRequest request,
            StreamObserver<GetStatusPResponse> responseObserver) {
          responseObserver.onNext(mResponse);
          responseObserver.onCompleted();
        }
      }
  );
  ServerServiceDefinition mMetaMasterConfService = ServerInterceptors.intercept(
      new MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceImplBase() {
        private final GetConfigurationPResponse mGetConfigResponse =
            GetConfigurationPResponse.newBuilder()
                .addClusterConfigs(
                ConfigProperty.newBuilder()
                    .setName(PropertyKey.VERSION.getName())
                    .setSource(Source.CLUSTER_DEFAULT.toString())
                    .setValue(Configuration.getString(PropertyKey.VERSION))
                    .build()
            ).build();

        @Override
        public void getConfiguration(GetConfigurationPOptions request,
            StreamObserver<GetConfigurationPResponse> responseObserver) {
          responseObserver.onNext(mGetConfigResponse);
          responseObserver.onCompleted();
        }

        private final GetConfigHashPResponse mGetConfigHashResponse =
          new ConfigHash(Configuration.hash(), new PathProperties().hash()).toProto();

        @Override
        public void getConfigHash(GetConfigHashPOptions request,
            StreamObserver<GetConfigHashPResponse> responseObserver) {
          responseObserver.onNext(mGetConfigHashResponse);
          responseObserver.onCompleted();
        }
      });

  ServerServiceDefinition mServiceVersionService = ServerInterceptors.intercept(
      new ServiceVersionClientServiceGrpc.ServiceVersionClientServiceImplBase() {
        private final GetServiceVersionPResponse mResponse = GetServiceVersionPResponse.newBuilder()
            .setVersion(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION)
            .build();

        @Override
        public void getServiceVersion(GetServiceVersionPRequest request,
            StreamObserver<GetServiceVersionPResponse> responseObserver) {
          responseObserver.onNext(mResponse);
          responseObserver.onCompleted();
        }
      });

  Server mServer;
  FileSystem mFs;

  public void init() throws IOException {
    Logger.getRootLogger().setLevel(Level.ERROR);
    mServer = NettyServerBuilder
        .forPort(0) // assigns a random open port
        .addService(mFsMasterClientService)
        .addService(mMetaMasterConfService)
        .addService(mServiceVersionService)
        .build()
        .start();
    Assert.assertTrue("port > 0", mServer.getPort() > 0);

    Configuration.set(PropertyKey.MASTER_RPC_PORT, mServer.getPort());
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);

    mFs = FileSystem.Factory.create(Configuration.global());
  }

  public void tearDown() throws Exception {
    mFs.close();
    mServer.awaitTermination(2, TimeUnit.SECONDS);
  }

  private final AlluxioURI mURI = new AlluxioURI("/");

  public URIStatus getStatus() throws IOException, AlluxioException {
    return mFs.getStatus(mURI);
  }
}
