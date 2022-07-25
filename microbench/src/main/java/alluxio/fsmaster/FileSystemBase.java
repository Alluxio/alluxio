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
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.executor.RpcExecutorType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.FileInfo;
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
import alluxio.grpc.PAcl;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.grpc.TtlAction;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.meta.PathProperties;
import alluxio.security.authentication.AuthType;
import alluxio.wire.ConfigHash;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;

public class FileSystemBase {
  ServerServiceDefinition mFsMasterClientService = ServerInterceptors.intercept(
      new FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceImplBase() {
        // dummy response based on a simple './bin/alluxio fs stat /' command
        private final GetStatusPResponse mResponse = GetStatusPResponse.newBuilder()
            .setFileInfo(
                FileInfo.newBuilder()
                    .setFileId(0)
                    .setName("")
                    .setPath("/")
                    .setUfsPath("/tmp/alluxio-tmp/alluxio-0/underFSStorage")
                    .setLength(0)
                    .setBlockSizeBytes(0)
                    .setCreationTimeMs(1658424362194L)
                    .setCompleted(true)
                    .setFolder(true)
                    .setPinned(false)
                    .setCacheable(false)
                    .setPersisted(true)
                    .setLastModificationTimeMs(1658424362194L)
                    .setTtl(-1)
                    .setOwner("arthurjenoudet")
                    .setGroup("staff")
                    .setMode(493)
                    .setPersistenceState("PERSISTED")
                    .setMountPoint(false)
                    .setTtlAction(TtlAction.DELETE)
                    .setMountId(1)
                    .setInAlluxioPercentage(0)
                    .setInMemoryPercentage(0)
                    .setUfsFingerprint("")
                    .setAcl(PAcl.newBuilder()
                        .setOwner("arthurjenoudet")
                        .setOwningGroup("staff")
                        .setMode(493)
                        .setIsDefault(false)
                        .build())
                    .setDefaultAcl(PAcl.newBuilder()
                        .setOwner("arthurjenoudet")
                        .setOwningGroup("staff")
                        .setMode(0)
                        .setIsDefault(true)
                        .setIsDefaultEmpty(true)
                        .build())
                    .setReplicationMax(0)
                    .setReplicationMin(0)
                    .setLastAccessTimeMs(1658424362194L)
                    .build()
            ).build();

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
  public FileSystem mFs;
  public ArrayList<ManagedChannel> mChannels = new ArrayList<>();

  public void init() throws IOException, InterruptedException {
    Logger.getRootLogger().setLevel(Level.ERROR);
    final int numThreads = Runtime.getRuntime().availableProcessors();
    // client and server conf
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    // client conf
    Configuration.set(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MIN, numThreads);
    // server conf
    Configuration.set(PropertyKey.MASTER_RPC_EXECUTOR_TYPE, RpcExecutorType.FJP);
    Configuration.set(PropertyKey.MASTER_RPC_EXECUTOR_FJP_MIN_RUNNABLE, numThreads);

    AlluxioExecutorService executor = ExecutorServiceBuilder.buildExecutorService(
            ExecutorServiceBuilder.RpcExecutorHost.MASTER);
//    mServer = GrpcServerBuilder
//        .forAddress(GrpcServerAddress.create("localhost", new InetSocketAddress(0)),
//            Configuration.global())
//        .executor(executor)
//        .flowControlWindow(
//            (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
//        .keepAliveTime(
//            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
//            TimeUnit.MILLISECONDS)
//        .keepAliveTimeout(
//            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
//            TimeUnit.MILLISECONDS)
//        .permitKeepAlive(
//            Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
//            TimeUnit.MILLISECONDS)
//        .maxInboundMessageSize((int) Configuration.getBytes(
//            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE))
//        .addService(ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE,
//            new GrpcService(mFsMasterClientService))
//        .addService(ServiceType.META_MASTER_CONFIG_SERVICE,
//            new GrpcService(mMetaMasterConfService))
//        .addService(new GrpcService(mServiceVersionService).disableAuthentication())
//        .build()
//        .start();
    mServer = ServerBuilder
        .forPort(0)
        .addService(mServiceVersionService)
        .addService(mFsMasterClientService)
        .addService(mMetaMasterConfService)
//        .directExecutor()
        .build()
        .start();

    for (int i = 0; i < 2; i++) {
      mChannels.add(ManagedChannelBuilder.forAddress("localhost", mServer.getPort())
          .usePlaintext().build());
    }

    Assert.assertTrue("port > 0", mServer.getPort() > 0);

    Configuration.set(PropertyKey.MASTER_RPC_PORT, mServer.getPort());
    // disabling authentication as it does not pertain to the measurements in this benchmark
    // in addition, authentication would only happen once at the beginning and would be negligible

    mFs = FileSystem.Factory.create(Configuration.global());
  }

  public void tearDown() throws Exception {
    mFs.close();
    mChannels.forEach(ManagedChannel::shutdown);
    mServer.shutdown();
  }

  public final AlluxioURI mURI = new AlluxioURI("/");

  public URIStatus getStatus() throws IOException, AlluxioException {
    return mFs.getStatus(mURI);
  }
}
