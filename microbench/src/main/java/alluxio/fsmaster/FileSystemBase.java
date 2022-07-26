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

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.executor.ExecutorServiceBuilder;
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
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.PAcl;
import alluxio.grpc.ServiceType;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

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

  public enum ServerType { ALLUXIO_GRPC_SERVER, BASIC_GRPC_SERVER, STANDALONE }

  private ServerType mServerType;
  private GrpcServer mAlluxioServer;
  private Server mBasicServer;

  public ArrayList<ManagedChannel> mChannels = new ArrayList<>();

  public void init(ServerType serverType, int numGrpcChannels, String standaloneHost,
      int standalonePort) throws Exception {
    Logger.getRootLogger().setLevel(Level.ERROR);
    setServerType(serverType);
    // disabling authentication as it does not pertain to the measurements in this benchmark
    // in addition, authentication would only happen once at the beginning and would be negligible
    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    InetSocketAddress address;
    if (mServerType == ServerType.STANDALONE) {
      address = InetSocketAddress.createUnresolved(standaloneHost, standalonePort);
      Configuration.set(PropertyKey.MASTER_HOSTNAME, standaloneHost);
      Configuration.set(PropertyKey.MASTER_RPC_PORT, standalonePort);
    } else {
      startServer();
      Assert.assertTrue("port > 0", getPort() > 0);
      address = InetSocketAddress.createUnresolved("localhost", getPort());
      Configuration.set(PropertyKey.MASTER_RPC_PORT, getPort());
    }
    for (int i = 0; i < numGrpcChannels; i++) {
      mChannels.add(ManagedChannelBuilder.forAddress(address.getHostName(), address.getPort())
          .usePlaintext().build());
    }
  }

  public void setServerType(ServerType serverType) {
    mServerType = serverType;
  }

  public void startServer() throws IOException {
    Assert.assertNotNull(mServerType);
    if (mServerType == ServerType.ALLUXIO_GRPC_SERVER) {
      AlluxioExecutorService executor = ExecutorServiceBuilder.buildExecutorService(
          ExecutorServiceBuilder.RpcExecutorHost.MASTER);
      mAlluxioServer = GrpcServerBuilder
          .forAddress(GrpcServerAddress.create("localhost", new InetSocketAddress(0)),
              Configuration.global())
          .executor(executor)
          .flowControlWindow(
              (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
          .keepAliveTime(
              Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
              TimeUnit.MILLISECONDS)
          .keepAliveTimeout(
              Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
              TimeUnit.MILLISECONDS)
          .permitKeepAlive(
              Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
              TimeUnit.MILLISECONDS)
          .maxInboundMessageSize((int) Configuration.getBytes(
              PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE))
          .addService(ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE,
              new GrpcService(mFsMasterClientService))
          .addService(ServiceType.META_MASTER_CONFIG_SERVICE,
              new GrpcService(mMetaMasterConfService))
          .addService(new GrpcService(mServiceVersionService).disableAuthentication())
          .build()
          .start();
    } else if (mServerType == ServerType.BASIC_GRPC_SERVER) {
      mBasicServer = ServerBuilder
          .forPort(0)
          .addService(mServiceVersionService)
          .addService(mFsMasterClientService)
          .addService(mMetaMasterConfService)
          .build()
          .start();
    }
  }

  private int getPort() {
    Assert.assertNotNull(mServerType);
    if (mServerType == ServerType.ALLUXIO_GRPC_SERVER) {
      return mAlluxioServer.getBindPort();
    } else if (mServerType == ServerType.BASIC_GRPC_SERVER) {
      return mBasicServer.getPort();
    }
    return -1;
  }

  public void tearDown() {
    Assert.assertNotNull(mServerType);
    mChannels.forEach(ManagedChannel::shutdown);
    if (mServerType == ServerType.ALLUXIO_GRPC_SERVER) {
      mAlluxioServer.shutdown();
    } else if (mServerType == ServerType.BASIC_GRPC_SERVER) {
      mBasicServer.shutdown();
    }
  }

  /**
   * This main function can be used to start a standalone server and use the stressbench with
   * said standalone server.
   * @param args -s | --server will specify the {@link ServerType}
   */
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option serverTypeOpt = new Option("s", "server", true,
        "server type (either " + ServerType.BASIC_GRPC_SERVER.name() + " or "
        + ServerType.ALLUXIO_GRPC_SERVER.name() + ")");
    serverTypeOpt.setRequired(true);
    options.addOption(serverTypeOpt);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("standalone-server", options);
      System.exit(1);
    }

    Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL);
    ServerType serverType = ServerType.valueOf(cmd.getOptionValue("server"));
    if (serverType == ServerType.STANDALONE) {
      System.out.println("Cannot use server type " + serverType);
      formatter.printHelp("standalone-server", options);
      System.exit(1);
    }
    FileSystemBase base = new FileSystemBase();
    base.setServerType(serverType);
    base.startServer();
    System.out.println("Starting standalone server of type " + serverType + " on port "
        + base.getPort());
    try {
      Thread.currentThread().join();
    } finally {
      System.out.println("Closing server");
      base.tearDown();
    }
  }
}
