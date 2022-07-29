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
import org.junit.Assert;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class BenchStandaloneGrpcServer {
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
  private GrpcServer mAlluxioServer;
  private Server mBasicServer;
  private FileSystemBase.ServerType mServerType;

  public void setServerType(FileSystemBase.ServerType serverType) {
    mServerType = serverType;
  }

  public void startServer(int port) throws IOException {
    Assert.assertNotNull(mServerType);
    if (mServerType == FileSystemBase.ServerType.ALLUXIO_GRPC_SERVER) {
      AlluxioExecutorService executor = ExecutorServiceBuilder.buildExecutorService(
          ExecutorServiceBuilder.RpcExecutorHost.MASTER);
      mAlluxioServer = GrpcServerBuilder
          .forAddress(GrpcServerAddress.create("localhost", new InetSocketAddress(port)),
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
    } else if (mServerType == FileSystemBase.ServerType.BASIC_GRPC_SERVER) {
      mBasicServer = ServerBuilder
          .forPort(port)
          .addService(mServiceVersionService)
          .addService(mFsMasterClientService)
          .addService(mMetaMasterConfService)
          .build()
          .start();
    }
  }

  private int getPort() {
    Assert.assertNotNull(mServerType);
    if (mServerType == FileSystemBase.ServerType.ALLUXIO_GRPC_SERVER) {
      return mAlluxioServer.getBindPort();
    } else if (mServerType == FileSystemBase.ServerType.BASIC_GRPC_SERVER) {
      return mBasicServer.getPort();
    }
    return -1;
  }

  public void shutdown() {
    if (mServerType == FileSystemBase.ServerType.ALLUXIO_GRPC_SERVER) {
      mAlluxioServer.shutdown();
    } else if (mServerType == FileSystemBase.ServerType.BASIC_GRPC_SERVER) {
      mBasicServer.shutdown();
    }
  }

  /**
   * This main function can be used to start a standalone server and use the stressbench with
   * said standalone server.
   * @param args -s | --server will specify the {@link FileSystemBase.ServerType}
   *             -p | --port will specify the port the server should bind to (0 will bind to any
   *             available open port)
   *             -cs | --client-socket to specify which port this process will report the server
   *             port chosen for the benchmark
   */
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    Option serverTypeOpt = new Option("s", "server", true,
        "server type (either " + FileSystemBase.ServerType.BASIC_GRPC_SERVER.name() + " or "
            + FileSystemBase.ServerType.ALLUXIO_GRPC_SERVER.name() + ")");
    serverTypeOpt.setRequired(true);
    Option portOpt = new Option("p", "port", true, "port of server");
    Option clientSocketOpt = new Option("cs", "client-socket", true, "use when launching the "
        + "process from code, not CLI. Specifies which port this server should report the server "
        + "bind port to");
    options.addOption(serverTypeOpt);
    options.addOption(portOpt);
    options.addOption(clientSocketOpt);

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
    FileSystemBase.ServerType serverType =
        FileSystemBase.ServerType.valueOf(cmd.getOptionValue(serverTypeOpt.getOpt()));
    int port = 0;
    if (cmd.hasOption(portOpt.getOpt())) {
      port = Integer.parseInt(cmd.getOptionValue("port"));
    }
    if (serverType == FileSystemBase.ServerType.STANDALONE) {
      System.out.println("Cannot use server type " + serverType);
      formatter.printHelp("standalone-server", options);
      System.exit(1);
    }
    BenchStandaloneGrpcServer standaloneServer = new BenchStandaloneGrpcServer();
    standaloneServer.setServerType(serverType);
    standaloneServer.startServer(port);
    System.out.println("Starting standalone server of type " + serverType + " at "
        + standaloneServer.getPort());
    if (cmd.hasOption(clientSocketOpt.getOpt())) {
      int clientSocketPort = Integer.parseInt(cmd.getOptionValue(clientSocketOpt.getOpt()));
      try (Socket socket = new Socket("localhost", clientSocketPort);
           DataOutputStream stream = new DataOutputStream(socket.getOutputStream())) {
        stream.writeInt(standaloneServer.getPort());
      }
    }
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Closing standalone server");
      standaloneServer.shutdown();
    }));
    // kill the server using ctrl+c
    Thread.currentThread().join();
  }
}
