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

package alluxio.hub.manager.process;

import static alluxio.conf.Source.CLUSTER_DEFAULT;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.RuntimeConstants;
import alluxio.cli.ValidationUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.meta.MetaMasterClient;
import alluxio.client.meta.RetryHandlingMetaMasterClient;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.MountPOptions;
import alluxio.hub.common.HubSslContextProvider;
import alluxio.hub.common.HubUtil;
import alluxio.hub.common.RpcClient;
import alluxio.hub.manager.rpc.interceptor.HubAuthenticationInterceptor;
import alluxio.hub.manager.rpc.observer.RequestStreamObserver;
import alluxio.hub.manager.util.AlluxioCluster;
import alluxio.hub.manager.util.HubCluster;
import alluxio.hub.proto.AgentDetectPrestoRequest;
import alluxio.hub.proto.AgentDetectPrestoResponse;
import alluxio.hub.proto.AgentFileUploadRequest;
import alluxio.hub.proto.AgentFileUploadResponse;
import alluxio.hub.proto.AgentGetConfigurationSetRequest;
import alluxio.hub.proto.AgentHeartbeatRequest;
import alluxio.hub.proto.AgentListCatalogRequest;
import alluxio.hub.proto.AgentListFileRequest;
import alluxio.hub.proto.AgentManagerServiceGrpc;
import alluxio.hub.proto.AgentProcessStatusChangeResponse;
import alluxio.hub.proto.AgentRemoveFileRequest;
import alluxio.hub.proto.AgentRemoveFileResponse;
import alluxio.hub.proto.AgentSetPrestoConfRequest;
import alluxio.hub.proto.AgentSetPrestoConfResponse;
import alluxio.hub.proto.AgentShutdownRequest;
import alluxio.hub.proto.AgentShutdownResponse;
import alluxio.hub.proto.AgentWriteConfigurationSetRequest;
import alluxio.hub.proto.AgentWriteConfigurationSetResponse;
import alluxio.hub.proto.AlluxioClusterHeartbeatRequest;
import alluxio.hub.proto.AlluxioClusterHeartbeatResponse;
import alluxio.hub.proto.AlluxioConfigurationSet;
import alluxio.hub.proto.AlluxioEdition;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.ApplyMountPointRequest;
import alluxio.hub.proto.ApplyMountPointResponse;
import alluxio.hub.proto.DeleteMountPointRequest;
import alluxio.hub.proto.DeleteMountPointResponse;
import alluxio.hub.proto.DetectPrestoRequest;
import alluxio.hub.proto.DetectPrestoResponse;
import alluxio.hub.proto.ExecutionType;
import alluxio.hub.proto.GetConfigurationSetRequest;
import alluxio.hub.proto.GetConfigurationSetResponse;
import alluxio.hub.proto.GetPrestoConfDirRequest;
import alluxio.hub.proto.GetPrestoConfDirResponse;
import alluxio.hub.proto.HostedManagerServiceGrpc;
import alluxio.hub.proto.HubAuthentication;
import alluxio.hub.proto.HubMetadata;
import alluxio.hub.proto.HubStatus;
import alluxio.hub.proto.ListCatalogRequest;
import alluxio.hub.proto.ListCatalogResponse;
import alluxio.hub.proto.ListFile;
import alluxio.hub.proto.ListFileRequest;
import alluxio.hub.proto.ListFileResponse;
import alluxio.hub.proto.ListMountPointRequest;
import alluxio.hub.proto.ListMountPointResponse;
import alluxio.hub.proto.HDFSMountPointInfo;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.PingManagerRequest;
import alluxio.hub.proto.PingManagerResponse;
import alluxio.hub.proto.PrestoCatalogListingResult;
import alluxio.hub.proto.ProcessState;
import alluxio.hub.proto.ProcessStatusChangeRequest;
import alluxio.hub.proto.ProcessStatusChangeResponse;
import alluxio.hub.proto.RegisterManagerRequest;
import alluxio.hub.proto.RegisterManagerResponse;
import alluxio.hub.proto.SetPrestoConfDirRequest;
import alluxio.hub.proto.SetPrestoConfDirResponse;
import alluxio.hub.proto.SpeedTestRequest;
import alluxio.hub.proto.RegisterAgentRequest;
import alluxio.hub.proto.RemoveFile;
import alluxio.hub.proto.RemoveFileRequest;
import alluxio.hub.proto.RemoveFileResponse;
import alluxio.hub.proto.SpeedStat;
import alluxio.hub.proto.SpeedTestParameter;
import alluxio.hub.proto.SpeedTestResponse;
import alluxio.hub.proto.UploadFile;
import alluxio.hub.proto.UploadFileRequest;
import alluxio.hub.proto.UploadFileResponse;
import alluxio.hub.proto.WriteConfigurationSetRequest;
import alluxio.hub.proto.WriteConfigurationSetResponse;
import alluxio.master.MasterClientContext;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.stress.cli.UfsIOBench;
import alluxio.stress.worker.IOTaskSummary;
import alluxio.stress.worker.UfsIOParameters;
import alluxio.util.ConfigurationUtils;
import alluxio.util.JsonSerializable;
import alluxio.util.LogUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.MountPointInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This context holds information and methods critical to the function of the Hub Manager.
 * This object may be safely shared between multiple threads.
 */
@ThreadSafe
public class ManagerProcessContext implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(ManagerProcessContext.class);
  private static final String K8S_CONFIG_MAP_ENV = "ALLUXIO_HUB_CONFIG_MAP";
  private static final String K8S_CONFIG_MAP_ENV_LOG4J = "ALLUXIO_LOG4J_PROPERTIES";
  private static final String K8S_CONFIG_MAP_ENV_SITE = "ALLUXIO_SITE_PROPERTIES";

  private final Lock mLock = new ReentrantLock();
  private final RpcClient<HostedManagerServiceGrpc.HostedManagerServiceBlockingStub> mHostedClient;
  private HostedManagerServiceGrpc.HostedManagerServiceStub mHostedAsyncSub;
  private HubMetadata mHubMetadata;
  private final AlluxioConfiguration mConf;
  private final AlluxioCluster mAlluxioCluster;
  private final HubCluster mHubCluster;
  private final ScheduledExecutorService mSvc;
  @Nullable
  private FileSystem mFileSystem;
  private String mK8sConfigMapName;
  private Config mK8sConfig;

  /**
   * Creates a new instance of {@link ManagerProcessContext}.
   *
   * @param conf alluxio configuration
   */
  public ManagerProcessContext(AlluxioConfiguration conf) {
    this(conf,
        new ScheduledThreadPoolExecutor(conf.getInt(PropertyKey.HUB_MANAGER_EXECUTOR_THREADS_MIN),
        ThreadFactoryUtils.build("alluxio-hub-manager-executor-%d", true)));
  }

  /**
   * Creates a new instance of {@link ManagerProcessContext}.
   *
   * @param conf alluxio configuration
   * @param svc the executor service used by this context
   */
  ManagerProcessContext(AlluxioConfiguration conf, ScheduledExecutorService svc) {
    this(conf, svc, new AlluxioCluster(svc));
  }

  /**
   * Creates a new instance of {@link ManagerProcessContext}.
   *
   * @param conf alluxio configuration
   * @param svc the executor service used by this context
   * @param cluster Alluxio cluster
   */
  ManagerProcessContext(AlluxioConfiguration conf, ScheduledExecutorService svc,
      AlluxioCluster cluster) {
    this(conf, svc, cluster, new HubCluster(svc,
                    conf.getMs(PropertyKey.HUB_MANAGER_AGENT_LOST_THRESHOLD_TIME),
                    conf.getMs(PropertyKey.HUB_MANAGER_AGENT_DELETE_THRESHOLD_TIME), cluster));
  }

  /**
   * Creates a new instance of {@link ManagerProcessContext}.
   *
   * @param conf alluxio configuration
   * @param svc the executor service to use
   * @param alluxioCluster the alluxio cluster backing this context
   * @param hubCluster the hub cluster backing this context
   */
  ManagerProcessContext(AlluxioConfiguration conf, ScheduledExecutorService svc,
      AlluxioCluster alluxioCluster, HubCluster hubCluster) {
    LOG.info("Initializing manager context");
    mConf = conf;
    mSvc = svc;
    mAlluxioCluster = alluxioCluster;
    mHubCluster = hubCluster;
    mK8sConfigMapName = System.getenv(K8S_CONFIG_MAP_ENV);
    if (mK8sConfigMapName != null) {
      LOG.info("Detected K8s environment with configMap {}", mK8sConfigMapName);
      try {
        mK8sConfig = new ConfigBuilder().build();
      } catch (Exception e) {
        LOG.error("Unable to initialize K8s client config", e);
      }
    }
    AlluxioConfiguration modConf = getConfWithHubTlsEnabled();
    InetSocketAddress addr = NetworkAddressUtils
        .getConnectAddress(NetworkAddressUtils.ServiceType.HUB_HOSTED_RPC, modConf);
    mHostedClient = new RpcClient<>(modConf, addr,
        (Channel channel) -> {
          ((GrpcChannel) channel).intercept(new HubAuthenticationInterceptor(
                  HubAuthentication.newBuilder()
                  .setApiKey(modConf.get(PropertyKey.HUB_AUTHENTICATION_API_KEY))
                  .setSecretKey(modConf.get(PropertyKey.HUB_AUTHENTICATION_SECRET_KEY))
                  .build()
          ));
          return HostedManagerServiceGrpc.newBlockingStub(channel);
        }, () -> ExponentialTimeBoundedRetry.builder()
            .withSkipInitialSleep()
            .withMaxDuration(
                    Duration.ofMillis(modConf.getMs(PropertyKey.HUB_MANAGER_REGISTER_RETRY_TIME)))
            .build());
    LOG.info("Initialized manager context");
  }

  private void connectToHub(alluxio.hub.proto.AlluxioCluster cluster) throws Exception {
    // initialize manager with Hub before setting up bi-di connections
    LOG.info("Connecting to Hub...");
    // Max time ~(retry time) in seconds
    long retryTimeMs = mConf.getMs(PropertyKey.HUB_MANAGER_REGISTER_RETRY_TIME);
    RetryPolicy retry = new ExponentialBackoffRetry(50, 1000,
            (int) (retryTimeMs / 1000));
    while (retry.attempt()) {
      try {
        RegisterManagerResponse response =
                mHostedClient.get().registerManager(RegisterManagerRequest.newBuilder()
                        .setHubMetadata(mHubMetadata)
                        .setPayload(RegisterManagerRequest.Payload.newBuilder()
                                .setAlluxioCluster(cluster))
                        .build());
        Preconditions.checkArgument(response.getPayload().getStatus()
                .equals(HubStatus.MANAGER_START_CONNECTION),
            "Missing status in registerManager response");
      } catch (Throwable t) {
        handleStatusRuntimeException("Failed trying to register manager with Hosted Hub", t);
        continue;
      }
      startPingManagerListener();
      startProcessStatusChangeListener();
      startGetConfigurationSetListener();
      startWriteConfigurationSetListener();
      startUploadFileListener();
      startListFileListener();
      startRemoveFileListener();
      startDetectPrestoListener();
      startListCatalogListener();
      startSetPrestoConfDirListener();
      startGetPrestoConfDirListener();
      startListMountPointsListener();
      startApplyMountPointListener();
      startDeleteMountPointListener();
      startSpeedTestListener();
      LOG.info("Connected to Hub.");
      return;
    }
    throw new Exception(String.format("Failed to connect to Hub after %s attempts",
            retry.getAttemptCount()));
  }

  private void handleStatusRuntimeException(String message, Throwable t) {
    LogUtils.warnWithException(LOG, message, t);
    if (Status.fromThrowable(t).getCode() == Status.UNAUTHENTICATED.getCode()) {
      mLock.lock();
      // shut down the Hub Manager and Agents
      String msg = String.format("Shutting down the Hub Agent because the Hub Manager is "
                      + "unauthenticated. Check %s, %s properties in the Hub Manager's "
                      + "alluxio-site.properties.",
              PropertyKey.Name.HUB_AUTHENTICATION_API_KEY,
              PropertyKey.Name.HUB_AUTHENTICATION_SECRET_KEY);
      AgentShutdownRequest req = AgentShutdownRequest.newBuilder()
              .setLogMessage(msg).setExitCode(401).build();
      Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub,
              AgentShutdownResponse> x = (client) -> {
                AgentShutdownResponse resp = client.shutdown(req);
                return resp;
              };
      try {
        execOnHub(x);
      } catch (Exception e) {
        LogUtils.warnWithException(LOG, "Failed to call shutdown() on Hub Agent. "
            + "Continuing with shutting down Hub Manager.", e);
      }
      LOG.error(String.format("Shutting down the Hub manager because it is unauthenticated. "
                      + "Check %s, %s properties in alluxio-site.properties.",
              PropertyKey.Name.HUB_AUTHENTICATION_API_KEY,
              PropertyKey.Name.HUB_AUTHENTICATION_SECRET_KEY));
      mLock.unlock();
      System.exit(401);
    }
  }

  private HubMetadata getHubMetadata() {
    AlluxioConfiguration configSet = getUpdatedProps(configurationSetFor(AlluxioNodeType.MASTER));
    ClientContext ctx = ClientContext.create(configSet);
    MasterClientContext masterConfig = MasterClientContext.newBuilder(ctx).build();
    MetaMasterClient metaMasterClient = new RetryHandlingMetaMasterClient(masterConfig);
    HubMetadata hubMetadata = HubMetadata.getDefaultInstance();
    try {
      String clusterId = metaMasterClient.getMasterInfo(Collections.emptySet()).getClusterId();
      hubMetadata = HubMetadata.newBuilder()
              .setAlluxioVersion(RuntimeConstants.VERSION)
              .setClusterId(clusterId)
              .setLabel(mConf.get(PropertyKey.HUB_CLUSTER_LABEL))
              .setAlluxioEdition(AlluxioEdition.ALLUXIO_COMMUNITY_EDITION)
              .build();
    } catch (IOException e) {
      LogUtils.warnWithException(LOG, "Unable to get cluster id from Alluxio master", e);
    }
    return hubMetadata;
  }

  private AlluxioConfiguration getConfWithHubTlsEnabled() {
    InstancedConfiguration modifiedConfig = InstancedConfiguration.defaults();
    Map<String, String> properties = new HashMap<>(mConf.toMap());
    properties.put(PropertyKey.NETWORK_TLS_ENABLED.getName(),
            mConf.get(PropertyKey.HUB_NETWORK_TLS_ENABLED));
    properties.put(PropertyKey.NETWORK_TLS_SSL_CONTEXT_PROVIDER_CLASSNAME.getName(),
            HubSslContextProvider.class.getName());
    modifiedConfig.merge(properties, Source.RUNTIME);
    return modifiedConfig;
  }

  private HostedManagerServiceGrpc.HostedManagerServiceStub getHostedAsyncStub() {
    AlluxioConfiguration modifiedConfig = getConfWithHubTlsEnabled();
    LOG.debug("Connecting to hosted hub with TLS enabled={}", modifiedConfig
          .getBoolean(PropertyKey.NETWORK_TLS_ENABLED));
    if (mHostedAsyncSub == null) {
      InetSocketAddress addr = NetworkAddressUtils
          .getConnectAddress(NetworkAddressUtils.ServiceType.HUB_HOSTED_RPC, modifiedConfig);
      try {
        GrpcChannel channel = RpcClient.createChannel(addr, modifiedConfig);
        channel.intercept(new HubAuthenticationInterceptor(HubAuthentication.newBuilder()
                .setApiKey(modifiedConfig.get(PropertyKey.HUB_AUTHENTICATION_API_KEY))
                .setSecretKey(modifiedConfig.get(PropertyKey.HUB_AUTHENTICATION_SECRET_KEY))
                .build()));
        mHostedAsyncSub =
                HostedManagerServiceGrpc.newStub(channel);
      } catch (AlluxioStatusException e) {
        LOG.error("Error connecting to hosted hub {}", e);
      }
    }
    return mHostedAsyncSub;
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} PingManager RPC calls.
   * The {@link PingManagerResponse}'s {@link HubMetadata} must be the {@link PingManagerRequest}'s
   * {@link HubMetadata} in order for the Hosted Hub to properly track where the ping is coming
   * from. This is to handle scenarios where the cluster id changed due to Alluxio journal formats.
   */
  public void startPingManagerListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<PingManagerRequest, PingManagerResponse>() {
      @Override
      public PingManagerResponse exec(PingManagerRequest req) {
        Preconditions.checkArgument(req.hasHubMetadata());
        Preconditions.checkArgument(req.getHubMetadata().hasClusterId());
        // set response's HubMetadata to request's HubMetadata for the Hosted Hub to
        // properly process response
        return PingManagerResponse.newBuilder()
                .setHubMetadata(req.getHubMetadata())
                .setPayload(PingManagerResponse.Payload.newBuilder()
                        .setSuccess(req.getHubMetadata().getClusterId().equals(mHubMetadata
                                .getClusterId())))
                .build();
      }

      @Override
      public void restart() {
        startPingManagerListener();
        // on ping stream connections - send heartbeat to Hub in-case Hub
        // needs to check if cluster id changed
        alluxioClusterHeartbeat(mAlluxioCluster.toProto());
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<PingManagerResponse> responseObserver =
            asyncStub.pingManager(requestObserver);
    requestObserver.start(responseObserver,
            PingManagerResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started PingManager async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} ProcessStatusChange
   * RPC calls.
   */
  public void startProcessStatusChangeListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<ProcessStatusChangeRequest, ProcessStatusChangeResponse>() {
      @Override
      public ProcessStatusChangeResponse exec(ProcessStatusChangeRequest req) {
        return ProcessStatusChangeResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(ProcessStatusChangeResponse.Payload.newBuilder()
                        .addAllProcessStatusChangeResponse(processStatusChange(req)))
                .build();
      }

      @Override
      public void restart() {
        startProcessStatusChangeListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<ProcessStatusChangeResponse> responseObserver =
            asyncStub.processStatusChange(requestObserver);
    requestObserver.start(responseObserver,
            ProcessStatusChangeResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started ProcessStatusChange async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} GetConfigurationSet
   * RPC calls.
   */
  public void startGetConfigurationSetListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<GetConfigurationSetRequest, GetConfigurationSetResponse>() {
      @Override
      public GetConfigurationSetResponse exec(GetConfigurationSetRequest req) {
        AlluxioConfigurationSet confSet = configurationSetFor(req.getPayload().getNodeType());
        return GetConfigurationSetResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(GetConfigurationSetResponse.Payload.newBuilder().setConfSet(confSet)
                        .build())
                .build();
      }

      @Override
      public void restart() {
        startGetConfigurationSetListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<GetConfigurationSetResponse> responseObserver =
            asyncStub.getConfigurationSet(requestObserver);
    requestObserver.start(responseObserver,
            GetConfigurationSetResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started GetConfigurationSet async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} WriteConfigurationSet
   * RPC calls.
   */
  public void startWriteConfigurationSetListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<WriteConfigurationSetRequest,
                    WriteConfigurationSetResponse>() {
      @Override
      public WriteConfigurationSetResponse exec(WriteConfigurationSetRequest req) {
        Preconditions.checkArgument(req.hasPayload());
        Preconditions.checkArgument(req.getPayload().hasConfSet());
        Preconditions.checkArgument(req.getPayload().getNodeTypeCount() > 0);
        req.getPayload().getNodeTypeList().forEach(
            nodeType -> updateConfigurationFor(nodeType, req.getPayload().getConfSet()));
        return WriteConfigurationSetResponse.newBuilder().setHubMetadata(mHubMetadata).build();
      }

      @Override
      public void restart() {
        startWriteConfigurationSetListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<WriteConfigurationSetResponse> responseObserver =
            asyncStub.writeConfigurationSet(requestObserver);
    requestObserver.start(responseObserver,
            WriteConfigurationSetResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started WriteConfigurationSet async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} UploadFile RPC calls.
   */
  public void startUploadFileListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<UploadFileRequest, UploadFileResponse>() {
      @Override
      public UploadFileResponse exec(UploadFileRequest req) {
        boolean success = uploadFile(req.getPayload().getFileList());
        return UploadFileResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(UploadFileResponse.Payload.newBuilder().setSuccess(success)).build();
      }

      @Override
      public void restart() {
        startUploadFileListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<UploadFileResponse> responseObserver = asyncStub.uploadFile(requestObserver);
    requestObserver.start(responseObserver,
            UploadFileResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started UploadFile async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} ListFile RPC calls.
   */
  public void startListFileListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<ListFileRequest, ListFileResponse>() {
      @Override
      public ListFileResponse exec(ListFileRequest req) {
        return ListFileResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(ListFileResponse.Payload.newBuilder().addAllFile(listFiles())).build();
      }

      @Override
      public void restart() {
        startListFileListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<ListFileResponse> responseObserver = asyncStub.listFile(requestObserver);
    requestObserver.start(responseObserver,
            ListFileResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started ListFile async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} RemoveFile RPC calls.
   */
  public void startRemoveFileListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<RemoveFileRequest, RemoveFileResponse>() {
      @Override
      public RemoveFileResponse exec(RemoveFileRequest req) {
        boolean success = removeFile(req.getPayload().getFileList());
        return RemoveFileResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(RemoveFileResponse.Payload.newBuilder().setSuccess(success)).build();
      }

      @Override
      public void restart() {
        startRemoveFileListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<RemoveFileResponse> responseObserver = asyncStub.removeFile(requestObserver);
    requestObserver.start(responseObserver,
            RemoveFileResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started RemoveFile async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} DetectPresto RPC calls.
   */
  public void startDetectPrestoListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<DetectPrestoRequest, DetectPrestoResponse>() {
      @Override
      public DetectPrestoResponse exec(DetectPrestoRequest req) {
        return DetectPrestoResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(detectPresto(req))
                .build();
      }

      @Override
      public void restart() {
        startDetectPrestoListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<DetectPrestoResponse> responseObserver = asyncStub.detectPresto(requestObserver);
    requestObserver.start(responseObserver,
            DetectPrestoResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started DetectPresto async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} ListCatalog RPC calls.
   */
  public void startListCatalogListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<ListCatalogRequest, ListCatalogResponse>() {
      @Override
      public ListCatalogResponse exec(ListCatalogRequest req) {
        PrestoCatalogListingResult result = listCatalogs(getUpdatedProps(
            configurationSetFor(AlluxioNodeType.MASTER))
            .get(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH));
        return ListCatalogResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(ListCatalogResponse.Payload.newBuilder().addAllCatalog(result
                        .getCatalogList()))
                .build();
      }

      @Override
      public void restart() {
        startListCatalogListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<ListCatalogResponse> responseObserver = asyncStub.listCatalog(requestObserver);
    requestObserver.start(responseObserver,
            ListCatalogResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started ListCatalog async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} SetPrestoConfDir
   * RPC calls.
   */
  public void startSetPrestoConfDirListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<SetPrestoConfDirRequest, SetPrestoConfDirResponse>() {
      @Override
      public SetPrestoConfDirResponse exec(SetPrestoConfDirRequest req) {
        Preconditions.checkArgument(req.hasPayload());
        Preconditions.checkArgument(req.getPayload().hasConfDir());
        return SetPrestoConfDirResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(setPrestoConfDir(req))
                .build();
      }

      @Override
      public void restart() {
        startSetPrestoConfDirListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<SetPrestoConfDirResponse> responseObserver = asyncStub
            .setPrestoConfDir(requestObserver);
    requestObserver.start(responseObserver,
            SetPrestoConfDirResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started SetPrestoConfDir async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} GetPrestoConfDir
   * RPC calls.
   */
  public void startGetPrestoConfDirListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<GetPrestoConfDirRequest, GetPrestoConfDirResponse>() {
      @Override
      public GetPrestoConfDirResponse exec(GetPrestoConfDirRequest req) {
        return GetPrestoConfDirResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(getPrestoConf())
                .build();
      }

      @Override
      public void restart() {
        startGetPrestoConfDirListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<GetPrestoConfDirResponse> responseObserver = asyncStub
            .getPrestoConfDir(requestObserver);
    requestObserver.start(responseObserver,
            GetPrestoConfDirResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started GetPrestoConfDir async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} ListMountPoints
   * RPC calls.
   */
  public void startListMountPointsListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<ListMountPointRequest, ListMountPointResponse>() {
      @Override
      public ListMountPointResponse exec(ListMountPointRequest req) {
        ListMountPointResponse.Payload p = getMountPointList();
        return ListMountPointResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(p)
                .build();
      }

      @Override
      public void restart() {
        startListMountPointsListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<ListMountPointResponse> responseObserver = asyncStub
            .listMountPoint(requestObserver);
    requestObserver.start(responseObserver,
            ListMountPointResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started ListMountPoints async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} ApplyMountPoint
   * RPC calls.
   */
  public void startApplyMountPointListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<ApplyMountPointRequest, ApplyMountPointResponse>() {
      @Override
      public ApplyMountPointResponse exec(ApplyMountPointRequest req) {
        return ApplyMountPointResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(applyMount(req))
                .build();
      }

      @Override
      public void restart() {
        startApplyMountPointListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<ApplyMountPointResponse> responseObserver = asyncStub
            .applyMountPoint(requestObserver);
    requestObserver.start(responseObserver,
            ApplyMountPointResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started ApplyMountPoint async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} DeleteMountPoint
   * RPC calls.
   */
  public void startDeleteMountPointListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<DeleteMountPointRequest, DeleteMountPointResponse>() {
      @Override
      public DeleteMountPointResponse exec(DeleteMountPointRequest req) {
        return DeleteMountPointResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(deleteMount(req.getPayload().getMountPoint()))
                .build();
      }

      @Override
      public void restart() {
        startDeleteMountPointListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<DeleteMountPointResponse> responseObserver = asyncStub
            .deleteMountPoint(requestObserver);
    requestObserver.start(responseObserver,
            DeleteMountPointResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started DeleteMountPoint async listener", asyncStub);
  }

  /**
   * Starts a request stream observer for {@link HostedManagerServiceGrpc} SpeedTest RPC calls.
   */
  public void startSpeedTestListener() {
    HostedManagerServiceGrpc.HostedManagerServiceStub asyncStub = getHostedAsyncStub();
    RequestStreamObserver requestObserver =
            new RequestStreamObserver<SpeedTestRequest, SpeedTestResponse>() {
      @Override
      public SpeedTestResponse exec(SpeedTestRequest req) {
        return SpeedTestResponse.newBuilder()
                .setHubMetadata(mHubMetadata)
                .setPayload(speedTest(req))
                .build();
      }

      @Override
      public void restart() {
        startSpeedTestListener();
      }

      @Override
      public void handleError(String message, Throwable t) {
        handleStatusRuntimeException(message, t);
      }
    };
    StreamObserver<SpeedTestResponse> responseObserver = asyncStub.speedTest(requestObserver);
    requestObserver.start(responseObserver,
            SpeedTestResponse.newBuilder().setHubMetadata(mHubMetadata).build());
    LOG.info("Started SpeedTest async listener", asyncStub);
  }

  /**
   * @return the {@link AlluxioCluster}
   */
  public AlluxioCluster getAlluxioCluster() {
    return mAlluxioCluster;
  }

  /**
   * @return the configuration associated with this context
   */
  public AlluxioConfiguration getConf() {
    return mConf;
  }

  /**
   * @return the {@link HubCluster}
   */
  public HubCluster getHubCluster() {
    return mHubCluster;
  }

  /**
   * Execute a function across all Hub agents.
   *
   * @param action a function that supplies a Hub agent client
   * @param <T> the response type that is returned from the client action
   * @return a mapping of responses from each hub agent the action was executed on to the
   *         agent's address
   */
  public <T> Map<HubNodeAddress, T> execOnHub(
          Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, T> action) {
    return mHubCluster.exec(mHubCluster.allNodes(), mConf, action, mSvc);
  }

  /**
   * Execute a function across a set of Alluxio nodes on the cluster.
   *
   * @param action a function that supplies a Hub agent client
   * @param hostname the host to run on
   * @param <T> the response type that is returned from the client action
   * @return a mapping of responses from each hub agent the action was executed on to the
   *         agent's address
   */
  public <T> Map<HubNodeAddress, T> execOnHub(
          Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, T> action,
          String hostname) {
    Set<HubNodeAddress> nodes = mHubCluster.allNodes().stream()
            .filter(x -> x.getHostname().equals(hostname)).collect(Collectors.toSet());
    return mHubCluster.exec(nodes, mConf, action, mSvc);
  }

  /**
   * Execute a function across a set of Alluxio nodes on the cluster.
   *
   * @param action a function that supplies a Hub agent client
   * @param type the hostname it should run on
   * @param <T> the response type that is returned from the client action
   * @return a mapping of responses from each hub agent the action was executed on to the
   *         agent's address
   */
  public <T> Map<HubNodeAddress, T> execOnHub(
          Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, T> action,
          AlluxioNodeType type) {
    return execOnHub(action, type, ExecutionType.PARALLEL);
  }

  /**
   * Execute a function across a set of Alluxio nodes on the cluster.
   *
   * @param action a function that supplies a Hub agent client
   * @param type the hostname it should run on
   * @param executionType parallel or serial
   * @param <T> the response type that is returned from the client action
   * @return a mapping of responses from each hub agent the action was executed on to the
   *         agent's address
   */
  public <T> Map<HubNodeAddress, T> execOnHub(
          Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, T> action,
          AlluxioNodeType type, ExecutionType executionType) {
    Set<HubNodeAddress> nodes = mHubCluster.nodesFromAlluxio(mAlluxioCluster, type);
    ExecutorService executorService = executionType == ExecutionType.PARALLEL
            ? mSvc : Executors.newSingleThreadExecutor();
    return mHubCluster.exec(nodes, mConf, action, executorService);
  }

  /**
   * Process state change request for Alluxio nodes.
   *
   * @param batchReq batch state change requests
   * @return a process status change response
   */
  public List<AgentProcessStatusChangeResponse> processStatusChange(
          ProcessStatusChangeRequest batchReq) {
    List<AgentProcessStatusChangeResponse> responses = new ArrayList<>();
    batchReq.getPayload().getProcessStatusChangeRequestList().forEach(req -> {
      Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub,
              AgentProcessStatusChangeResponse> x = (client) -> {
                AgentProcessStatusChangeResponse resp = client.processStatusChange(req);
                return resp;
              };
      List<AgentProcessStatusChangeResponse> agentResponses;
      if (req.hasHostname() && (!req.getHostname().isEmpty())) {
        agentResponses = execOnHub(x, req.getHostname())
                .values().stream().collect(Collectors.toList());
      } else {
        agentResponses = execOnHub(x, req.getNodeType(), req.getExecType())
                .values().stream().collect(Collectors.toList());
      }
      responses.addAll(agentResponses);
    });
    return responses;
  }

  /**
   * Register agent with hosted Hub.
   *
   * @param req registration request
   */
  public void registerAgent(RegisterAgentRequest req) {
    mHubCluster.add(req.getNode());
  }

  /**
   * Register agent with hosted Hub.
   *
   * @param req registration request
   */
  public void agentHearbeat(AgentHeartbeatRequest req) {
    mAlluxioCluster.setContext(this);
    mAlluxioCluster.heartbeat(req.getAlluxioStatus());
    mHubCluster.heartbeat(req.getHubNode());
  }

  /**
   * Sends an Alluxio cluster heartbeat to the Hosted Hub.
   * The {@link HubMetadata} will always try to use the latest cluster id to handle
   * journal formats. This allows the Hosted Hub to catch cluster id changes.
   * If the response fails, re-initialize the connection with the Hosted Hub with
   * the new cluster id.
   *
   * @param cluster alluxio cluster
   */
  public void alluxioClusterHeartbeat(alluxio.hub.proto.AlluxioCluster cluster) {
    boolean isMasterRunning = cluster.getNodeList().stream().anyMatch(
        n -> n.getProcessList().stream().anyMatch(
            p -> p.getNodeType().equals(AlluxioNodeType.MASTER)
                      && p.getState().equals(ProcessState.RUNNING)));
    try {
      // connect to Hosted Hub on first heartbeat containing running Alluxio master
      if (isMasterRunning) {
        if (mHubMetadata == null) {
          // HubMetadata not initialized, initialize bidi streams with Hosted Hub
          mHubMetadata = getHubMetadata();
          connectToHub(cluster);
          return;
        }
        // keep updating HubMetadata if master is up to get latest cluster id
        mHubMetadata = getHubMetadata();
      }
      AlluxioClusterHeartbeatResponse response = mHostedClient.get()
              .alluxioClusterHeartbeat(AlluxioClusterHeartbeatRequest.newBuilder()
              .setHubMetadata(mHubMetadata)
              .setPayload(AlluxioClusterHeartbeatRequest.Payload.newBuilder()
                      .setAlluxioCluster(cluster))
              .build());
      if (!response.getPayload().getSuccess()
              && response.getPayload().getStatus().equals(HubStatus.MANAGER_NOT_CONNECTED)) {
        connectToHub(cluster);
      }
    } catch (Throwable t) {
      handleStatusRuntimeException("Failed during alluxio cluster heartbeat", t);
    }
  }

  /**
   * Gets the configuration for a given alluxio node type.
   *
   * @param type the type of node to get the configuration from
   * @return a mapping of responses from each hub agent the action was executed on to the
   *         agent's address
   */
  public AlluxioConfigurationSet configurationSetFor(AlluxioNodeType type) {
    Set<HubNodeAddress> nodes = mHubCluster.nodesFromAlluxio(mAlluxioCluster, type);
    Iterator<HubNodeAddress> iter = nodes.iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("No nodes of type " + type + " to get configuration in the "
              + "cluster.");
    }
    HubNodeAddress addr = iter.next();
    Map<HubNodeAddress, AlluxioConfigurationSet> confSet =
            mHubCluster.exec(Collections.singleton(addr), mConf,
              (client) -> client.getConfigurationSet(AgentGetConfigurationSetRequest.newBuilder()
                              .build())
                            .getConfSet(),
                    mSvc);
    return confSet.get(addr);
  }

  /**
   * Updates the configuration for a set of Alluxio nodes.
   *
   * @param type the type of node to write the configuration to
   * @param conf the configuration to write
   * @return a set of the nodes which had their configuration updated
   */
  public Set<HubNodeAddress> updateConfigurationFor(AlluxioNodeType type,
                                                    AlluxioConfigurationSet conf) {
    Set<HubNodeAddress> nodes = mHubCluster.nodesFromAlluxio(mAlluxioCluster, type);
    Map<HubNodeAddress, AgentWriteConfigurationSetResponse> confSet =
            mHubCluster.exec(nodes, mConf,
              (client) -> client.writeConfigurationSet(AgentWriteConfigurationSetRequest
                      .newBuilder()
                            .setConfSet(conf)
                            .build()),
                    mSvc);
    // update the file system client to reflect the new conf
    if (type.equals(AlluxioNodeType.MASTER)) {
      updateFileSystemClient(conf);
    }
    // if running in K8s, update configmap
    if (mK8sConfig != null
            && (type.equals(AlluxioNodeType.MASTER) || type.equals(AlluxioNodeType.ALL))) {
      // We maintain a single configMap for all pods, so the config set is symmetric
      LOG.info("Persist configuration to K8s configMap {}", mK8sConfigMapName);
      try (KubernetesClient client = new DefaultKubernetesClient(mK8sConfig)) {
        String namespace = client.getNamespace();
        if (namespace == null) {
          LOG.info("Use K8s default namespace ");
          namespace = "default";
        }
        // Update configMap
        ConfigMap existingConfig =
                client.configMaps().inNamespace(namespace).withName(mK8sConfigMapName).get();
        if (existingConfig != null) {
          ConfigMapBuilder builder = new ConfigMapBuilder()
                  .withMetadata(existingConfig.getMetadata())
                  .withData(existingConfig.getData());
          // Replace log4 properties
          builder.addToData(K8S_CONFIG_MAP_ENV_LOG4J, conf.getLog4JProperties());
          // Replace site properties
          builder.addToData(K8S_CONFIG_MAP_ENV_SITE, conf.getSiteProperties());
          // Replace environment variables
          for (Map.Entry<String, String> entry : HubUtil
                  .getEnvVarsFromFile(conf.getAlluxioEnv(), LOG).entrySet()) {
            builder.addToData(entry.getKey(), entry.getValue());
          }
          // Write config map
          client.configMaps().inNamespace(namespace).withName(mK8sConfigMapName)
                  .replace(builder.build());
          LOG.info("Updated K8s configMap {}", mK8sConfigMapName);
        }
      } catch (Exception e) {
        LOG.error("Unable to persist configuration to K8s configMap {}", mK8sConfigMapName, e);
      }
    }
    return confSet.keySet();
  }

  private void updateFileSystemClient(AlluxioConfigurationSet configurationSet) {
    mFileSystem = FileSystem.Factory.create(
            FileSystemContext.create(getUpdatedProps(configurationSet)));
  }

  private DetectPrestoResponse.Payload detectPresto(DetectPrestoRequest request) {
    AgentDetectPrestoRequest detectRequest = AgentDetectPrestoRequest.newBuilder()
            .setConfDir(request.getPayload().getConfDir()).build();
    boolean workers = execOnHub(client -> client.detectPresto(detectRequest),
            AlluxioNodeType.WORKER).values().stream()
            .map(AgentDetectPrestoResponse::getDetected)
            .reduce(Boolean::logicalAnd)
            .orElse(false);
    boolean result = workers
            && execOnHub(client -> client.detectPresto(detectRequest),
            AlluxioNodeType.MASTER).values().stream()
            .map(AgentDetectPrestoResponse::getDetected)
            .reduce(Boolean::logicalAnd)
            .orElse(false);
    return DetectPrestoResponse.Payload.newBuilder()
            .setDetected(result)
            .build();
  }

  /**
   * Get presto conf.
   * @return presto conf
   */
  public GetPrestoConfDirResponse.Payload getPrestoConf() {
    String path = getUpdatedProps(configurationSetFor(AlluxioNodeType.MASTER))
            .get(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH);
    return GetPrestoConfDirResponse.Payload.newBuilder().setConfDir(path).setIsDefault(
            path.equals(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getDefaultValue())).build();
  }

  /**
   * Get updated cluster configuration from the configuration set.
   *
   * @param configSet cluster config set
   * @return Alluxio configuration
   */
  public AlluxioConfiguration getUpdatedProps(AlluxioConfigurationSet configSet) {
    if (!configSet.hasSiteProperties()) {
      return ServerConfiguration.global();
    }
    Properties props = ConfigurationUtils.loadProperties(
            new ByteArrayInputStream(configSet.getSiteProperties().getBytes()));
    AlluxioProperties alluxioProperties = ConfigurationUtils.defaults().copy();
    alluxioProperties.merge(props, CLUSTER_DEFAULT);
    return new InstancedConfiguration(alluxioProperties);
  }

  /**
   * Get an instance of the alluxio file system.
   *
   * @return alluxio file system or null
   */
  public FileSystem getFileSystem() {
    if (mFileSystem == null) {
      AlluxioConfigurationSet configSet = configurationSetFor(AlluxioNodeType.MASTER);
      updateFileSystemClient(configSet);
    }
    return mFileSystem;
  }

  @Override
  public void close() throws IOException {
    mSvc.shutdownNow();
    if (mFileSystem != null) {
      mFileSystem.close();
    }
  }

  /**
   * Get a list of mount points and their information.
   *
   * @return a MountPointList object
   */
  public ListMountPointResponse.Payload getMountPointList() {
    List<alluxio.hub.proto.MountPointInfo> mountPoints = new ArrayList<>();
    FileSystem fs = getFileSystem();
    if (fs == null) {
      LOG.debug("Unable to get file system client");
      return ListMountPointResponse.Payload.getDefaultInstance();
    }
    try {
      Map<String, MountPointInfo> mountTable = fs.getMountTable();
      Set<String> syncSet = fs.getSyncPathList().stream()
              .map(syncPoint -> syncPoint.getSyncPointUri().toString())
              .collect(Collectors.toSet());
      for (Map.Entry<String, MountPointInfo> entry: mountTable.entrySet()) {
        alluxio.hub.proto.MountPointInfo.Builder builder =
                alluxio.hub.proto.MountPointInfo.newBuilder();
        MountPointInfo mountInfo = entry.getValue();
        // Generic mount point info
        builder.setAlluxioPath(entry.getKey())
                .setUfsUri(mountInfo.getUfsUri()).setUfsType(mountInfo.getUfsType())
                .setReadOnly(mountInfo.getReadOnly()).setShared(mountInfo.getShared());
        Map<String, String> prop = new HashMap<>(mountInfo.getProperties());
        // remove properties that are explicitly shown as options
        prop.remove(PropertyKey.UNDERFS_HDFS_CONFIGURATION.getName());

        // HDFS specific mount point info
        if (mountInfo.getUfsType().toLowerCase().equals("hdfs")) {
          prop.remove(PropertyKey.UNDERFS_VERSION.getName());
          HDFSMountPointInfo.Builder hdfsBuilder = HDFSMountPointInfo.newBuilder();
          String[] confFileNames = mountInfo.getProperties()
                  .getOrDefault(PropertyKey.UNDERFS_HDFS_CONFIGURATION.getName(), "")
                  .split(":");
          String coreSiteFileName = "";
          String hdfsSiteFileName = "";
          for (String confFile : confFileNames) {
            if (confFile.contains("hdfs-site.xml")) {
              hdfsSiteFileName = confFile;
            } else if (confFile.contains("core-site.xml")) {
              coreSiteFileName = confFile;
            }
          }
          String hdfsVersion = mountInfo.getProperties().getOrDefault(
                  PropertyKey.UNDERFS_VERSION.getName(), "");
          // Use file name in place of file content for now
          // Need to verify syncing through active sync api
          hdfsBuilder.setEnableSync(syncSet.contains(entry.getKey()))
                  .setCoreSiteFilePath(coreSiteFileName)
                  .setHdfsSiteFilePath(hdfsSiteFileName).setHdfsVersion(hdfsVersion);
          builder.setHdfsInfo(hdfsBuilder);
        }
        builder.putAllProperties(prop);
        mountPoints.add(builder.build());
      }
    } catch (AlluxioException e) {
      LOG.debug("getMountTable failed due to " + e.getMessage());
      return ListMountPointResponse.Payload.getDefaultInstance();
    } catch (IOException e) {
      LOG.debug("RPC to get mount table failed due to " + e.getMessage());
      return ListMountPointResponse.Payload.getDefaultInstance();
    }

    return ListMountPointResponse.Payload.newBuilder().addAllMountPoint(mountPoints).build();
  }

  private Map<String, String> constructProp(alluxio.hub.proto.MountPointInfo mountPointInfo,
                                            boolean writeFile) {
    Map<String, String> properties = new HashMap<>(mountPointInfo.getPropertiesMap());
    return properties;
  }

  /**
   * Apply mount hdfs command.
   *
   * @param req validated request
   * @return response
   */
  public ApplyMountPointResponse.Payload applyMount(ApplyMountPointRequest req) {
    ApplyMountPointRequest.Payload p = req.getPayload();
    ApplyMountPointResponse.Payload.Builder resultBuilder = ApplyMountPointResponse.Payload
            .newBuilder();
    alluxio.hub.proto.MountPointInfo mountPointInfo = p.getNew();
    Map<String, String> properties = constructProp(mountPointInfo, false);
    MountPOptions.Builder optionBuilder = MountPOptions.newBuilder()
            .setReadOnly(mountPointInfo.getReadOnly())
            .setShared(mountPointInfo.getShared()).putAllProperties(properties);
    boolean needToRemount = false;
    try {
      boolean needToMount = true;
      if (p.hasBefore()) {
        AlluxioURI originalMount = new AlluxioURI(p.getBefore().getAlluxioPath());
        if (originalMount.isRoot() && (!(p.getBefore().getAlluxioPath()
                .equals(p.getNew().getAlluxioPath())
                && p.getBefore().getUfsUri()
                .equals(p.getNew().getUfsUri())))) {
          throw new IOException("Cannot change the mount location or ufs location of a root mount");
        }
        // Edit mount options
        // Use updateMount if we are operating on rootmount or
        // if both mount uri and ufs uri has not changed
        if (originalMount.isRoot()
                || (p.getBefore().getAlluxioPath()
                .equals(p.getNew().getAlluxioPath())
                && p.getBefore().getUfsUri()
                .equals(p.getNew().getUfsUri()))) {
          getFileSystem().updateMount(originalMount, optionBuilder.build());
          needToMount = false;
        } else {
          getFileSystem().unmount(originalMount);
          needToRemount = true;
        }
      }
      if (needToMount) {
        getFileSystem().mount(new AlluxioURI(mountPointInfo.getAlluxioPath()),
                new AlluxioURI(mountPointInfo.getUfsUri()), optionBuilder.build());
        needToRemount = false;
      }
      if (mountPointInfo.hasHdfsInfo() && mountPointInfo.getHdfsInfo().getEnableSync()) {
        getFileSystem().startSync(new AlluxioURI(mountPointInfo.getAlluxioPath()));
      }
      resultBuilder.setSuccess(true);
    } catch (Exception e) {
      if (needToRemount) {
        alluxio.hub.proto.MountPointInfo oldMountPoint = p.getBefore();
        MountPOptions.Builder oldOptionBuilder = MountPOptions.newBuilder()
                .setReadOnly(oldMountPoint.getReadOnly())
                .setShared(oldMountPoint.getShared())
                .putAllProperties(constructProp(oldMountPoint, false));
        try {
          getFileSystem().mount(new AlluxioURI(oldMountPoint.getAlluxioPath()),
                  new AlluxioURI(oldMountPoint.getUfsUri()), oldOptionBuilder.build());
        } catch (Exception ex) {
          LOG.warn("Failed to restore the mount point " + oldMountPoint.getAlluxioPath()
                  + " to previous state");
          resultBuilder.addError(ValidationUtils.getErrorInfo(ex));
        }
      }
      LOG.warn("Failed applying mount settings due to " + ValidationUtils.getErrorInfo(e));
      resultBuilder.setSuccess(false);
      resultBuilder.addError(ValidationUtils.getErrorInfo(e));

      // BEGIN Advice handling
      // =====================
      if (e instanceof InvalidPathException) {
        if (exceptionMatchesPattern(e, "mount point.*already exists.*")) {
          resultBuilder.setAdvice("Another file, directory, or mount point already exists at this"
                  + " path. Check the Alluxio path you are mounting to again to make sure it"
                  + " doesn't conflict with paths that already exist.");
        } else if (exceptionMatchesPattern(e, "mount point.*is a prefix of.*")) {
          resultBuilder.setAdvice("A mount point with a similar UFS URI is already mounted to"
                  + " Alluxio. Make sure that your UFS URI doesn't have a namespace overlap with"
                  + " another existing mount point");
        } else if (exceptionMatchesPattern(e, ".*path.*is invalid.*")) {
          resultBuilder.setAdvice("We couldn't parse your Alluxio path. Make sure to provide the"
                  + " absolute path to where the new mount point should exist.");
        }
      } else if (e instanceof FileDoesNotExistException) {
        if (exceptionMatchesPattern(e, "file.*creation failed. component.*does not exist")) {
          resultBuilder.setAdvice("One of components in the path you are trying to mount to does"
                  + " not exist. Make sure that all path components in the Alluxio namespace you"
                  + " are trying to mount at exists, except for the last one.");
        }
      } else if (e instanceof AlluxioException) {
        if (exceptionMatchesPattern(e, ".*no underfilesystem factory found for.*")) {
          resultBuilder.setAdvice(String.format("Alluxio couldn't find a matching library with the"
                  + " URI (%s) and version (%s) provided. This means that you're trying to mount a"
                  + " URI with an unsupported scheme (<scheme>://) or, that Alluxio doesn't have"
                  + " an available version of the UFS type you're trying to mount.",
                  mountPointInfo.getUfsUri(),
                  p.getNew().hasHdfsInfo()
                          ? p.getNew().getHdfsInfo().getHdfsVersion()
                          : "N/A"));
        } else if (exceptionMatchesPattern(e, ".*ufs path.*does not exist.*")) {
          resultBuilder.setAdvice("The UFS path you are trying to mount does not exist in the UFS."
                  + " Please make sure the UFS path specified exists.");
        } else if (exceptionMatchesPattern(e, ".*java.net.unknownhostexception:.*")) {
          resultBuilder.setAdvice(String.format("We weren't able to resolve the hostname in your"
                  + " HDFS uri %s. Please double check that you've typed the hostname correctly and"
                  + " that the system DNS can resolve the hostname.", mountPointInfo.getUfsUri()));
        } else if (exceptionMatchesPattern(e,
                "call from.*to.*failed on connection exception.*")) {
          resultBuilder.setAdvice("We were able to resolve the hostname of your URI, but failed to"
                  + " connect. Double check that the hostname and port are correct. Then, if it"
                  + " still fails verify that there is no firewall blocking connections between"
                  + " this machine and the UFS URI specified.");
        }
      }
      // END Advice handling
      // ===================
    }
    return resultBuilder.build();
  }

  private static boolean exceptionMatchesPattern(Exception e, String regex) {
    return e.getMessage().toLowerCase().matches(regex);
  }

  /**
   * Gets a list of known UDBs from an Alluxio master.
   *
   * @param confDir the presto configuration directory to search for catalogs
   * @return the validation results
   */
  public PrestoCatalogListingResult listCatalogs(String confDir) {
    Set<HubNodeAddress> masters =
            mHubCluster.nodesFromAlluxio(mAlluxioCluster, AlluxioNodeType.MASTER);
    PrestoCatalogListingResult.Builder res = PrestoCatalogListingResult.newBuilder();
    if (masters.size() == 0) {
      return res.build();
    }
    mHubCluster.exec(Collections.singleton(masters.iterator().next()), mConf,
      client -> client.listCatalogs(AgentListCatalogRequest.newBuilder()
            .setConfDir(confDir).build()), mSvc)
            .values()
            .iterator()
            .next().getCatalogList().forEach(res::addCatalog);
    return res.build();
  }

  /**
   * List all files.
   *
   * @return a list of uploaded file info
   */
  public List<ListFile> listFiles() {
    List<AlluxioNodeType> nodeTypes = ImmutableList.of(AlluxioNodeType.MASTER,
            AlluxioNodeType.WORKER);
    return nodeTypes.stream().map(type -> execOnHub(
      (client) -> new Pair<>(type, client.listFile(AgentListFileRequest.getDefaultInstance())),
                    type))
            .flatMap(x -> x.values().stream().flatMap(pair
                -> pair.getSecond().getFileInfoList().stream().map(
                  fileInfo -> ListFile.newBuilder().setLocation(pair.getFirst())
                            .setProcessType(fileInfo.getProcessType())
                            .setName(fileInfo.getFileName()).build())))
            .collect(Collectors.toList());
  }

  /**
   * Add new files.
   *
   * @param fileInfoList add a list of file add request
   * @return true if adds are successful
   */
  public boolean uploadFile(List<UploadFile> fileInfoList) {
    return fileInfoList.stream().map(fileInfo -> execOnHub((client) ->
                    client.uploadFile(AgentFileUploadRequest.newBuilder()
                            .setFilePath(fileInfo.getName())
                            .setProcessType(fileInfo.getProcessType())
                            .setPermission(fileInfo.getPermission())
                            .setFileContent(ByteString.copyFrom(Base64.getDecoder().decode(
                                    fileInfo.getContent().getBytes(StandardCharsets.UTF_8))))
                            .build()), fileInfo.getDestination()))
            .allMatch(x -> x.values().stream().allMatch(AgentFileUploadResponse::getSuccess));
  }

  /**
   * Remove a list of files.
   *
   * @param filesList a list of files to be removed
   * @return true if file removal successful
   */
  public boolean removeFile(List<RemoveFile> filesList) {
    return filesList.stream().map(fileInfo -> execOnHub((client) ->
                    client.removeFile(AgentRemoveFileRequest.newBuilder()
                            .setFileName(fileInfo.getName())
                            .setProcessType(fileInfo.getProcessType())
                            .build()), fileInfo.getLocation()))
            .allMatch(x -> x.values().stream().allMatch(AgentRemoveFileResponse::getSuccess));
  }

  private static SpeedStat convertToProto(IOTaskSummary.SpeedStat speedStat) {
    return SpeedStat.newBuilder()
            .setAvgSpeedMbps(speedStat.mAvgSpeedMbps)
            .setMaxSpeedMbps(speedStat.mMaxSpeedMbps).setMinSpeedMbps(speedStat.mMinSpeedMbps)
            .setStdDev(speedStat.mStdDev).setTotalDurationSeconds(speedStat.mTotalDurationSeconds)
            .setTotalSizeBytes(speedStat.mTotalSizeBytes)
            .build();
  }

  /**
   * speed test for ufs.
   *
   * @param request ufs speed test request
   * @return result of the test
   */
  public SpeedTestResponse.Payload speedTest(SpeedTestRequest request) {
    SpeedTestRequest.Payload req = request.getPayload();
    String dataSize = req.getDataSize();
    String mountPoint = req.getMountPoint();
    String relativePath = req.getPath();
    int clusterParallelism = req.getClusterParallelism();
    int nodeParallelism = req.getNodeParallelism();
    List<String> errors = new ArrayList<>();
    MountPointInfo mountInfo = null;
    try {
      mountInfo = getFileSystem().getMountTable().get(mountPoint);
    } catch (IOException | AlluxioException e) {
      errors.add("Failed to retrieve mount point information");
      errors.add(ValidationUtils.getErrorInfo(e));
    }
    if (mountInfo == null) {
      errors.add("Failed to retrieve mount point information for " + mountPoint);
      return SpeedTestResponse.Payload.newBuilder().addAllError(errors).build();
    }
    if (clusterParallelism > mAlluxioCluster.size()) {
      errors.add("Can not set cluster parallelism to be " + clusterParallelism
              + " which is greater than cluster size " + mAlluxioCluster.size());
      return SpeedTestResponse.Payload.newBuilder().addAllError(errors).build();
    }
    if (relativePath.isEmpty()) {
      relativePath = "/";
    }
    String ufsPath = PathUtils.normalizePath(
            PathUtils.concatPath(mountInfo.getUfsUri(), relativePath), AlluxioURI.SEPARATOR);
    List<String> argList = new ArrayList<>();
    String[] args = new String[]{
        "--io-size", dataSize,
        "--threads", String.valueOf(nodeParallelism), "--path", ufsPath,
        "--cluster-limit", String.valueOf(clusterParallelism), "--cluster", "--use-mount-conf"
    };
    Collections.addAll(argList, args);

    UfsIOBench bench = getIOBench();
    try {
      String output = bench.run(argList.toArray(new String[0]));
      IOTaskSummary result = (IOTaskSummary) JsonSerializable.fromJson(output);
      SpeedTestResponse.Payload.Builder builder = SpeedTestResponse.Payload.newBuilder();
      if (result.getErrors() != null) {
        builder.addAllError(result.getErrors());
      }
      if (result.getParameters() != null) {
        UfsIOParameters param = result.getParameters();
        builder.setParameters(SpeedTestParameter.newBuilder()
                .setDataSize(param.mDataSize).setPath(param.mPath)
                .setThreads(param.mThreads).putAllProperties(param.mConf).build());
      }
      builder.setReadSpeedStat(convertToProto(result.getReadSpeedStat()))
              .setWriteSpeedStat(convertToProto(result.getWriteSpeedStat()));
      return builder.build();
    } catch (Exception e) {
      errors.add("UfsIOBench failed due to " + ValidationUtils.getErrorInfo(e));
    }
    return SpeedTestResponse.Payload.newBuilder().addAllError(errors).build();
  }

  @VisibleForTesting
  UfsIOBench getIOBench() {
    return new UfsIOBench();
  }

  /**
   * Set Presto configuration directory.
   * @param request presto conf directory request
   * @return response object
   */
  public SetPrestoConfDirResponse.Payload setPrestoConfDir(SetPrestoConfDirRequest request) {
    String confDir = request.getPayload().getConfDir();
    SetPrestoConfDirResponse.Payload.Builder builder = SetPrestoConfDirResponse.Payload
            .newBuilder();
    List<AlluxioNodeType> nodeTypes = ImmutableList.of(AlluxioNodeType.MASTER,
            AlluxioNodeType.WORKER);
    Map<String, String> map = ImmutableMap.of(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getName(),
            confDir);
    if (nodeTypes.stream().map(type -> execOnHub((client) ->
                    client.setPrestoConfDir(AgentSetPrestoConfRequest.newBuilder().putAllProps(map)
                            .build()),
            type)).allMatch(response -> response.values().stream()
            .allMatch(AgentSetPrestoConfResponse::getSuccess))) {
      return builder.setSuccess(true).setConfDir(confDir)
              .setIsDefault(confDir.equals(
                      PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getDefaultValue())).build();
    } else {
      String oldConfDir = getUpdatedProps(configurationSetFor(AlluxioNodeType.MASTER))
              .get(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH);
      return builder.setSuccess(false).setConfDir(oldConfDir).setIsDefault(
              oldConfDir.equals(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getDefaultValue()))
              .build();
    }
  }

  /**
   * Remove mount point.
   * @param mountPointInfo mount point info
   * @return response
   */
  public DeleteMountPointResponse.Payload deleteMount(
          alluxio.hub.proto.MountPointInfo mountPointInfo) {
    if (mountPointInfo.getAlluxioPath().equals("/")) {
      return DeleteMountPointResponse.Payload.newBuilder()
              .setDeleted(false).addError("Can not remove the root mount point").build();
    }
    try {
      getFileSystem().unmount(new AlluxioURI(mountPointInfo.getAlluxioPath()));
    } catch (IOException | AlluxioException e) {
      return DeleteMountPointResponse.Payload.newBuilder()
              .setDeleted(false).addError(e.getMessage()).build();
    }
    return DeleteMountPointResponse.Payload.newBuilder().setDeleted(true).build();
  }
}
