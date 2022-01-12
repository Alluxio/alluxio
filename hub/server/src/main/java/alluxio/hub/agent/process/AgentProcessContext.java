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

package alluxio.hub.agent.process;

import static alluxio.conf.Source.CLUSTER_DEFAULT;
import static alluxio.hub.agent.util.file.PrestoCatalogUtils.getProcessInfo;

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.hub.agent.util.conf.ConfigurationEditor;
import alluxio.hub.agent.util.file.FileManager;
import alluxio.hub.agent.util.file.PrestoCatalogUtils;
import alluxio.hub.agent.util.file.SimpleFileManager;
import alluxio.hub.agent.util.process.NodeStatus;
import alluxio.hub.agent.util.process.ProcessLauncher;
import alluxio.hub.common.RpcClient;
import alluxio.hub.proto.AgentHeartbeatRequest;
import alluxio.hub.proto.AgentListFileInfo;
import alluxio.hub.proto.AlluxioConfigurationSet;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.ManagerAgentServiceGrpc;
import alluxio.hub.proto.ProcessState;
import alluxio.hub.proto.ProcessStateChange;
import alluxio.hub.proto.RegisterAgentRequest;
import alluxio.hub.proto.UploadProcessType;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.ConfigurationUtils;
import alluxio.util.LogUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This context holds information and methods critical to the function of the Hub Agent.
 * This object may be safely shared between multiple threads.
 */
@ThreadSafe
public class AgentProcessContext implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(AgentProcess.class);
  public static final String UPLOAD_SUBDIR = "upload";

  private final RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> mClient;
  private final AlluxioConfiguration mConf;
  private final ScheduledExecutorService mSvc;
  private final Map<AlluxioNodeType, Object> mProcs;
  private final ProcessLauncher mLauncher;
  private final boolean mNoStart;
  private final Map<UploadProcessType, FileManager> mFileManager;

  private ScheduledFuture<?> mHeartbeat;

  /**
   * Creates a new instance of {@link AgentProcessContext}.
   *
   * @param conf the Alluxio Configuration
   * @param rpcClient the client to use to make RPCs
   * @param processTypes the mapping of process types to use
   * @param launcher the process launcher to use
   * @throws Exception if there are issues initiating the state of the context. This may result
   *                   from issues reading the process table, registering the agent, or starting the
   *                   heartbeat.
   */
  AgentProcessContext(AlluxioConfiguration conf,
      RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> rpcClient,
      Map<AlluxioNodeType, Object> processTypes,
      ProcessLauncher launcher) throws Exception {
    mConf = conf;
    mSvc = new ScheduledThreadPoolExecutor(
        mConf.getInt(PropertyKey.HUB_AGENT_EXECUTOR_THREADS_MIN),
        ThreadFactoryUtils.build("AgentProcessContext-%d", true));
    mClient = rpcClient;
    RetryPolicy retry = new ExponentialBackoffRetry(50, 1000,
            130); // Max time ~2 min
    mProcs = processTypes;
    mLauncher = launcher;
    mNoStart = System.getenv("ALLUXIO_HUB_NO_START") != null;
    mFileManager = new HashMap<>();
    registerAgent(retry);
    startHeartbeat();
  }

  private FileManager getFileManager(UploadProcessType uploadType) {
    return mFileManager.computeIfAbsent(uploadType, key -> {
      Pair<String, String> processInfo = getProcessInfo(uploadType);
      String rootDir;
      if (key.equals(UploadProcessType.ALLUXIO)) {
        rootDir = PathUtils.concatPath(mConf.get(PropertyKey.CONF_DIR), UPLOAD_SUBDIR);
      } else {
        rootDir = PathUtils.concatPath(mConf.get(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH),
            UPLOAD_SUBDIR);
      }
      return new SimpleFileManager(rootDir, processInfo.getFirst(), processInfo.getSecond());
    });
  }

  static Map<AlluxioNodeType, Object> getRunningProcessTypes() throws IOException {
    NodeStatus ns = new NodeStatus();
    return Arrays.stream(AlluxioNodeType.values())
        .filter(x -> !x.equals(AlluxioNodeType.ALL))
        .map(type -> {
          try {
            return ns.getProcessStatus(type);
          } catch (IOException e) {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .filter(s -> s.getState() == ProcessState.RUNNING)
        .map(AlluxioProcessStatus::getNodeType)
        .collect(Collectors.toMap((n) -> n, (n) -> new Object()));
  }

  /**
   * Creates a new instance of {@link AgentProcessContext}.
   *
   * @param conf the Alluxio Configuration
   * @throws Exception if the context fails to initialize
   */
  public AgentProcessContext(AlluxioConfiguration conf) throws Exception {
    this(conf, new RpcClient<>(conf, NetworkAddressUtils
            .getConnectAddress(NetworkAddressUtils.ServiceType.HUB_MANAGER_RPC, conf),
            ManagerAgentServiceGrpc::newBlockingStub, () -> ExponentialTimeBoundedRetry.builder()
            .withSkipInitialSleep()
            .withMaxDuration(
                Duration.ofMillis(conf.getMs(PropertyKey.HUB_AGENT_HEARTBEAT_INTERVAL)))
            .build()),
        getRunningProcessTypes(),
        new ProcessLauncher(conf));
  }

  /**
   * Creates a new instance of {@link AgentProcessContext}.
   *
   * @param conf the Alluxio Configuration
   * @param client the RPC client to use
   * @throws Exception if the context fails to initialize
   */
  public AgentProcessContext(AlluxioConfiguration conf,
      RpcClient<ManagerAgentServiceGrpc.ManagerAgentServiceBlockingStub> client) throws Exception {
    this(conf, client, getRunningProcessTypes(), new ProcessLauncher(conf));
  }

  @Override
  public void close() {
    if (mHeartbeat != null) {
      mHeartbeat.cancel(true);
      mHeartbeat = null;
    }
    mSvc.shutdownNow();
  }

  private void heartbeat() {
    try {
      NodeStatus ps = new NodeStatus();
      List<AlluxioProcessStatus> alluxioStatus = mProcs.keySet().stream()
          .map(t -> {
            try {
              return ps.getProcessStatus(t);
            } catch (IOException e) {
              LogUtils.warnWithException(LOG, "Failed to get process status for {}", t, e);
              return AlluxioProcessStatus.newBuilder()
                  .setNodeType(t)
                  .setState(ProcessState.UNKNOWN)
                  .setPid(-1)
                  .build();
            }
          })
          .collect(Collectors.toList());
      AlluxioNodeStatus s = AlluxioNodeStatus.newBuilder()
          .setHostname(NetworkAddressUtils
              .getConnectHost(NetworkAddressUtils.ServiceType.HUB_AGENT_RPC, mConf))
          .addAllProcess(alluxioStatus)
          .build();
      AgentHeartbeatRequest request = AgentHeartbeatRequest.newBuilder()
          .setHubNode(getNodeAddress())
          .setAlluxioStatus(s)
          .build();
      mClient.get().agentHeartbeat(request);
    } catch (Throwable t) {
      LogUtils.warnWithException(LOG, "Failed to send agent heartbeat", t);
    }
  }

  /**
   * @return A set containing the types of Alluxio nodes that this agent is responsible for
   */
  public Set<AlluxioNodeType> alluxioProcesses() {
    return Collections.unmodifiableSet(mProcs.keySet());
  }

  void registerAgent(RetryPolicy retry) throws Exception {
    while (retry.attempt()) {
      try {
        RegisterAgentRequest req = RegisterAgentRequest.newBuilder()
            .setNode(getNodeAddress())
            .build();
        mClient.get().registerAgent(req);
        LOG.info("Successfully registered agent");
        return;
      } catch (Throwable t) {
        LogUtils.warnWithException(LOG, "Failed to register agent", t);
      }
    }
    throw new Exception("Failed to register agent after " + retry.getAttemptCount() + " attempts");
  }

  protected HubNodeAddress getNodeAddress() {
    InetSocketAddress addr = NetworkAddressUtils
        .getConnectAddress(NetworkAddressUtils.ServiceType.HUB_AGENT_RPC, mConf);
    return HubNodeAddress.newBuilder()
        .setHostname(addr.getHostName())
        .setRpcPort(addr.getPort()).build();
  }

  void startHeartbeat() throws Exception {
    if (mHeartbeat != null) {
      throw new Exception("Heartbeat scheduler is already started");
    }
    mHeartbeat = mSvc.scheduleAtFixedRate(
        this::heartbeat, 0, mConf.getMs(PropertyKey.HUB_AGENT_HEARTBEAT_INTERVAL),
            TimeUnit.MILLISECONDS);
  }

  /**
   * Attempts to change the state of a process.
   *
   * An action of {@link ProcessStateChange#RESTART} will result in the same state but guarantee
   * that the desired process was stopped and then started.
   *
   * @param type the process to perform the action on
   * @param action the action to perform on the process
   * @throws IOException if there is a failure to obtain the current state of the process
   * @throws TimeoutException if a process fails to start in a timely manner
   * @throws InterruptedException if the thread handling is interrupted during a start or stop
   */
  public void changeState(AlluxioNodeType type, ProcessStateChange action)
      throws IOException, TimeoutException, InterruptedException {
    Preconditions.checkArgument(mProcs.containsKey(type),
            "this node cannot start this process");
    Object sync = mProcs.get(type);
    if (sync == null) {
      throw new RuntimeException("null sync object in process tracking map for key: " + type);
    }
    synchronized (sync) {
      switch (action) {
        case RESTART:
          changeState(type, ProcessStateChange.STOP);
          changeState(type, ProcessStateChange.START);
          break;
        case STOP:
          mLauncher.stop(type);
          heartbeat();
          break;
        case START:
          if (!mNoStart) {
            mLauncher.start(type);
          }
          heartbeat();
          break;
        default:
          throw new IllegalArgumentException("Unknown or unimplemented state change request "
                  + action);
      }
    }
  }

  /**
   * Gets the configuration for the current node.
   *
   * @return an {@link AlluxioConfigurationSet} representing the node's configuration on disk
   * @throws IOException if there are any failures reading the configuration
   */
  public AlluxioConfigurationSet getConf() throws IOException {
    return new ConfigurationEditor(mConf.get(PropertyKey.SITE_CONF_DIR)).readConf();
  }

  /**
   * Writes the configuration for the current node.
   *
   * Upon failure, no attempt is made to undo or roll back any changes already made.
   *
   * @param conf the configuration to write to disk
   * @throws IOException if there are any failures writing to disk
   */
  public void writeConf(AlluxioConfigurationSet conf) throws IOException {
    new ConfigurationEditor(mConf.get(PropertyKey.SITE_CONF_DIR)).writeConf(conf);
  }

  /**
   * Get Property from file.
   *
   * @param key property key
   * @return property value
   */
  public String getPropertyFromFile(PropertyKey key) {
    Properties fileProps = null;
    try {
      fileProps = ConfigurationUtils.loadProperties(
          new ByteArrayInputStream(getConf().getSiteProperties().getBytes()));
    } catch (IOException | RuntimeException e) {
      fileProps = new Properties();
    }
    AlluxioProperties alluxioProperties = ConfigurationUtils.defaults().copy();
    alluxioProperties.merge(fileProps, CLUSTER_DEFAULT);
    return alluxioProperties.get(key);
  }

  private boolean validatePrestoConf(String confPath) {
    Path path = Paths.get(confPath);
    return Files.isDirectory(path)
        && Files.isDirectory(Paths.get(confPath, "catalog"));
  }

  private boolean validateConf(Properties props) {
    try {
      if (props.containsKey(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getName())) {
        String confPath = props.getProperty(
            PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getName());
        if (!validatePrestoConf(confPath)) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  /**
   * Update alluxio site property values.
   * @param props properties to be updated
   * @return true if property is updated
   */
  public boolean updateConf(Properties props) {
    Properties fileProps = null;
    if (!validateConf(props)) {
      return false;
    }
    try {
      fileProps = ConfigurationUtils.loadProperties(
          new ByteArrayInputStream(getConf().getSiteProperties().getBytes()));
    } catch (Exception e) {
      LOG.info("Exception when reading old configuration " + e);
    }

    if (fileProps == null) {
      fileProps = props;
    } else {
      fileProps.putAll(props);
    }

    try (StringWriter stringWriter = new StringWriter()) {
      fileProps.store(stringWriter,
          "Updated Alluxio site properties generated by Alluxio hub agent");
      AlluxioConfigurationSet conf = AlluxioConfigurationSet.newBuilder()
          .setSiteProperties(stringWriter.toString()).build();
      Path path = Paths.get(mConf.get(PropertyKey.CONF_DIR),
          ConfigurationEditor.SITE_PROPERTIES);
      if (Files.notExists(path)) {
        Files.createFile(path);
      }
      new ConfigurationEditor(mConf.get(PropertyKey.SITE_CONF_DIR)).writeConf(conf);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Upload a file to the current node.
   *
   * @param filePath file path
   * @param permission permission string such as 0777
   * @param fileContent file content
   * @param uploadType upload destination type
   * @return true if upload is successful
   */
  public boolean uploadFile(String filePath, String permission, ByteString fileContent,
      UploadProcessType uploadType) {
    return getFileManager(uploadType).addFile(filePath, permission, fileContent.toByteArray());
  }

  /**
   * Upload a file.
   *
   * @param fileName the name of the file
   * @param uploadProcessType upload type
   * @return true if upload was successful
   */
  public boolean removeFile(String fileName, UploadProcessType uploadProcessType) {
    return getFileManager(uploadProcessType).removeFile(fileName);
  }

  /**
   * List files.
   *
   * @return a list of file info
   */
  public List<AgentListFileInfo> listFile() {
    return Arrays.stream(UploadProcessType.values())
        .flatMap(processType -> getFileManager(processType).listFile().stream().map(fileName ->
            AgentListFileInfo.newBuilder().setFileName(fileName)
                .setProcessType(processType).build()))
        .collect(Collectors.toList());
  }

  /**
   * Restarts the local instance of presto on this node.
   *
   * Blocks until completed, or throws an {@link IOException} upon failure.
   */
  public void restartPresto() throws IOException {
    try {
      PrestoCatalogUtils.restartPrestoEmr();
    } catch (IOException e) {
      LOG.debug("Couldn't restart presto via EMR method, attempting dataproc method", e);
      try {
        PrestoCatalogUtils.restartPrestoDataproc();
      } catch (IOException e2) {
        LOG.warn("Failed to restart presto on EMR and Dataproc", e2);
        throw new IOException("Failures to restart presto: emr method:" + e
            + "\nDataproc method: " + e2);
      }
    }
  }
}
