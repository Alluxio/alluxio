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

package alluxio.yarn;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.yarn.YarnUtils.YarnContainerType;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.HttpMethod;

/**
 * Actual owner of Alluxio running on Yarn. The YARN ResourceManager will launch this
 * ApplicationMaster on an allocated container. The ApplicationMaster communicates with the YARN
 * cluster, and handles application execution. It performs operations asynchronously.
 *
 * @deprecated since 2.0
 */
@Deprecated
@NotThreadSafe
public final class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

  /**
   * Resources needed by the master and worker containers. Yarn will copy these to the container
   * before running the container's command.
   */
  private static final List<String> LOCAL_RESOURCE_NAMES =
      Lists.newArrayList(YarnUtils.ALLUXIO_TARBALL, YarnUtils.ALLUXIO_SETUP_SCRIPT);

  /* Parameters sent from Client. */
  private final int mMasterCpu;
  private final int mWorkerCpu;
  private final int mMasterMemInMB;
  private final int mWorkerMemInMB;
  private final int mRamdiskMemInMB;
  private final int mNumWorkers;
  private final String mMasterAddress;
  private final int mMaxWorkersPerHost;
  private final String mResourcePath;

  private final YarnConfiguration mYarnConf = new YarnConfiguration();
  /** The count starts at 1, then becomes 0 when the application is done. */
  private final CountDownLatch mApplicationDoneLatch;

  /** Client to talk to Resource Manager. */
  private final AMRMClientAsync<ContainerRequest> mRMClient;
  /** Client to talk to Node Manager. */
  private final NMClient mNMClient;
  /** Client Resource Manager Service. */
  private final YarnClient mYarnClient;
  /** Network address of the container allocated for Alluxio master. */
  private String mMasterContainerNetAddress;

  private final AlluxioConfiguration mAlluxioConf;

  private volatile ContainerAllocator mContainerAllocator;

  /**
   * A factory which creates an AMRMClientAsync with a heartbeat interval and callback handler.
   */
  public interface AMRMClientAsyncFactory {
    /**
     * @param heartbeatMs the interval at which to send heartbeats to the resource manager
     * @param handler a handler for callbacks from the resource manager
     * @return a client for making requests to the resource manager
     */
    AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
        CallbackHandler handler);
  }

  /** Security tokens for HDFS. */
  private ByteBuffer mAllTokens;

  /**
   * Convenience constructor which uses the default Alluxio configuration.
   *
   * @param numWorkers the number of workers to launch
   * @param masterAddress the address at which to start the Alluxio master
   * @param resourcePath an hdfs path shared by all yarn nodes which can be used to share resources
   * @param alluxioConf Alluxio configuration
   */
  public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath,
      AlluxioConfiguration alluxioConf) {
    this(numWorkers, masterAddress, resourcePath, YarnClient.createYarnClient(),
        NMClient.createNMClient(), new AMRMClientAsyncFactory() {
          @Override
          public AMRMClientAsync<ContainerRequest> createAMRMClientAsync(int heartbeatMs,
              CallbackHandler handler) {
            return AMRMClientAsync.createAMRMClientAsync(heartbeatMs, handler);
          }
        }, alluxioConf);
  }

  /**
   * Constructs an {@link ApplicationMaster}.
   *
   * Clients will be initialized and started during the {@link #start()} method.
   *
   * @param numWorkers the number of workers to launch
   * @param masterAddress the address at which to start the Alluxio master
   * @param resourcePath an hdfs path shared by all yarn nodes which can be used to share resources
   * @param yarnClient the client to use for communicating with Yarn
   * @param nMClient the client to use for communicating with the node manager
   * @param amrmFactory a factory for creating an {@link AMRMClientAsync}
   * @param alluxioConf Alluxio configuration
   */
  public ApplicationMaster(int numWorkers, String masterAddress, String resourcePath,
      YarnClient yarnClient, NMClient nMClient, AMRMClientAsyncFactory amrmFactory,
      AlluxioConfiguration alluxioConf) {
    mMasterCpu = alluxioConf.getInt(PropertyKey.INTEGRATION_MASTER_RESOURCE_CPU);
    mMasterMemInMB =
        (int) (alluxioConf.getBytes(PropertyKey.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB);
    mWorkerCpu = alluxioConf.getInt(PropertyKey.INTEGRATION_WORKER_RESOURCE_CPU);
    // TODO(binfan): request worker container and ramdisk container separately
    // memory for running worker
    mWorkerMemInMB =
        (int) (alluxioConf.getBytes(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB);
    // memory for running ramdisk
    mRamdiskMemInMB = (int) (alluxioConf.getBytes(PropertyKey.WORKER_RAMDISK_SIZE) / Constants.MB);
    mMaxWorkersPerHost = alluxioConf.getInt(PropertyKey.INTEGRATION_YARN_WORKERS_PER_HOST_MAX);
    mNumWorkers = numWorkers;
    mMasterAddress = masterAddress;
    mResourcePath = resourcePath;
    mApplicationDoneLatch = new CountDownLatch(1);
    mYarnClient = yarnClient;
    mNMClient = nMClient;
    mAlluxioConf = alluxioConf;
    // Heartbeat to the resource manager every 500ms.
    mRMClient = amrmFactory.createAMRMClientAsync(500, this);
  }

  /**
   * @param args Command line arguments to launch application master
   */
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("num_workers", true, "Number of Alluxio workers to launch. Default 1");
    options.addOption("master_address", true, "(Required) Address to run Alluxio master");
    options.addOption("resource_path", true,
        "(Required) HDFS path containing the Application Master");

    try {
      LOG.info("Starting Application Master with args {}", Arrays.toString(args));
      final CommandLine cliParser = new GnuParser().parse(options, args);
      AlluxioConfiguration alluxioConf = new InstancedConfiguration(ConfigurationUtils.defaults());
      YarnConfiguration conf = new YarnConfiguration();
      UserGroupInformation.setConfiguration(conf);
      if (UserGroupInformation.isSecurityEnabled()) {
        String user = System.getenv("ALLUXIO_USER");
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        for (Token token : UserGroupInformation.getCurrentUser().getTokens()) {
          ugi.addToken(token);
        }
        LOG.info("UserGroupInformation: " + ugi);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            runApplicationMaster(cliParser, alluxioConf);
            return null;
          }
        });
      } else {
        runApplicationMaster(cliParser, alluxioConf);
      }
    } catch (Exception e) {
      LOG.error("Error running Application Master", e);
      System.exit(1);
    }
  }

  /**
   * Run the application master.
   *
   * @param cliParser client arguments parser
   */
  private static void runApplicationMaster(final CommandLine cliParser,
      AlluxioConfiguration alluxioConf) throws Exception {
    int numWorkers = Integer.parseInt(cliParser.getOptionValue("num_workers", "1"));
    String masterAddress = cliParser.getOptionValue("master_address");
    String resourcePath = cliParser.getOptionValue("resource_path");

    ApplicationMaster applicationMaster =
        new ApplicationMaster(numWorkers, masterAddress, resourcePath, alluxioConf);
    applicationMaster.start();
    applicationMaster.requestAndLaunchContainers();
    applicationMaster.waitForShutdown();
    applicationMaster.stop();
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    for (Container container : containers) {
      mContainerAllocator.allocateContainer(container);
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus status : statuses) {
      // Releasing worker containers because we already have workers on their host will generate a
      // callback to this method, so we use debug instead of error.
      if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
        LOG.debug("Aborted container {}", status.getContainerId());
      } else {
        LOG.error("Container {} completed with exit status {}", status.getContainerId(),
            status.getExitStatus());
      }
    }
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updated) {}

  @Override
  public void onShutdownRequest() {
    mApplicationDoneLatch.countDown();
  }

  @Override
  public void onError(Throwable t) {
    LOG.error("Error reported by resource manager", t);
  }

  @Override
  public float getProgress() {
    return 0;
  }

  /**
   * Starts the application master.
   */
  public void start() throws IOException, YarnException {
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials =
          UserGroupInformation.getCurrentUser().getCredentials();
      DataOutputBuffer credentialsBuffer = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(credentialsBuffer);
      // Now remove the AM -> RM token so that containers cannot access it.
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          iter.remove();
        }
      }
      mAllTokens = ByteBuffer.wrap(credentialsBuffer.getData(), 0, credentialsBuffer.getLength());
    }
    mNMClient.init(mYarnConf);
    mNMClient.start();

    mRMClient.init(mYarnConf);
    mRMClient.start();

    mYarnClient.init(mYarnConf);
    mYarnClient.start();

    // Register with ResourceManager
    String hostname = NetworkAddressUtils.getLocalHostName((int) mAlluxioConf
            .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    mRMClient.registerApplicationMaster(hostname, 0 /* port */, "" /* tracking url */);
    LOG.info("ApplicationMaster registered");
  }

  /**
   * Submits requests for containers until the master and all workers are launched.
   */
  public void requestAndLaunchContainers() throws Exception {

    if (masterExists()) {
      InetAddress address = InetAddress.getByName(mMasterAddress);
      mMasterContainerNetAddress = address.getHostAddress();
      LOG.info("Found master already running on " + mMasterAddress);
    } else {
      LOG.info("Configuring master container request.");
      Resource masterResource = Records.newRecord(Resource.class);
      masterResource.setMemory(mMasterMemInMB);
      masterResource.setVirtualCores(mMasterCpu);
      mContainerAllocator = new ContainerAllocator("master", 1, 1, masterResource, mYarnClient,
              mRMClient, mMasterAddress);
      List<Container> masterContainers = mContainerAllocator.allocateContainers();
      launchMasterContainer(Iterables.getOnlyElement(masterContainers));
    }

    Resource workerResource = Records.newRecord(Resource.class);
    workerResource.setMemory(mWorkerMemInMB + mRamdiskMemInMB);
    workerResource.setVirtualCores(mWorkerCpu);
    mContainerAllocator = new ContainerAllocator("worker", mNumWorkers, mMaxWorkersPerHost,
        workerResource, mYarnClient, mRMClient);
    List<Container> workerContainers = mContainerAllocator.allocateContainers();
    for (Container container : workerContainers) {
      launchWorkerContainer(container);
    }
    LOG.info("Master and workers are launched");
  }

  /**
   * @throws InterruptedException if interrupted while awaiting shutdown
   */
  public void waitForShutdown() throws InterruptedException {
    mApplicationDoneLatch.await();
  }

  /**
   * Shuts down the application master, unregistering it from Yarn and stopping its clients.
   */
  public void stop() {
    try {
      mRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    } catch (YarnException e) {
      LOG.error("Failed to unregister application", e);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    mRMClient.stop();
    // TODO(andrew): Think about whether we should stop mNMClient here
    mYarnClient.stop();
  }

  private void launchMasterContainer(Container container) {
    String command = YarnUtils.buildCommand(YarnContainerType.ALLUXIO_MASTER);
    try {
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
      ctx.setCommands(Lists.newArrayList(command));
      ctx.setLocalResources(setupLocalResources(mResourcePath));
      ctx.setEnvironment(setupMasterEnvironment());
      if (UserGroupInformation.isSecurityEnabled()) {
        ctx.setTokens(mAllTokens.duplicate());
      }
      LOG.info("Launching container {} for Alluxio master on {} with master command: {}",
          container.getId(), container.getNodeHttpAddress(), command);
      mNMClient.startContainer(container, ctx);
      String containerUri = container.getNodeHttpAddress(); // in the form of 1.2.3.4:8042
      mMasterContainerNetAddress = containerUri.split(":")[0];
      LOG.info("Master address: {}", mMasterContainerNetAddress);
      return;
    } catch (Exception e) {
      LOG.error("Error launching container {}", container.getId(), e);
    }
  }

  private void launchWorkerContainer(Container container) {
    String command = YarnUtils.buildCommand(YarnContainerType.ALLUXIO_WORKER);

    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    ctx.setCommands(Lists.newArrayList(command));
    ctx.setLocalResources(setupLocalResources(mResourcePath));
    ctx.setEnvironment(setupWorkerEnvironment(mMasterContainerNetAddress, mRamdiskMemInMB));
    if (UserGroupInformation.isSecurityEnabled()) {
      ctx.setTokens(mAllTokens.duplicate());
    }

    try {
      LOG.info("Launching container {} for Alluxio worker on {} with worker command: {}",
          container.getId(), container.getNodeHttpAddress(), command);
      mNMClient.startContainer(container, ctx);
    } catch (Exception e) {
      LOG.error("Error launching container {}", container.getId(), e);
    }
  }

  /**
   * Checks if an Alluxio master node is already running
   * or not on the master address given.
   *
   * @return true if master exists, false otherwise
   */
  private boolean masterExists() {

    String webPort = mAlluxioConf.get(PropertyKey.MASTER_WEB_PORT);

    try {
      URL myURL = new URL("http://" + mMasterAddress + ":" + webPort + Constants.REST_API_PREFIX + "/master/version");
      LOG.debug("Checking for master at: " + myURL.toString());
      HttpURLConnection connection = (HttpURLConnection) myURL.openConnection();
      connection.setRequestMethod(HttpMethod.GET);
      int resCode = connection.getResponseCode();
      LOG.debug("Response code from master was: " + Integer.toString(resCode));

      connection.disconnect();
      return resCode == HttpURLConnection.HTTP_OK;
    } catch (MalformedURLException e) {
      LOG.error("Malformed URL in attempt to check if master is running already", e);
    } catch (IOException e) {
      LOG.debug("No existing master found", e);
    }

    return false;
  }

  private static Map<String, LocalResource> setupLocalResources(String resourcePath) {
    try {
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      for (String resourceName : LOCAL_RESOURCE_NAMES) {
        localResources.put(resourceName, YarnUtils.createLocalResourceOfFile(
            new YarnConfiguration(), PathUtils.concatPath(resourcePath, resourceName)));
      }
      return localResources;
    } catch (IOException e) {
      throw new RuntimeException("Cannot find resource", e);
    }
  }

  private static Map<String, String> setupMasterEnvironment() {
    return setupCommonEnvironment();
  }

  private static Map<String, String> setupWorkerEnvironment(String masterContainerNetAddress,
      int ramdiskMemInMB) {
    Map<String, String> env = setupCommonEnvironment();
    env.put("ALLUXIO_MASTER_HOSTNAME", masterContainerNetAddress);
    env.put("ALLUXIO_WORKER_RAMDISK_SIZE",
        FormatUtils.getSizeFromBytes((long) ramdiskMemInMB * Constants.MB));
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        env.put("ALLUXIO_USER", UserGroupInformation.getCurrentUser().getShortUserName());
      } catch (IOException e) {
        LOG.error("Get user name failed", e);
      }
    }
    return env;
  }

  private static Map<String, String> setupCommonEnvironment() {
    // Setup the environment needed for the launch context.
    Map<String, String> env = new HashMap<String, String>();
    env.put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$());
    env.put("ALLUXIO_RAM_FOLDER", ApplicationConstants.Environment.LOCAL_DIRS.$());
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        env.put("ALLUXIO_USER", UserGroupInformation.getCurrentUser().getShortUserName());
      } catch (IOException e) {
        LOG.error("Get user name failed", e);
      }
    }
    return env;
  }
}
