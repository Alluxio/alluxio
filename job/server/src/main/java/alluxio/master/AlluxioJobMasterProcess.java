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

package alluxio.master;

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalDomain;
import alluxio.master.job.JobMaster;
import alluxio.master.journal.DefaultJournalMaster;
import alluxio.master.journal.JournalMasterClientServiceHandler;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.JobUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils.ProcessType;
import alluxio.util.URIUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public class AlluxioJobMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobMasterProcess.class);

  /** The master managing all job related metadata. */
  protected JobMaster mJobMaster;

  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;

  AlluxioJobMasterProcess(JournalSystem journalSystem) {
    super(journalSystem, ServiceType.JOB_MASTER_RPC, ServiceType.JOB_MASTER_WEB);
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC,
        ServerConfiguration.global());
    if (!ServerConfiguration.isSet(PropertyKey.JOB_MASTER_HOSTNAME)) {
      ServerConfiguration.set(PropertyKey.JOB_MASTER_HOSTNAME,
          NetworkAddressUtils.getLocalHostName(
              (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    }
    FileSystemContext fsContext = FileSystemContext.create(ServerConfiguration.global());
    FileSystem fileSystem = FileSystem.Factory.create(fsContext);
    UfsManager ufsManager = new JobUfsManager();
    try {
      if (!mJournalSystem.isFormatted()) {
        mJournalSystem.format();
      }
      // Create master.
      mJobMaster = new JobMaster(
          new MasterContext<>(mJournalSystem, null, ufsManager), fileSystem, fsContext,
          ufsManager);
    } catch (Exception e) {
      LOG.error("Failed to create job master", e);
      throw new RuntimeException("Failed to create job master", e);
    }
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    if (clazz == JobMaster.class) {
      return (T) mJobMaster;
    } else {
      throw new RuntimeException(String.format("Could not find the master: %s", clazz));
    }
  }

  /**
   * @return the {@link JobMaster} for this process
   */
  public JobMaster getJobMaster() {
    return mJobMaster;
  }

  @Override
  @Nullable
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  /**
   * Starts the Alluxio job master server.
   *
   * @throws Exception if starting the master fails
   */
  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    startMaster(true);
    startServing();
  }

  /**
   * Stops the Alluxio job master server.
   *
   * @throws Exception if stopping the master fails
   */
  @Override
  public void stop() throws Exception {
    stopRejectingServers();
    if (isGrpcServing()) {
      stopServing();
    }
    mJournalSystem.stop();
    stopMaster();
  }

  protected void startMaster(boolean isLeader) {
    try {
      if (!isLeader) {
        startRejectingServers();
      }
      mJobMaster.start(isLeader);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected void stopMaster() {
    try {
      mJobMaster.stop();
    } catch (IOException e) {
      LOG.error("Failed to stop job master", e);
      throw new RuntimeException("Failed to stop job master", e);
    }
  }

  protected void startServing(String startMessage, String stopMessage) {
    LOG.info("Alluxio job master web server version {} starting{}. webAddress={}",
        RuntimeConstants.VERSION, startMessage, mWebBindAddress);
    startServingRPCServer();
    startServingWebServer();
    LOG.info(
        "Alluxio job master version {} started{}. bindAddress={}, connectAddress={}, webAddress={}",
        RuntimeConstants.VERSION, startMessage, mRpcBindAddress, mRpcConnectAddress,
        mWebBindAddress);
    mGrpcServer.awaitTermination();
    LOG.info("Alluxio job master ended {}", stopMessage);
  }

  protected void startServingWebServer() {
    stopRejectingWebServer();
    mWebServer =
        new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(), mWebBindAddress, this);
    mWebServer.start();
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    stopRejectingRpcServer();

    LOG.info("Starting gRPC server on address:{}", mRpcBindAddress);
    mGrpcServer = createRPCServer();

    try {
      // Start serving.
      mGrpcServer.start();
      // Acquire and log bind port from newly started server.
      InetSocketAddress listeningAddress = InetSocketAddress
          .createUnresolved(mRpcBindAddress.getHostName(), mGrpcServer.getBindPort());
      LOG.info("gRPC server listening on: {}", listeningAddress);
    } catch (IOException e) {
      LOG.error("gRPC serving failed.", e);
      throw new RuntimeException("gRPC serving failed");
    }
  }

  private GrpcServer createRPCServer() {
    // Create underlying gRPC server.
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            ServerConfiguration.global(), ServerUserState.global())
        .flowControlWindow(
            (int) ServerConfiguration.getBytes(PropertyKey.JOB_MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(ServerConfiguration.getMs(PropertyKey.JOB_MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            ServerConfiguration.getMs(PropertyKey.JOB_MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            ServerConfiguration.getMs(PropertyKey.JOB_MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) ServerConfiguration
            .getBytes(PropertyKey.JOB_MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
    // Register job-master services.
    registerServices(builder, mJobMaster.getServices());

    // Bind manifest of Alluxio JournalMaster service.
    // TODO(ggezer) Merge this with registerServices() logic.
    builder.addService(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
        new GrpcService(new JournalMasterClientServiceHandler(
            new DefaultJournalMaster(JournalDomain.JOB_MASTER, mJournalSystem))));

    // Builds a server that is not started yet.
    return builder.build();
  }

  protected void stopServing() throws Exception {
    if (isGrpcServing()) {
      LOG.info("Stopping Alluxio job master RPC server on {} @ {}", this, mRpcBindAddress);
      if (!mGrpcServer.shutdown()) {
        LOG.warn("Alluxio job master RPC server shutdown timed out.");
      }
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
  }

  @Override
  public String toString() {
    return "Alluxio job master @ " + mRpcConnectAddress;
  }

  /**
   * Factory for creating {@link AlluxioJobMasterProcess}.
   */
  @ThreadSafe
  static final class Factory {
    /**
     * @return a new instance of {@link AlluxioJobMasterProcess}
     */
    public static AlluxioJobMasterProcess create() {
      URI journalLocation = JournalUtils.getJournalLocation();
      JournalSystem journalSystem = new JournalSystem.Builder()
          .setLocation(URIUtils.appendPathOrDie(journalLocation, Constants.JOB_JOURNAL_NAME))
          .build(ProcessType.JOB_MASTER);
      if (ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(!(journalSystem instanceof RaftJournalSystem),
            "Raft journal cannot be used with Zookeeper enabled");
        PrimarySelector primarySelector = PrimarySelector.Factory.createZkJobPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector);
      } else if (journalSystem instanceof RaftJournalSystem) {
        PrimarySelector primarySelector = ((RaftJournalSystem) journalSystem).getPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector);
      }
      return new AlluxioJobMasterProcess(journalSystem);
    }

    private Factory() {} // prevent instantiation
  }
}
