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

import alluxio.RuntimeConstants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NodeState;
import alluxio.master.cross.cluster.CrossClusterMaster;
import alluxio.master.cross.cluster.DefaultCrossClusterMaster;
import alluxio.master.journal.AlwaysPrimaryPrimarySelector;
import alluxio.master.journal.DefaultJournalMaster;
import alluxio.master.journal.JournalMasterClientServiceHandler;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.CrossClusterMasterWebServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public class AlluxioCrossClusterMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCrossClusterMasterProcess.class);

  /** The master managing all job related metadata. */
  protected CrossClusterMaster mCrossClusterMaster;

  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;

  AlluxioCrossClusterMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector) {
    super(journalSystem, leaderSelector, ServiceType.CROSS_CLUSTER_MASTER_WEB,
        ServiceType.CROSS_CLUSTER_MASTER_RPC);
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.CROSS_CLUSTER_MASTER_RPC,
        Configuration.global());
    if (!Configuration.isSet(PropertyKey.CROSS_CLUSTER_MASTER_HOSTNAME)) {
      Configuration.set(PropertyKey.CROSS_CLUSTER_MASTER_HOSTNAME,
          NetworkAddressUtils.getLocalHostName(
              (int) Configuration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    }
    try {
      if (!mJournalSystem.isFormatted()) {
        mJournalSystem.format();
      }
      // Create master.
      mCrossClusterMaster = new DefaultCrossClusterMaster(
          new MasterContext<>(mJournalSystem, null, new NoopUfsManager()));
    } catch (Exception e) {
      LOG.error("Failed to create job master", e);
      throw new RuntimeException("Failed to create job master", e);
    }
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    if (clazz == CrossClusterMaster.class) {
      return (T) mCrossClusterMaster;
    } else {
      throw new RuntimeException(String.format("Could not find the master: %s", clazz));
    }
  }

  /**
   * @return the {@link CrossClusterMaster} for this process
   */
  public CrossClusterMaster getCrossClusterMaster() {
    return mCrossClusterMaster;
  }

  @Override
  public InetSocketAddress getWebAddress() {
    synchronized (mWebServerLock) {
      if (mWebServer != null) {
        return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
      }
      return NetworkAddressUtils.getConnectAddress(ServiceType.CROSS_CLUSTER_MASTER_RPC,
          Configuration.global());
    }
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    // the leader selector is created in state STANDBY. Once mLeaderSelector.start is called, it
    // can transition to PRIMARY at any point.
    mLeaderSelector.start(getRpcAddress());

    while (!Thread.interrupted()) {
      if (mServingThread == null) {
        // We are in standby mode. Nothing to do until we become the primary.
        mLeaderSelector.waitForState(NodeState.PRIMARY);
        LOG.info("Transitioning from standby to primary");
        mJournalSystem.gainPrimacy();
        stopMaster();
        LOG.info("Secondary stopped");
        startMaster(true);
        mServingThread = new Thread(() -> startServing(
            " (gained leadership)", " (lost leadership)"), "MasterServingThread");
        mServingThread.start();
        LOG.info("Primary started");
      } else {
        // We are in primary mode. Nothing to do until we become the standby.
        mLeaderSelector.waitForState(NodeState.STANDBY);
        LOG.info("Transitioning from primary to standby");
        stopServing();
        mServingThread.join();
        mServingThread = null;
        stopMaster();
        mJournalSystem.losePrimacy();
        LOG.info("Primary stopped");
        startMaster(false);
        LOG.info("Standby started");
      }
    }
  }

  /**
   * Stops the Alluxio cross cluster master server.
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
    mLeaderSelector.stop();
  }

  protected void startMaster(boolean isLeader) {
    try {
      if (!isLeader) {
        startRejectingServers();
      }
      mCrossClusterMaster.start(isLeader);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected void stopMaster() {
    try {
      mCrossClusterMaster.stop();
    } catch (IOException e) {
      LOG.error("Failed to stop job master", e);
      throw new RuntimeException("Failed to stop job master", e);
    }
  }

  protected void startServing(String startMessage, String stopMessage) {
    LOG.info("Alluxio cross cluster master web server version {} starting{}. webAddress={}",
        RuntimeConstants.VERSION, startMessage, mWebBindAddress);
    startServingRPCServer();
    startServingWebServer();
    LOG.info(
        "Alluxio cross cluster master version {} started{}. bindAddress={}, connectAddress={},"
            + " webAddress={}",
        RuntimeConstants.VERSION, startMessage, mRpcBindAddress, mRpcConnectAddress,
        mWebBindAddress);
    mGrpcServer.awaitTermination();
    LOG.info("Alluxio job master ended {}", stopMessage);
  }

  protected void startServingWebServer() {
    stopRejectingWebServer();
    synchronized (mWebServerLock) {
      mWebServer =
          new CrossClusterMasterWebServer(ServiceType.CROSS_CLUSTER_MASTER_WEB.getServiceName(),
              mWebBindAddress, this);
      mWebServer.start();
    }
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    stopRejectingRpcServer();
    try {
      synchronized (mGrpcServerLock) {
        LOG.info("Starting gRPC server on address:{}", mRpcBindAddress);
        mGrpcServer = createRPCServer();
        // Start serving.
        mGrpcServer.start();
        // Acquire and log bind port from newly started server.
        InetSocketAddress listeningAddress = InetSocketAddress
            .createUnresolved(mRpcBindAddress.getHostName(), mGrpcServer.getBindPort());
        LOG.info("gRPC server listening on: {}", listeningAddress);
      }
    } catch (IOException e) {
      LOG.error("gRPC serving failed.", e);
      throw new RuntimeException("gRPC serving failed");
    }
  }

  private GrpcServer createRPCServer() {
    // Create underlying gRPC server.
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            Configuration.global())
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
            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));

    // Register cross cluster-master services.
    registerServices(builder, mCrossClusterMaster.getServices());

    // Bind manifest of Alluxio JournalMaster service.
    builder.addService(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
        new GrpcService(new JournalMasterClientServiceHandler(
            new DefaultJournalMaster(JournalDomain.CROSS_CLUSTER_MASTER, mJournalSystem,
                mLeaderSelector))));

    // Builds a server that is not started yet.
    return builder.build();
  }

  protected void stopServing() throws Exception {
    synchronized (mGrpcServerLock) {
      if (mGrpcServer != null && mGrpcServer.isServing()) {
        LOG.info("Stopping Alluxio job master RPC server on {} @ {}", this, mRpcBindAddress);
        if (!mGrpcServer.shutdown()) {
          LOG.warn("Alluxio job master RPC server shutdown timed out.");
        }
      }
    }
    synchronized (mWebServerLock) {
      if (mWebServer != null) {
        mWebServer.stop();
        mWebServer = null;
      }
    }
  }

  @Override
  public String toString() {
    return "Alluxio cross cluster master @ " + mRpcConnectAddress;
  }

  /**
   * Factory for creating {@link AlluxioCrossClusterMasterProcess}.
   */
  @ThreadSafe
  static final class Factory {
    /**
     * Factory to create an instance of a cross cluster master process.
     * Currently, this process only supports a no-op journal system, and
     * a single master who is always a primary.
     * @return a new instance of {@link AlluxioCrossClusterMasterProcess}
     */
    public static AlluxioCrossClusterMasterProcess create() {
      JournalSystem journalSystem = new NoopJournalSystem();
      return new AlluxioCrossClusterMasterProcess(journalSystem,
          new AlwaysPrimaryPrimarySelector());
    }

    private Factory() {} // prevent instantiation
  }
}
