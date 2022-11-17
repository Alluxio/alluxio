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
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.JournalDomain;
import alluxio.grpc.NodeState;
import alluxio.master.journal.JournalSystem;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.WebServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public abstract class AlluxioSimpleMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioSimpleMasterProcess.class);

  /**
   * @return the master that is running on this process
   */
  abstract List<AbstractMaster> getAbstractMasters();

  /**
   * @return a newly created web server for this master
   */
  abstract WebServer createWebServer();

  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;

  /** The connection address for the web service. */
  final InetSocketAddress mWebConnectAddress;

  /** The master name. */
  final String mMasterName;

  /** The master journal domain. */
  final JournalDomain mJournalDomain;

  /**
   * The base class for running a master process that runs a single master implementing
   * the {@link AbstractMaster} interface.
   * @param masterName the name of the master (used when printing log messages)
   * @param journalDomain the journal domain of the master
   * @param journalSystem the journal system
   * @param leaderSelector the leader selector
   * @param webService the web service information
   * @param rpcService the rpc service information
   * @param hostNameKey the property key used to get the host name of this master
   */
  AlluxioSimpleMasterProcess(String masterName, JournalDomain journalDomain,
      JournalSystem journalSystem, PrimarySelector leaderSelector, ServiceType webService,
      ServiceType rpcService, PropertyKey hostNameKey) {
    super(journalSystem, leaderSelector, webService, rpcService);
    mMasterName = masterName;
    mJournalDomain = journalDomain;
    mWebConnectAddress = NetworkAddressUtils.getConnectAddress(webService,
        Configuration.global());
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(rpcService,
        Configuration.global());
    if (!Configuration.isSet(hostNameKey)) {
      Configuration.set(hostNameKey,
          NetworkAddressUtils.getLocalHostName(
              (int) Configuration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    }
    try {
      if (!mJournalSystem.isFormatted()) {
        mJournalSystem.format();
      }
    } catch (Exception e) {
      LOG.error("Failed to create {} master", mMasterName, e);
      throw new RuntimeException(String.format("Failed to create %s master", mMasterName), e);
    }
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InetSocketAddress getWebAddress() {
    synchronized (mWebServerLock) {
      if (mWebServer != null) {
        return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
      }
      return mWebConnectAddress;
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
   * Stops the Alluxio master server.
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
      for (AbstractMaster master : getAbstractMasters()) {
        master.start(isLeader);
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  protected void stopMaster() {
    try {
      for (AbstractMaster master : getAbstractMasters()) {
        master.stop();
      }
    } catch (IOException e) {
      LOG.error("Failed to stop {} master", mMasterName, e);
      throw new RuntimeException(String.format("Failed to stop %s master", mMasterName), e);
    }
  }

  protected void startServing(String startMessage, String stopMessage) {
    LOG.info("Alluxio {} master web server version {} starting{}. webAddress={}",
        mMasterName, RuntimeConstants.VERSION, startMessage, mWebBindAddress);
    startServingRPCServer();
    startServingWebServer();
    LOG.info(
        "Alluxio {} master version {} started{}. bindAddress={}, connectAddress={}, webAddress={}",
        mMasterName, RuntimeConstants.VERSION, startMessage, mRpcBindAddress,
        mRpcConnectAddress, mWebBindAddress);
    mGrpcServer.awaitTermination();
    LOG.info("Alluxio {} master ended {}", mMasterName, stopMessage);
  }

  protected void startServingWebServer() {
    stopRejectingWebServer();
    synchronized (mWebServerLock) {
      mWebServer = createWebServer();
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

  abstract GrpcServerBuilder createBaseRPCServer();

  private GrpcServer createRPCServer() {
    // Create underlying gRPC server.
    GrpcServerBuilder builder = createBaseRPCServer();
    // Register master services.
    getAbstractMasters().forEach(master -> {
      master.getServices().forEach((type, service) -> {
        builder.addService(type, service);
        LOG.info("registered service {}", type.name());
      });
    });
    // Builds a server that is not started yet.
    return builder.build();
  }

  protected void stopServing() throws Exception {
    synchronized (mGrpcServerLock) {
      if (mGrpcServer != null && mGrpcServer.isServing()) {
        LOG.info("Stopping Alluxio {} master RPC server on {} @ {}",
            mMasterName, this, mRpcBindAddress);
        if (!mGrpcServer.shutdown()) {
          LOG.warn("Alluxio {} master RPC server shutdown timed out.", mMasterName);
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
    return String.format("Alluxio %s master @ %s", mMasterName, mRpcConnectAddress);
  }
}
