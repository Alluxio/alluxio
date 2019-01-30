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
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.job.JobMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.underfs.JobUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.util.URIUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.URI;

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

  /** The connect address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;

  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

  AlluxioJobMasterProcess(JournalSystem journalSystem, ServerSocket rpcBindSocket,
      ServerSocket webBindSocket) {
    super(journalSystem, rpcBindSocket, webBindSocket);
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC,
        ServerConfiguration.global());
    if (!ServerConfiguration.isSet(PropertyKey.JOB_MASTER_HOSTNAME)) {
      ServerConfiguration.set(PropertyKey.JOB_MASTER_HOSTNAME,
          NetworkAddressUtils.getLocalHostName(
              (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS)));
    }
    mUfsManager = new JobUfsManager();
    try {
      // Create master.
      mJobMaster = new JobMaster(new MasterContext(mJournalSystem), mUfsManager);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
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
    if (isServing()) {
      stopServing();
      stopMaster();
      mJournalSystem.stop();
    }
  }

  protected void startMaster(boolean isLeader) {
    try {
      mJobMaster.start(isLeader);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected void stopMaster() {
    try {
      mJobMaster.stop();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected void startServing(String startMessage, String stopMessage) {
    startServingWebServer();
    LOG.info("Alluxio job master version {} started{}. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        startMessage,
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_RPC,
            ServerConfiguration.global()),
        NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC,
            ServerConfiguration.global()),
        NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC,
            ServerConfiguration.global()),
        NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_WEB,
            ServerConfiguration.global()));

    startServingRPCServer();
    LOG.info("Alluxio job master ended");
  }

  protected void startServingWebServer() {
    try {
      mWebServer = new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(),
          getWebAddressFromBindSocket(), this);
      mWebServer.addHandler(mMetricsServlet.getHandler());
      mWebServer.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    // TODO(ggezer) Executor threads not reused until thread capacity is hit.
    //ExecutorService executorService = Executors.newFixedThreadPool(mMaxWorkerThreads);
    try {
      SocketAddress bindAddress = getRpcAddressFromBindSocket();
      LOG.info("Starting gRPC server on address {}", bindAddress);
      GrpcServerBuilder serverBuilder = GrpcServerBuilder.forAddress(bindAddress,
          ServerConfiguration.global());
      registerServices(serverBuilder, mJobMaster.getServices());

      mGrpcServer = serverBuilder.build().start();
      LOG.info("Started gRPC server on address {}", bindAddress);

      // Wait until the server is shut down.
      mGrpcServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void stopServing() throws Exception {
    if (isServing()) {
      LOG.info("Stopping RPC server on {} @ {}", this, mRpcBindSocket.getInetAddress());
      if (!mGrpcServer.shutdown()) {
        LOG.warn("RPC Server shutdown timed out.");
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

    public static AlluxioJobMasterProcess create() {
      return create(MasterProcess.setupBindSocket(ServiceType.JOB_MASTER_RPC),
          MasterProcess.setupBindSocket(ServiceType.JOB_MASTER_WEB));
    }

    /**
     * @return a new instance of {@link AlluxioJobMasterProcess}
     */
    public static AlluxioJobMasterProcess create(ServerSocket rpcBindSocket,
        ServerSocket webBindSocket) {
      URI journalLocation = JournalUtils.getJournalLocation();
      JournalSystem journalSystem = new JournalSystem.Builder()
          .setLocation(URIUtils.appendPathOrDie(journalLocation, Constants.JOB_JOURNAL_NAME))
          .build();
      if (ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(!(journalSystem instanceof RaftJournalSystem),
            "Raft journal cannot be used with Zookeeper enabled");
        PrimarySelector primarySelector = PrimarySelector.Factory.createZkJobPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector,
            rpcBindSocket, webBindSocket);
      } else if (journalSystem instanceof RaftJournalSystem) {
        PrimarySelector primarySelector = ((RaftJournalSystem) journalSystem).getPrimarySelector();
        return new FaultTolerantAlluxioJobMasterProcess(journalSystem, primarySelector,
            rpcBindSocket, webBindSocket);
      }
      return new AlluxioJobMasterProcess(journalSystem, rpcBindSocket, webBindSocket);
    }

    private Factory() {} // prevent instantiation
  }
}
