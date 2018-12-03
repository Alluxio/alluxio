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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.job.JobMaster;
import alluxio.master.job.JobMasterClientServiceHandler;
import alluxio.master.job.JobMasterWorkerServiceHandler;
import alluxio.master.journal.JournalSystem;
import alluxio.master.version.AlluxioVersionServiceHandler;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.network.thrift.ThriftUtils;
import alluxio.security.authentication.TransportProvider;
import alluxio.underfs.JobUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.grpc.GrpcServer;
import alluxio.util.grpc.GrpcServerBuilder;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobMasterWebServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different masters that are configured to run.
 */
@NotThreadSafe
public class AlluxioJobMasterProcess implements JobMasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobMasterProcess.class);

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /** The socket for thrift rpc server. */
  private TServerSocket mTServerSocket;

  // TODO(ggezer) review gRPC initialization
  private GrpcServer mGrpcServer;

  /** The transport provider to create thrift server transport. */
  private final TransportProvider mTransportProvider;

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress;

  /** The bind address for the rpc server. */
  private final InetSocketAddress mRpcBindAddress;

  /** The master managing all job related metadata. */
  protected JobMaster mJobMaster;

  /** The RPC server. */
  private TThreadPoolServer mMasterServiceServer = null;

  /** is true if the master is serving the RPC server. */
  private boolean mIsServing = false;

  /** The start time for when the master started. */
  private final long mStartTimeMs = System.currentTimeMillis();

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** The web server. */
  private JobMasterWebServer mWebServer = null;

  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

  AlluxioJobMasterProcess(JournalSystem journalSystem) {
    if (!Configuration.containsKey(PropertyKey.MASTER_HOSTNAME)) {
      Configuration.set(PropertyKey.MASTER_HOSTNAME, NetworkAddressUtils.getLocalHostName());
    }
    mUfsManager = new JobUfsManager();
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        Preconditions.checkState(Configuration.getInt(PropertyKey.JOB_MASTER_RPC_PORT) > 0,
            "Master rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(Configuration.getInt(PropertyKey.JOB_MASTER_WEB_PORT) > 0,
            "Master web port is only allowed to be zero in test mode.");
      }
      mTransportProvider = TransportProvider.Factory.create();
      mTServerSocket = new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_RPC));
      mPort = ThriftUtils.getThriftPort(mTServerSocket);
      // reset master port
      Configuration.set(PropertyKey.JOB_MASTER_RPC_PORT, Integer.toString(mPort));
      mRpcBindAddress = NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_RPC);
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC);

      // Create master.
      createMaster();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  protected void createMaster() {
    mJobMaster = new JobMaster(new MasterContext(mJournalSystem), mUfsManager);
  }

  @Override
  public JobMaster getJobMaster() {
    return mJobMaster;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public boolean isServing() {
    return mIsServing;
  }

  @Override
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> mMasterServiceServer != null && mMasterServiceServer.isServing()
              && mWebServer != null && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /**
   * Starts the Alluxio job master server.
   *
   * @throws Exception if starting the master fails
   */
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
  public void stop() throws Exception {
    LOG.info("Stopping RPC server on {} @ {}", this, mRpcBindAddress);
    if (mIsServing) {
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

  private void startServing() {
    startServing("", "");
  }

  protected void startServing(String startMessage, String stopMessage) {
    startServingWebServer();
    LOG.info("Alluxio job master version {} started{}. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        startMessage,
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_RPC),
        NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_WEB));

    startServingRPCServer();
    startServingRPCServerNew();
    LOG.info("Alluxio job master ended");
  }

  protected void startServingWebServer() {
    mWebServer = new JobMasterWebServer(ServiceType.JOB_MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.JOB_MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
    mWebServer.addHandler(mMetricsServlet.getHandler());
    mWebServer.start();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
      LOG.info("registered service {}", service.getKey());
    }
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServerNew() {
    int port = 50052;
    ExecutorService executorService = Executors.newFixedThreadPool(mMaxWorkerThreads);
    try {
      LOG.info("Starting gRPC server on port {}", port);
      mGrpcServer = GrpcServerBuilder.forPort(port)
              .addService(new JobMasterClientServiceHandler(mJobMaster))
              .addService(new JobMasterWorkerServiceHandler(mJobMaster))
              .addService(new AlluxioVersionServiceHandler())
              .executor(executorService)
              .build()
              .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    registerServices(processor, mJobMaster.getServices());
    // register meta services
    // processor.registerProcessor(Constants.JOB_MASTER_CLIENT_SERVICE_NAME,
    //    new JobMasterClientService.Processor<>(new JobMasterClientServiceHandler(mJobMaster)));
    LOG.info("registered service " + Constants.JOB_MASTER_CLIENT_SERVICE_NAME);

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.JOB_MASTER_RPC);
      transportFactory = mTransportProvider.getServerTransportFactory(serverName);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    try {
      if (mTServerSocket != null) {
        mTServerSocket.close();
      }
      mTServerSocket = ThriftUtils.createThriftServerSocket(mRpcBindAddress);
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }
    // create master thrift service with the multiplexed processor.
    Args args = new Args(mTServerSocket).maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads).processor(processor).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    mMasterServiceServer = new TThreadPoolServer(args);

    // start thrift rpc server
    mIsServing = true;
    mMasterServiceServer.serve();
  }

  protected void stopServing() throws Exception {
    if (mMasterServiceServer != null) {
      mMasterServiceServer.stop();
      mMasterServiceServer = null;
    }
    if (mTServerSocket != null) {
      mTServerSocket.close();
      mTServerSocket = null;
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    mIsServing = false;
  }

  @Override
  public String toString() {
    return "Alluxio job master";
  }
}
