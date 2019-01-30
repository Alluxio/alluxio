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

import static alluxio.util.network.NetworkAddressUtils.ServiceType;

import alluxio.Process;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.master.journal.JournalSystem;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.WebServer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Defines a set of methods which any {@link MasterProcess} should implement.
 *
 * This class serves as a common implementation for functions that both the
 * {@link alluxio.master.AlluxioMasterProcess} and {@link alluxio.master.AlluxioJobMasterProcess}
 * use. Each master should have an RPC server, web server, and journaling system which can serve
 * client requests.
 */
public abstract class MasterProcess implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcess.class);

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** Maximum number of threads to serve the rpc server. */
  final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  final int mMinWorkerThreads;

  /** Used for auto binding. **/
  final ServerSocket mRpcBindSocket;

  /** Used for auto binding web server. **/
  final ServerSocket mWebBindSocket;

  /** The start time for when the master started. */
  final long mStartTimeMs = System.currentTimeMillis();

  /** The RPC server. */
  GrpcServer mGrpcServer;

  /** The web ui server. */
  WebServer mWebServer;

  /**
   * Prepares a {@link MasterProcess} journal, rpc and web server using the given sockets.
   *
   * @param journalSystem The journaling system
   * @param rpcBindSocket a socket bound to an address that the master's rpc server will use
   * @param webBindSocket a socket bound to an address that the master's web server will use
   */
  public MasterProcess(JournalSystem journalSystem, ServerSocket rpcBindSocket,
      ServerSocket webBindSocket) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mMinWorkerThreads = ServerConfiguration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = ServerConfiguration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);
    mRpcBindSocket = rpcBindSocket;
    mWebBindSocket = webBindSocket;
    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);
  }

  /**
   * @return this master's rpc address
   */
  public abstract InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  protected void startServing() {
    startServing("", "");
  }

  abstract void startServing(String startMessage, String stopMessage);

  /**
   * @return the uptime of the master in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  public abstract InetSocketAddress getWebAddress();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  public boolean isServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  void registerServices(GrpcServerBuilder serverBuilder,
      Map<alluxio.grpc.ServiceType, GrpcService> services) {
    for (Map.Entry<alluxio.grpc.ServiceType, GrpcService> serviceEntry : services.entrySet()) {
      serverBuilder.addService(serviceEntry.getKey(), serviceEntry.getValue());
      LOG.info("registered service {}", serviceEntry.getKey().name());
    }
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing() && mWebServer != null && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  public static final ImmutableList<ServiceType> MASTER_PROCESS_PORT_SERVICE_LIST =
      ImmutableList.of(
          ServiceType.MASTER_RPC,
          ServiceType.MASTER_WEB,
          ServiceType.JOB_MASTER_RPC,
          ServiceType.JOB_MASTER_WEB,
          ServiceType.PROXY_WEB);

  /**
   * Creates a socket bound to a specific port to "reserve" the port until the master process is
   * ready to start. Only a select number of corresponding {@link ServiceType} are valid for this
   * operation. The returned socket should then be passed to the master process for creation. The
   * socket created respects the current {@link ServerConfiguration}
   *
   * @param service The service corresponding to the port that a socket should be created for
   * @return ServerSocket for a given service updating the configuration if necessary
   */
  protected static ServerSocket setupBindSocket(ServiceType service) {
    if (!MASTER_PROCESS_PORT_SERVICE_LIST.contains(service)) {
      throw new IllegalArgumentException(String.format(
          "Cannot set up BindSocket for service \"%s\"", service.getServiceName()));
    }

    PropertyKey portKey = service.getPortKey();

    // Extract the port from the generated socket.
    // When running tests, it is fine to use port '0' so the system will figure out what port to
    // use (any random free port).
    // In a production or any real deployment setup, port '0' should not be used as it will make
    // deployment more complicated.
    if (!ServerConfiguration.getBoolean(PropertyKey.TEST_MODE)) {
      Preconditions.checkState(ServerConfiguration.getInt(portKey) > 0,
          String.format("MasterProcess %s is only allowed to be zero in test mode.",
              portKey.getName()));
    }
    InetSocketAddress bindAddress = NetworkAddressUtils
        .getBindAddress(service, ServerConfiguration.global());
    try {
      ServerSocket bindSocket =
          new ServerSocket(ServerConfiguration.getInt(portKey), 50, bindAddress.getAddress());
      if (bindAddress.getPort() == 0) {
        ServerConfiguration.set(portKey, bindSocket.getLocalPort());
      }
      return bindSocket;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** This method will close the socket upon first initialization. */
  protected InetSocketAddress getWebAddressFromBindSocket() throws IOException {
    Preconditions.checkNotNull(mWebBindSocket, "mWebBindSocket");
    InetSocketAddress socketAddr = new InetSocketAddress(mWebBindSocket.getInetAddress(),
        mWebBindSocket.getLocalPort());
    mWebBindSocket.close();
    return socketAddr;
  }

  /** This method will close the socket upon first initialization. */
  protected SocketAddress getRpcAddressFromBindSocket() throws IOException {
    Preconditions.checkNotNull(mRpcBindSocket, "mRpcBindSocket");
    SocketAddress addr = mRpcBindSocket.getLocalSocketAddress();
    mRpcBindSocket.close();
    return addr;
  }
}
