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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.master.journal.JournalSystem;
import alluxio.network.RejectingServer;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.WebServer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Defines a set of methods which any {@link MasterProcess} should implement.
 *
 * This class serves as a common implementation for functions that both the AlluxioMasterProcess and
 * AlluxioJobMasterProcess use. Each master should have an RPC server, web server, and journaling
 * system which can serve client requests.
 */
public abstract class MasterProcess implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcess.class);

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** Rpc server bind address. **/
  final InetSocketAddress mRpcBindAddress;

  /** Web server bind address. **/
  final InetSocketAddress mWebBindAddress;

  /** The start time for when the master started. */
  final long mStartTimeMs = System.currentTimeMillis();

  /**
   * Rejecting servers for used by backup masters to reserve ports but reject connection requests.
   */
  private RejectingServer mRejectingRpcServer;
  private RejectingServer mRejectingWebServer;

  /** The RPC server. */
  protected GrpcServer mGrpcServer;

  /** The web ui server. */
  protected WebServer mWebServer;

  /**
   * Prepares a {@link MasterProcess} journal, rpc and web server using the given sockets.
   *
   * @param journalSystem the journaling system
   * @param rpcService the rpc service type
   * @param webService the web service type
   */
  public MasterProcess(JournalSystem journalSystem, ServiceType rpcService,
      ServiceType webService) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mRpcBindAddress = configureAddress(rpcService);
    mWebBindAddress = configureAddress(webService);
  }

  private static InetSocketAddress configureAddress(ServiceType service) {
    InstancedConfiguration conf = ServerConfiguration.global();
    int port = NetworkAddressUtils.getPort(service, conf);
    if (!ConfigurationUtils.isHaMode(conf) && port == 0) {
      throw new RuntimeException(
          String.format("%s port must be nonzero in single-master mode", service));
    }
    if (port == 0) {
      try (ServerSocket s = new ServerSocket(0)) {
        s.setReuseAddress(true);
        conf.set(service.getPortKey(), s.getLocalPort());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return NetworkAddressUtils.getBindAddress(service, conf);
  }

  /**
   * @return this master's rpc address
   */
  public abstract InetSocketAddress getRpcAddress();

  /**
   * Gets the registered class from the master registry.
   *
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get
   * @return the given master
   */
  public abstract <T extends Master> T getMaster(Class<T> clazz);

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
          () -> {
            boolean ready = isServing();
            if (ready && !ServerConfiguration.getBoolean(PropertyKey.TEST_MODE)) {
              ready &= mWebServer != null && mWebServer.getServer().isRunning();
            }
            return ready;
          }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  protected void startRejectingServers() {
    if (mRejectingRpcServer == null) {
      mRejectingRpcServer = new RejectingServer(mRpcBindAddress);
      mRejectingRpcServer.start();
    }
    if (mRejectingWebServer == null) {
      mRejectingWebServer = new RejectingServer(mWebBindAddress);
      mRejectingWebServer.start();
    }
  }

  protected void stopRejectingRpcServer() {
    if (mRejectingRpcServer != null) {
      mRejectingRpcServer.stopAndJoin();
      mRejectingRpcServer = null;
    }
  }

  protected void stopRejectingWebServer() {
    if (mRejectingWebServer != null) {
      mRejectingWebServer.stopAndJoin();
      mRejectingWebServer = null;
    }
  }

  protected void stopRejectingServers() {
    stopRejectingRpcServer();
    stopRejectingWebServer();
  }
}
