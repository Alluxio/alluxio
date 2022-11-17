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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.grpc.GrpcServer;
import alluxio.master.journal.JournalSystem;
import alluxio.master.service.SimpleService;
import alluxio.metrics.MetricsSystem;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Defines a set of methods which any {@link MasterProcess} should implement.
 *
 * This class serves as a common implementation for functions that both the
 * {@link AlluxioMasterProcess} and {@link AlluxioJobMasterProcess} use.
 * Each master should have an RPC server, web server, and journaling
 * system which can serve client requests.
 */
public abstract class MasterProcess implements Process {
  private static final Logger LOG = LoggerFactory.getLogger(MasterProcess.class);

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;
  protected final PrimarySelector mLeaderSelector;

  /** Rpc server bind address. **/
  final InetSocketAddress mRpcBindAddress;

  /** Web server bind address. **/
  final InetSocketAddress mWebBindAddress;

  /** The start time for when the master started. */
  final long mStartTimeMs;

  Set<SimpleService> mServices = new HashSet<>();

  /**
   * Rejecting servers for used by backup masters to reserve ports but reject connection requests.
   */
  private RejectingServer mRejectingRpcServer;
  private RejectingServer mRejectingWebServer;

  /** The RPC server. */
  @Nullable @GuardedBy("mGrpcServerLock")
  protected GrpcServer mGrpcServer;
  protected final Lock mGrpcServerLock = new ReentrantLock();

  /** The web ui server. */
  @Nullable @GuardedBy("mWebServerLock")
  protected WebServer mWebServer;
  protected final Lock mWebServerLock = new ReentrantLock();

  protected Thread mServingThread = null;

  /**
   * Prepares a {@link MasterProcess} journal, rpc and web server using the given sockets.
   *
   * @param journalSystem the journaling system
   * @param leaderSelector the leader selector
   * @param webService    the web service type
   * @param rpcService    the rpc service type
   */
  public MasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector,
      ServiceType webService, ServiceType rpcService) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mLeaderSelector = Preconditions.checkNotNull(leaderSelector, "leaderSelector");
    mRpcBindAddress = configureAddress(rpcService);
    mWebBindAddress = configureAddress(webService);
    mStartTimeMs = System.currentTimeMillis();
  }

  private static InetSocketAddress configureAddress(ServiceType service) {
    AlluxioConfiguration conf = Configuration.global();
    int port = NetworkAddressUtils.getPort(service, conf);
    if (!ConfigurationUtils.isHaMode(conf) && port == 0) {
      throw new RuntimeException(
          String.format("%s port must be nonzero in single-master mode", service));
    }
    if (port == 0) {
      try (ServerSocket s = new ServerSocket(0)) {
        s.setReuseAddress(true);
        Configuration.set(service.getPortKey(), s.getLocalPort());
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

  /**
   * @return the uptime of the master in milliseconds
   */
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  /**
   * @return a newly created web server for this master
   */
  public abstract WebServer createWebServer();

  /**
   * @return the master's web address
   */
  public abstract InetSocketAddress getWebAddress();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  public boolean isGrpcServing() {
    synchronized (mGrpcServerLock) {
      return mGrpcServer != null && mGrpcServer.isServing();
    }
  }

  /**
   * @return true if the system is serving the web server, false otherwise
   */
  public boolean isWebServing() {
    synchronized (mWebServerLock) {
      return mWebServer != null && mWebServer.getServer().isRunning();
    }
  }

  /**
   * @return true if the system is serving the metric sink, false otherwise
   */
  public boolean isMetricSinkServing() {
    return MetricsSystem.isStarted();
  }

  /**
   * Waits until the grpc server is ready to serve requests.
   *
   * @param timeoutMs how long to wait in milliseconds
   * @return whether the grpc server became ready before the specified timeout
   */
  public boolean waitForGrpcServerReady(int timeoutMs) {
    return pollFor(this + " to start", this::isGrpcServing, timeoutMs);
  }

  /**
   * Waits until the web server is ready to serve requests.
   *
   * @param timeoutMs how long to wait in milliseconds
   * @return whether the web server became ready before the specified timeout
   */
  public boolean waitForWebServerReady(int timeoutMs) {
    return pollFor(this + " to start", this::isWebServing, timeoutMs);
  }

  /**
   * Wait until the metrics sinks have been started.
   *
   * @param timeoutMs how long to wait in milliseconds
   * @return whether the metrics sinks have begun serving before the specified timeout
   */
  public boolean waitForMetricSinkServing(int timeoutMs) {
    return pollFor("metrics sinks to start", this::isMetricSinkServing, timeoutMs);
  }

  private boolean pollFor(String message, Supplier<Boolean> waitFor, int timeoutMs) {
    try {
      CommonUtils.waitFor(message, waitFor, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    return waitForGrpcServerReady(timeoutMs);
  }

  protected void startRejectingServers() {
    if (mRejectingRpcServer == null) {
      mRejectingRpcServer = new RejectingServer(mRpcBindAddress);
      mRejectingRpcServer.start();
    }
    if (!isWebServing() && mRejectingWebServer == null) {
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
