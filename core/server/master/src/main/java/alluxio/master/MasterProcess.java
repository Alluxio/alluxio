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
import alluxio.grpc.GrpcServerBuilder;
import alluxio.master.journal.JournalSystem;
import alluxio.master.service.SimpleService;
import alluxio.master.service.rpc.RpcServerService;
import alluxio.master.service.web.WebServerService;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

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
  /** The master registry. */
  protected final MasterRegistry mRegistry = new MasterRegistry();

  /** Rpc server bind address. **/
  final InetSocketAddress mRpcBindAddress;
  /** Web server bind address. **/
  final InetSocketAddress mWebBindAddress;
  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;
  /** The connection address for the web service. */
  final InetSocketAddress mWebConnectAddress;

  /** The start time for when the master started. */
  final long mStartTimeMs;
  // holds the simple services registered with the master, should only be accessed through
  // #registerService
  List<SimpleService> mServices = new ArrayList<>();

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
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(rpcService, Configuration.global());
    mWebConnectAddress = NetworkAddressUtils.getConnectAddress(webService, Configuration.global());
    mStartTimeMs = System.currentTimeMillis();
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_START_TIME.getName(), () -> mStartTimeMs);
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
   * Used in master factory after instantiating the master process and before starting the master
   * process. This allows you to define with services are necessary for the operation of the
   * master process.
   * @param service a service to register for the functioning of the master process
   */
  public void registerService(SimpleService service) {
    mServices.add(service);
  }

  /**
   * @return a fully configured rpc server except for the executor option (provided by the
   * {@link #createRpcExecutorService()}) and the rpc services (provided by {@link #mRegistry}
   */
  public abstract GrpcServerBuilder createBaseRpcServer();

  /**
   * This method is decoupled from {@link #createBaseRpcServer()} because the
   * {@link AlluxioExecutorService} needs to be managed (i.e. started and stopped) independently
   * of the rpc server that uses it
   * @return a custom executor service to be used in the rpc server
   */
  public Optional<AlluxioExecutorService> createRpcExecutorService() {
    return Optional.empty();
  }

  /**
   * @return the {@link SafeModeManager} if you have one
   */
  public Optional<SafeModeManager> getSafeModeManager() {
    return Optional.empty();
  }

  /**
   * @return this master's rpc bind address
   */
  public final InetSocketAddress getRpcBindAddress() {
    return mRpcBindAddress;
  }

  /**
   * @return this master's rpc connect address
   */
  public final InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  /**
   * @param clazz the class of the master to retrieve
   * @return the desired master from the registry if it exists
   * @param <T> parameterized class of the desired master
   */
  public final <T extends Master> T getMaster(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  /**
   * Use only to instantiate {@link SimpleService}s.
   * @return the master registry
   */
  public final MasterRegistry getRegistry() {
    return mRegistry;
  }

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
   * @return the master's web bind address
   */
  public final InetSocketAddress getWebBindAddress() {
    return mWebBindAddress;
  }

  /**
   * @return the master's web connect address
   */
  public final InetSocketAddress getWebAddress() {
    return mWebConnectAddress;
  }

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  public boolean isGrpcServing() {
    return mServices.stream().anyMatch(service -> service instanceof RpcServerService
        && ((RpcServerService) service).isServing());
  }

  /**
   * @return true if the system is serving the web server, false otherwise
   */
  public boolean isWebServing() {
    return mServices.stream().anyMatch(service -> service instanceof WebServerService
        && ((WebServerService) service).isServing());
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

  /**
   * @return the primary selector
   */
  public PrimarySelector getPrimarySelector() {
    return mLeaderSelector;
  }
}
