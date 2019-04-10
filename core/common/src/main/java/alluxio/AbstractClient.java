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

package alluxio;

import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ServiceType;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.metrics.CommonMetrics;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.LoginUser;
import alluxio.grpc.GrpcChannel;
import alluxio.util.SecurityUtils;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for clients.
 */
@ThreadSafe
public abstract class AbstractClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  protected InetSocketAddress mAddress;

  /** Underlying channel to the target service. */
  protected GrpcChannel mChannel;

  /** Used to query service version for the remote service type. */
  protected ServiceVersionClientServiceGrpc.ServiceVersionClientServiceBlockingStub mVersionService;

  /** Is true if this client is currently connected. */
  protected boolean mConnected = false;

  /**
   * Is true if this client was closed by the user. No further actions are possible after the client
   * is closed.
   */
  protected volatile boolean mClosed = false;

  /**
   * Stores the service version; used for detecting incompatible client-server pairs.
   */
  protected long mServiceVersion;

  protected ClientContext mContext;

  /**
   * Creates a new client base.
   *
   * @param context information required to connect to Alluxio
   * @param address the address
   */
  public AbstractClient(ClientContext context, InetSocketAddress address) {
    this(context, address, () -> RetryUtils.defaultClientRetry(
        context.getConf().getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
        context.getConf().getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
        context.getConf().getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS)));
  }

  /**
   * Creates a new client base.
   *
   * @param context information required to connect to Alluxio
   * @param address the address
   * @param retryPolicySupplier factory for retry policies to be used when performing RPCs
   */
  public AbstractClient(ClientContext context, InetSocketAddress address,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mAddress = address;
    mContext = Preconditions.checkNotNull(context);
    mRetryPolicySupplier = retryPolicySupplier;
    mServiceVersion = Constants.UNKNOWN_SERVICE_VERSION;
  }

  /**
   * @return the type of remote service
   */
  protected abstract ServiceType getRemoteServiceType();

  protected long getRemoteServiceVersion() throws AlluxioStatusException {
    return retryRPC(new RpcCallable<Long>() {
      public Long call() {
        return mVersionService.getServiceVersion(
            GetServiceVersionPRequest.newBuilder().setServiceType(getRemoteServiceType()).build())
            .getVersion();
      }
    });
  }

  /**
   * @return a string representing the specific service
   */
  protected abstract String getServiceName();

  /**
   * @return the client service version
   */
  protected abstract long getServiceVersion();

  /**
   * Checks that the service version is compatible with the client.
   *
   * @param clientVersion the client version
   */
  protected void checkVersion(long clientVersion) throws IOException {
    if (mServiceVersion == Constants.UNKNOWN_SERVICE_VERSION) {
      mServiceVersion = getRemoteServiceVersion();
      if (mServiceVersion != clientVersion) {
        throw new IOException(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(getServiceName(),
            clientVersion, mServiceVersion));
      }
    }
  }

  /**
   * This method is called after the connection is made to the remote. Implementations should create
   * internal state to finish the connection process.
   */
  protected void afterConnect() throws IOException {
    // Empty implementation.
  }

  /**
   * This method is called before the connection is connected. Implementations should add any
   * additional operations before the connection is connected.
   * loading the cluster defaults
   */
  protected void beforeConnect()
      throws IOException {
    // Bootstrap once for clients
    if (!isConnected()) {
      if (!mContext.getConf().clusterDefaultsLoaded()) {
        mContext.updateWithClusterDefaults(mAddress);
      }
    }
  }

  /**
   * This method is called after the connection is disconnected. Implementations should clean up any
   * additional state created for the connection.
   */
  protected void afterDisconnect() {
    // Empty implementation.
  }

  /**
   * This method is called before the connection is disconnected. Implementations should add any
   * additional operations before the connection is disconnected.
   */
  protected void beforeDisconnect() {
    // Empty implementation.
  }

  /**
   * Connects with the remote.
   */
  @Override
  public synchronized void connect() throws AlluxioStatusException {
    if (mConnected) {
      return;
    }
    disconnect();
    Preconditions.checkState(!mClosed, "Client is closed, will not try to connect.");

    IOException lastConnectFailure = null;
    RetryPolicy retryPolicy = mRetryPolicySupplier.get();

    while (retryPolicy.attempt()) {
      if (mClosed) {
        throw new FailedPreconditionException("Failed to connect: client has been closed");
      }
      // Re-query the address in each loop iteration in case it has changed (e.g. master
      // failover).
      try {
        mAddress = getAddress();
      } catch (UnavailableException e) {
        LOG.warn("Failed to determine {} rpc address ({}): {}",
            getServiceName(), retryPolicy.getAttemptCount(), e.toString());
        continue;
      }
      if (mAddress.isUnresolved()) {
        // Sometimes the acquired addressed wasn't resolved, retry resolving before
        // using it to connect.
        LOG.info("Retry resolving address {}", mAddress);
        // Creates a new InetSocketAddress to force resolving the hostname again.
        mAddress = new InetSocketAddress(mAddress.getHostName(), mAddress.getPort());
        if (mAddress.isUnresolved()) {
          LOG.warn("Failed to resolve address on retry {}", mAddress);
        }
      }
      try {
        beforeConnect();
        LOG.info("Alluxio client (version {}) is trying to connect with {} @ {}",
            RuntimeConstants.VERSION, getServiceName(), mAddress);
        mChannel = GrpcChannelBuilder
            .newBuilder(new GrpcServerAddress(mAddress), mContext.getConf())
            .setSubject(mContext.getSubject())
            .build();
        // Create stub for version service on host
        mVersionService = ServiceVersionClientServiceGrpc.newBlockingStub(mChannel);
        mConnected = true;
        afterConnect();
        checkVersion(getServiceVersion());
        LOG.info("Alluxio client (version {}) is connected with {} @ {}", RuntimeConstants.VERSION,
            getServiceName(), mAddress);
        return;
      } catch (IOException e) {
        LOG.warn("Failed to connect ({}) with {} @ {}: {}", retryPolicy.getAttemptCount(),
            getServiceName(), mAddress, e.getMessage());
        lastConnectFailure = e;
      }
    }
    // Reaching here indicates that we did not successfully connect.

    if (mChannel != null) {
      mChannel.shutdown();
    }

    if (mAddress == null) {
      throw new UnavailableException(
          String.format("Failed to determine address for %s after %s attempts", getServiceName(),
              retryPolicy.getAttemptCount()));
    }

    /**
     * Throw as-is if {@link UnauthenticatedException} occurred.
     */
    if (lastConnectFailure instanceof UnauthenticatedException) {
      throw (AlluxioStatusException) lastConnectFailure;
    }

    throw new UnavailableException(String.format("Failed to connect to %s @ %s after %s attempts",
        getServiceName(), mAddress, retryPolicy.getAttemptCount()), lastConnectFailure);
  }

  /**
   * Closes the connection with the Alluxio remote and does the necessary cleanup. It should be used
   * if the client has not connected with the remote for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      Preconditions.checkNotNull(mChannel, PreconditionMessage.CHANNEL_NULL_WHEN_CONNECTED);
      LOG.debug("Disconnecting from the {} @ {}", getServiceName(), mAddress);
      beforeDisconnect();
      mChannel.shutdown();
      mConnected = false;
      afterDisconnect();
    }
  }

  /**
   * @return true if this client is connected to the remote
   */
  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * Closes the connection with the remote permanently. This instance should be not be reused after
   * closing.
   */
  @Override
  public synchronized void close() {
    disconnect();
    mClosed = true;
  }

  @Override
  public synchronized InetSocketAddress getAddress() throws UnavailableException {
    return mAddress;
  }

  /**
   * The RPC to be executed in {@link #retryRPC(RpcCallable)}.
   *
   * @param <V> the return value of {@link #call()}
   */
  protected interface RpcCallable<V> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws StatusRuntimeException when any exception defined in gRPC happens
     */
    V call() throws StatusRuntimeException;
  }

  /**
   * Tries to execute an RPC defined as a {@link RpcCallable}.
   *
   * If a {@link UnavailableException} occurs, a reconnection will be tried through
   * {@link #connect()} and the action will be re-executed.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   */
  protected synchronized <V> V retryRPC(RpcCallable<V> rpc) throws AlluxioStatusException {
    return retryRPCInternal(rpc, () -> null);
  }

  /**
   * Tries to execute an RPC defined as a {@link RpcCallable}. Metrics will be recorded based on
   * the provided rpc name.
   *
   * If a {@link UnavailableException} occurs, a reconnection will be tried through
   * {@link #connect()} and the action will be re-executed.
   *
   * @param rpc the RPC call to be executed
   * @param rpcName the human readable name of the RPC call
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   */
  protected synchronized <V> V retryRPC(RpcCallable<V> rpc, String rpcName)
      throws AlluxioStatusException {
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(rpcName)).time()) {
      return retryRPCInternal(rpc, () -> {
        MetricsSystem.counter(getQualifiedRetryMetricName(rpcName)).inc();
        return null;
      });
    } catch (Exception e) {
      MetricsSystem.counter(getQualifiedFailureMetricName(rpcName)).inc();
      throw e;
    }
  }

  private synchronized <V> V retryRPCInternal(RpcCallable<V> rpc, Supplier<Void> onRetry)
      throws AlluxioStatusException {
    RetryPolicy retryPolicy = mRetryPolicySupplier.get();
    Exception ex = null;
    while (retryPolicy.attempt()) {
      if (mClosed) {
        throw new FailedPreconditionException("Client is closed");
      }
      connect();
      try {
        return rpc.call();
      } catch (StatusRuntimeException e) {
        AlluxioStatusException se = AlluxioStatusException.fromStatusRuntimeException(e);
        if (se.getStatusCode() == Status.Code.UNAVAILABLE
            || se.getStatusCode() == Status.Code.CANCELLED
            || se.getStatusCode() == Status.Code.UNAUTHENTICATED
            || e.getCause() instanceof UnresolvedAddressException) {
          ex = se;
        } else {
          throw se;
        }
      }
      LOG.info("Rpc failed ({}): {}", retryPolicy.getAttemptCount(), ex.toString());
      onRetry.get();
      disconnect();
    }
    throw new UnavailableException("Failed after " + retryPolicy.getAttemptCount()
        + " attempts: " + ex.toString(), ex);
  }

  // TODO(calvin): General tag logic should be in getMetricName
  private String getQualifiedMetricName(String metricName) {
    try {
      if (SecurityUtils.isAuthenticationEnabled(mContext.getConf())
          && LoginUser.get(mContext.getConf()) != null) {
        return Metric.getMetricNameWithTags(metricName, CommonMetrics.TAG_USER,
            LoginUser.get(mContext.getConf()).getName());
      } else {
        return metricName;
      }
    } catch (IOException e) {
      return metricName;
    }
  }

  // TODO(calvin): This should not be in this class
  private String getQualifiedRetryMetricName(String metricName) {
    return getQualifiedMetricName(metricName + "Retries");
  }

  // TODO(calvin): This should not be in this class
  private String getQualifiedFailureMetricName(String metricName) {
    return getQualifiedMetricName(metricName + "Failures");
  }
}
