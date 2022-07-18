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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.ServiceNotFoundException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ServiceType;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.util.SecurityUtils;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.UnresolvedAddressException;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AbstractClient} is the base class for all the grpc clients in alluxio.
 * It provides framework methods for the grpc workflow like connection, version checking
 * and automatic retrying for rpc calls.
 * <p>
 * Concrete Child classes extend this class by filling in the missing component, like where
 * to find the actual server address {@link AbstractClient#queryGrpcServerAddress}, and rpc
 * callables to {@link AbstractClient#retryRPC(RpcCallable, Logger, String, String, Object...)}
 * that carries actual logic.
 *
 */
@ThreadSafe
public abstract class AbstractClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * Grpc Address of the remote server.
   * This field is lazily initialized by {@link AbstractClient#queryGrpcServerAddress},
   * and could only be null right after instantiation and before use.
   */
  protected GrpcServerAddress mServerAddress = null;

  /** Underlying channel to the target service. */
  protected GrpcChannel mChannel;

  @SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "the error seems a bug in findbugs")
  /* Used to query service version for the remote service type. */
  protected ServiceVersionClientServiceGrpc.ServiceVersionClientServiceBlockingStub mVersionService;

  /** Is true if this client is currently connected. */
  protected boolean mConnected = false;

  /**
   * Is true if this client was closed by the user. No further actions are possible after the client
   * is closed.
   */
  protected volatile boolean mClosed = false;

  /**
   * Stores the actual remote service version, used to compare with expected local version.
   */
  protected long mServiceVersion;

  /** Context of the client. */
  protected ClientContext mContext;

  /** If rpc call takes more than this duration, signal warning. */
  private final long mRpcThreshold;

  /**
   * Creates a new client base with default retry policy supplier.
   *
   * @param context information required to connect to Alluxio
   */
  protected AbstractClient(ClientContext context) {
    this(context, RetryUtils::defaultClientRetry);
  }

  /**
   * Creates a new client base with specified retry policy supplier.
   *
   * @param context information required to connect to Alluxio
   * @param retryPolicySupplier factory for retry policies to be used when performing RPCs
   */
  protected AbstractClient(ClientContext context, Supplier<RetryPolicy> retryPolicySupplier) {
    mContext = Preconditions.checkNotNull(context, "context");
    mRetryPolicySupplier = retryPolicySupplier;
    mServiceVersion = Constants.UNKNOWN_SERVICE_VERSION;
    mRpcThreshold = mContext.getClusterConf().getMs(PropertyKey.USER_LOGGING_THRESHOLD);
  }

  /**
   * @return the expected type of remote service
   */
  protected abstract ServiceType getRemoteServiceType();

  /**
   * @return the actual remote service version
   * @throws AlluxioStatusException if query rpc failed
   */
  protected long getRemoteServiceVersion() throws AlluxioStatusException {
    // Calling directly as this method is subject to an encompassing retry loop.
    try {
      return mVersionService
          .getServiceVersion(
              GetServiceVersionPRequest.newBuilder().setServiceType(getRemoteServiceType()).build())
          .getVersion();
    } catch (Throwable t) {
      throw AlluxioStatusException.fromThrowable(t);
    }
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
      mContext.loadConfIfNotLoaded(getConfAddress());
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
        mServerAddress = queryGrpcServerAddress();
      } catch (UnavailableException e) {
        LOG.debug("Failed to determine {} rpc address ({}): {}",
            getServiceName(), retryPolicy.getAttemptCount(), e.toString());
        continue;
      }
      try {
        beforeConnect();
        LOG.debug("Alluxio client (version {}) is trying to connect with {} @ {}",
            RuntimeConstants.VERSION, getServiceName(), mServerAddress);
        AlluxioConfiguration conf = mContext.getClusterConf();
        // set up rpc group channel
        mChannel = GrpcChannelBuilder
            .newBuilder(mServerAddress, conf)
            .setSubject(mContext.getSubject())
            .build();
        // Create stub for version service on host
        mVersionService = ServiceVersionClientServiceGrpc.newBlockingStub(mChannel);
        mConnected = true;
        afterConnect();
        checkVersion(getServiceVersion());

        LOG.debug("Alluxio client (version {}) is connected with {} @ {}", RuntimeConstants.VERSION,
            getServiceName(), mServerAddress);
        return;
      } catch (IOException e) {
        LOG.debug("Failed to connect ({}) with {} @ {}", retryPolicy.getAttemptCount(),
            getServiceName(), mServerAddress, e);
        lastConnectFailure = e;
        if (e instanceof UnauthenticatedException) {
          // If there has been a failure in opening GrpcChannel, it's possible because
          // the authentication credential has expired. Re-login.
          mContext.getUserState().relogin();
        }
        if (e instanceof NotFoundException) {
          // service is not found in the server, skip retry
          break;
        }
      }
    }
    // Reaching here indicates that we did not successfully connect.
    if (mServerAddress == null) {
      throw new UnavailableException(
          String.format("Failed to determine address for %s after %s attempts", getServiceName(),
              retryPolicy.getAttemptCount()));
    }

    /*
     * Throw as-is if {@link UnauthenticatedException} occurred.
     */
    if (lastConnectFailure instanceof UnauthenticatedException) {
      throw (AlluxioStatusException) lastConnectFailure;
    }
    if (lastConnectFailure instanceof NotFoundException) {
      throw new NotFoundException(lastConnectFailure.getMessage(),
          new ServiceNotFoundException(lastConnectFailure.getMessage(), lastConnectFailure));
    }

    throw new UnavailableException(
        String.format(
            "Failed to connect to master (%s) after %s attempts."
                + "Please check if Alluxio master is currently running on \"%s\". Service=\"%s\"",
            mServerAddress, retryPolicy.getAttemptCount(), mServerAddress, getServiceName()),
        lastConnectFailure);
  }

  @Override
  public synchronized void disconnect() {
    if (mConnected) {
      Preconditions.checkNotNull(mChannel,
          "The client channel should never be null when the client is connected");
      LOG.debug("Disconnecting from the {} @ {}", getServiceName(), mServerAddress);
      beforeDisconnect();
      mChannel.shutdown();
      mConnected = false;
      afterDisconnect();
    }
  }

  @Override
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

  /**
   *  {@link AbstractClient} works with Grpc Servers.
   *  Child classes should only override this method to query the address
   *  of the grpc server they talk to. The conversion from {@link GrpcServerAddress}
   *  to more generic {@link SocketAddress} required by {@link Client#getRemoteSockAddress()}
   *  is handled by this class.
   *
   * @return the {@link GrpcServerAddress} of the remote server
   * @throws UnavailableException if address can't be determined
   */
  protected abstract GrpcServerAddress queryGrpcServerAddress() throws UnavailableException;

  @Override
  public synchronized SocketAddress getRemoteSockAddress() throws UnavailableException {
    if (mServerAddress == null) {
      mServerAddress = queryGrpcServerAddress();
    }
    return mServerAddress.getSocketAddress();
  }

  @Override
  public synchronized String getRemoteHostName() throws UnavailableException {
    if (mServerAddress == null) {
      mServerAddress = queryGrpcServerAddress();
    }
    return mServerAddress.getHostName();
  }

  /**
   * By default, return the same underlying address as
   * {@link AbstractClient#getRemoteSockAddress()}.
   * Child classes should override this implementation if they intend to have different
   * address to fetch configuration.
   *
   * @return the remote address of the configuration server
   * @throws UnavailableException if address cannot be determined
   */
  @Override
  public synchronized InetSocketAddress getConfAddress() throws UnavailableException {
    if (mServerAddress == null) {
      mServerAddress = queryGrpcServerAddress();
    }

    SocketAddress sockAddress = mServerAddress.getSocketAddress();
    if (sockAddress instanceof InetSocketAddress) {
      return (InetSocketAddress) sockAddress;
    }

    // We would reach here if a child's implementation provided a socket address
    // that is not a TCP/IP socket, e.g. a block worker client that talks to
    // a worker via Unix domain socket. But client configuration is only provided
    // by meta master via a TCP/IP socket, so reaching here indicates a bug in
    // the implementation of the child.
    throw new UnavailableException("Remote is not an InetSockAddress");
  }

  /**
   * The RPC to be executed in {@link #retryRPC}.
   *
   * @param <V> the return value of {@link #call()}
   */
  @FunctionalInterface
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
   * Tries to execute an RPC defined as a {@link RpcCallable}. Metrics will be recorded based on
   * the provided rpc name.
   *
   * If a {@link UnavailableException} occurs, a reconnection will be tried through
   * {@link #connect()} and the action will be re-executed.
   *
   * @param <V> type of return value of the RPC call
   * @param rpc the RPC call to be executed
   * @param logger the logger to use for this call
   * @param rpcName the human-readable name of the RPC call
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @return the return value of the RPC call
   * @throws AlluxioStatusException status exception
   */
  protected synchronized <V> V retryRPC(RpcCallable<V> rpc, Logger logger, String rpcName,
      String description, Object... args) throws AlluxioStatusException {
    return retryRPC(mRetryPolicySupplier.get(), rpc, logger, rpcName, description, args);
  }

  protected synchronized <V> V retryRPC(RetryPolicy retryPolicy, RpcCallable<V> rpc,
      Logger logger, String rpcName, String description, Object... args)
      throws AlluxioStatusException {
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    // TODO(binfan): create RPC context so we could get RPC duration from metrics timer directly
    long startMs = System.currentTimeMillis();
    logger.debug("Enter: {}({})", rpcName, debugDesc);
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(rpcName)).time()) {
      V ret = retryRPCInternal(retryPolicy, rpc, () -> {
        MetricsSystem.counter(getQualifiedRetryMetricName(rpcName)).inc();
        return null;
      });
      long duration = System.currentTimeMillis() - startMs;
      logger.debug("Exit (OK): {}({}) in {} ms", rpcName, debugDesc, duration);
      if (duration >= mRpcThreshold) {
        logger.warn("{}({}) returned {} in {} ms (>={} ms)",
            rpcName, String.format(description, args),
            CommonUtils.summarizeCollection(ret), duration, mRpcThreshold);
      }
      return ret;
    } catch (Exception e) {
      long duration = System.currentTimeMillis() - startMs;
      MetricsSystem.counter(getQualifiedFailureMetricName(rpcName)).inc();
      logger.debug("Exit (ERROR): {}({}) in {} ms: {}",
          rpcName, debugDesc, duration, e.toString());
      if (duration >= mRpcThreshold) {
        logger.warn("{}({}) exits with exception [{}] in {} ms (>={}ms)",
            rpcName, String.format(description, args), e, duration, mRpcThreshold);
      }
      throw e;
    }
  }

  private synchronized <V> V retryRPCInternal(RetryPolicy retryPolicy, RpcCallable<V> rpc,
      Supplier<Void> onRetry) throws AlluxioStatusException {
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
      LOG.debug("Rpc failed ({}): ", retryPolicy.getAttemptCount(), ex);
      onRetry.get();
      disconnect();
    }
    throw new UnavailableException(String.format("Failed after %d attempts: %s",
        retryPolicy.getAttemptCount(), ex), ex);
  }

  // TODO(calvin): General tag logic should be in getMetricName
  private String getQualifiedMetricName(String metricName) {
    try {
      if (SecurityUtils.isAuthenticationEnabled(mContext.getClusterConf())
          && mContext.getUserState().getUser() != null) {
        return Metric.getMetricNameWithTags(metricName, MetricInfo.TAG_USER,
            mContext.getUserState().getUser().getName());
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

  @Override
  public boolean isClosed() {
    return mClosed;
  }
}
