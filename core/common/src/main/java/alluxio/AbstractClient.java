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

import alluxio.conf.Source;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.Status;
import alluxio.exception.status.UnavailableException;
import alluxio.network.thrift.BootstrapClientTransport;
import alluxio.network.thrift.ThriftUtils;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetConfigurationTOptions;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.MetaMasterClientService;
import alluxio.wire.ConfigProperty;
import alluxio.wire.Scope;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * The base class for clients.
 */
// TODO(peis): Consolidate this to ThriftClientPool.
@ThreadSafe
public abstract class AbstractClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

  private static Supplier<RetryPolicy> defaultRetry() {
    Duration maxRetryDuration = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION);
    Duration baseSleepMs = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS);
    Duration maxSleepMs = Configuration.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
    return () -> ExponentialTimeBoundedRetry.builder().withMaxDuration(maxRetryDuration)
        .withInitialSleep(baseSleepMs).withMaxSleep(maxSleepMs).build();
  }

  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  protected InetSocketAddress mAddress;
  protected TProtocol mProtocol;

  /** Whether the client needs to handshake with master first. */
  private static final boolean HANDSHAKE_NEEDED =
      Configuration.getBoolean(PropertyKey.USER_CONF_CLUSTER_DEFAULT_ENABLED);
  /** Whether the handshake is complete. */
  private static AtomicBoolean sHandshakeComplete = new AtomicBoolean(false);

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

  /** Handler to the transport provider according to the authentication type. */
  protected final TransportProvider mTransportProvider;

  private final Subject mParentSubject;

  /**
   * Creates a new client base.
   *
   * @param subject the parent subject, set to null if not present
   * @param address the address
   */
  public AbstractClient(Subject subject, InetSocketAddress address) {
    this(subject, address, defaultRetry());
  }

  /**
   * Creates a new client base.
   *
   * @param subject the parent subject, set to null if not present
   * @param address the address
   * @param retryPolicySupplier factory for retry policies to be used when performing RPCs
   */
  public AbstractClient(Subject subject, InetSocketAddress address,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mAddress = address;
    mParentSubject = subject;
    mRetryPolicySupplier = retryPolicySupplier;
    mServiceVersion = Constants.UNKNOWN_SERVICE_VERSION;
    mTransportProvider = TransportProvider.Factory.create();
  }

  /**
   * @return a Thrift service client
   */
  protected abstract AlluxioService.Client getClient();

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
   * @param client the service client
   * @param version the client version
   */
  protected void checkVersion(AlluxioService.Client client, long version) throws IOException {
    if (mServiceVersion == Constants.UNKNOWN_SERVICE_VERSION) {
      try {
        mServiceVersion = client.getServiceVersion(new GetServiceVersionTOptions()).getVersion();
      } catch (TException e) {
        throw new IOException(e);
      }
      if (mServiceVersion != version) {
        throw new IOException(ExceptionMessage.INCOMPATIBLE_VERSION.getMessage(getServiceName(),
            version, mServiceVersion));
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
   * Handshakes with meta master.
   */
  private void doHandshake() throws AlluxioStatusException {
    synchronized (AbstractClient.class) {
      if (isConnected() || sHandshakeComplete.get()) {
        return;
      }
      LOG.info("Alluxio client (version {}) is trying to bootstrap-connect with {}",
          RuntimeConstants.VERSION, mAddress);
      // A plain socket transport to bootstrap
      TSocket socket = ThriftUtils.createThriftSocket(mAddress);
      TTransport bootstrapTransport = new BootstrapClientTransport(socket);
      TProtocol protocol = ThriftUtils.createThriftProtocol(bootstrapTransport,
          Constants.META_MASTER_CLIENT_SERVICE_NAME);
      List<ConfigProperty> clusterConfig;
      try {
        bootstrapTransport.open();
        MetaMasterClientService.Client client = new MetaMasterClientService.Client(protocol);
        clusterConfig = client.getConfiguration(new GetConfigurationTOptions())
            .getConfigList()
            .stream()
            .map(ConfigProperty::fromThrift)
            .collect(Collectors.toList());
      } catch (TException e) {
        throw new UnavailableException(String.format(
            "Failed to handshake with master %s to load cluster default configuration values",
            mAddress), e);
      } finally {
        bootstrapTransport.close();
      }
      // merge conf returned by master as the cluster default into Configuration
      Properties clusterProps = new Properties();
      for (ConfigProperty property : clusterConfig) {
        String name = property.getName();
        // TODO(binfan): support propagating unsetting properties from master
        if (PropertyKey.isValid(name) && property.getValue() != null) {
          PropertyKey key = PropertyKey.fromString(name);
          if (!key.getScope().contains(Scope.CLIENT)) {
            continue;
          }
          String value = property.getValue();
          clusterProps.put(key, value);
          LOG.debug("Loading cluster default: {} ({}) -> {}", key, key.getScope(), value);
        }
      }
      String clientVersion = Configuration.get(PropertyKey.VERSION);
      String clusterVersion = clusterProps.get(PropertyKey.VERSION).toString();
      if (!clientVersion.equals(clusterVersion)) {
        LOG.warn("Alluxio client version ({}) does not match Alluxio cluster version ({})",
            clientVersion, clusterVersion);
        clusterProps.remove(PropertyKey.VERSION);
      }
      Configuration.merge(clusterProps, Source.CLUSTER_DEFAULT);
      Configuration.validate();
      // This needs to be the last
      sHandshakeComplete.set(true);
      LOG.info("Alluxio client has bootstrap-connected with {}", mAddress);
    }
  }

  private void doConnect() throws IOException, TTransportException {
    LOG.info("Alluxio client (version {}) is trying to connect with {} @ {}",
        RuntimeConstants.VERSION, getServiceName(), mAddress);
    // The plain socket transport
    TSocket socket = ThriftUtils.createThriftSocket(mAddress);
    // The wrapper transport
    TTransport clientTransport =
        mTransportProvider.getClientTransport(mParentSubject, socket);
    mProtocol = ThriftUtils.createThriftProtocol(clientTransport, getServiceName());
    mProtocol.getTransport().open();
    LOG.info("Client registered with {} @ {}", getServiceName(), mAddress);
    mConnected = true;
    afterConnect();
    checkVersion(getClient(), getServiceVersion());
  }

  /**
   * Connects with the remote.
   */
  public synchronized void connect() throws AlluxioStatusException {
    if (mConnected) {
      return;
    }
    disconnect();
    Preconditions.checkState(!mClosed, "Client is closed, will not try to connect.");

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
      // Bootstrap once
      if (HANDSHAKE_NEEDED && !sHandshakeComplete.get()) {
        try {
          doHandshake();
        } catch (UnavailableException e) {
          LOG.warn("Failed to handshake ({}) with {} @ {}: {}", retryPolicy.getAttemptCount(),
              getServiceName(), mAddress, e.getMessage());
          continue;
        }
      }
      try {
        doConnect();
        return;
      } catch (IOException | TTransportException e) {
        LOG.warn("Failed to connect ({}) with {} @ {}: {}", retryPolicy.getAttemptCount(),
            getServiceName(), mAddress, e.getMessage());
        if (e.getCause() instanceof java.net.SocketTimeoutException) {
          // Do not retry if socket timeout.
          String message = "Thrift transport open times out. Please check whether the "
              + "authentication types match between client and server. Note that NOSASL client "
              + "is not able to connect to servers with SIMPLE security mode.";
          throw new UnavailableException(message, e);
        }
      }
    }
    // Reaching here indicates that we did not successfully connect.
    if (mAddress == null) {
      throw new UnavailableException(
          String.format("Failed to determine address for %s after %s attempts", getServiceName(),
              retryPolicy.getAttemptCount()));
    }
    throw new UnavailableException(String.format("Failed to connect to %s @ %s after %s attempts",
        getServiceName(), mAddress, retryPolicy.getAttemptCount()));
  }

  /**
   * Closes the connection with the Alluxio remote and does the necessary cleanup. It should be used
   * if the client has not connected with the remote for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      Preconditions.checkNotNull(mProtocol, PreconditionMessage.PROTOCOL_NULL_WHEN_CONNECTED);
      LOG.debug("Disconnecting from the {} @ {}", getServiceName(), mAddress);
      beforeDisconnect();
      mProtocol.getTransport().close();
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
     * @throws TException when any exception defined in thrift happens
     */
    V call() throws TException;
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
    RetryPolicy retryPolicy = mRetryPolicySupplier.get();
    Exception ex = null;
    while (retryPolicy.attempt()) {
      if (mClosed) {
        throw new FailedPreconditionException("Client is closed");
      }
      connect();
      try {
        return rpc.call();
      } catch (AlluxioTException e) {
        AlluxioStatusException se = AlluxioStatusException.fromThrift(e);
        if (se.getStatus() == Status.UNAVAILABLE) {
          ex = se;
        } else {
          throw se;
        }
      } catch (TException e) {
        ex = e;
      }
      LOG.info("Rpc failed ({}): {}", retryPolicy.getAttemptCount(), ex.toString());
      disconnect();
    }
    throw new UnavailableException("Failed after " + retryPolicy.getAttemptCount()
            + " attempts: " + ex.toString(), ex);
  }
}
