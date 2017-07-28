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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.Status;
import alluxio.exception.status.UnavailableException;
import alluxio.exception.status.UnimplementedException;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetServiceVersionTOptions;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * The base class for clients.
 */
// TODO(peis): Consolidate this to ThriftClientPool.
@ThreadSafe
public abstract class AbstractClient implements Client {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

  /** The pattern of exception message when client and server transport frame sizes do not match. */
  private static final Pattern FRAME_SIZE_EXCEPTION_PATTERN =
      Pattern.compile("Frame size \\((\\d+)\\) larger than max length");

  private static final int BASE_SLEEP_MS =
      (int) Configuration.getMs(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS);
  private static final int MAX_SLEEP_MS =
      (int) Configuration.getMs(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);

  /** The number of times to retry a particular RPC. */
  protected static final int RPC_MAX_NUM_RETRY =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY);

  protected InetSocketAddress mAddress = null;
  protected TProtocol mProtocol = null;

  /** Is true if this client is currently connected. */
  protected boolean mConnected = false;

  /**
   * Is true if this client was closed by the user. No further actions are possible after the client
   * is closed.
   */
  protected boolean mClosed = false;

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
    mAddress = Preconditions.checkNotNull(address, "address");
    mServiceVersion = Constants.UNKNOWN_SERVICE_VERSION;
    mTransportProvider = TransportProvider.Factory.create();
    mParentSubject = subject;
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
   * Connects with the remote.
   */
  public synchronized void connect() throws IOException {
    if (mConnected) {
      return;
    }
    disconnect();
    Preconditions.checkState(!mClosed, "Client is closed, will not try to connect.");

    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
    while (!mClosed) {
      mAddress = getAddress();
      LOG.info("Alluxio client (version {}) is trying to connect with {} @ {}",
          RuntimeConstants.VERSION, getServiceName(), mAddress);

      TProtocol binaryProtocol =
          new TBinaryProtocol(mTransportProvider.getClientTransport(mParentSubject, mAddress));
      mProtocol = new TMultiplexedProtocol(binaryProtocol, getServiceName());
      try {
        mProtocol.getTransport().open();
        LOG.info("Client registered with {} @ {}", getServiceName(), mAddress);
        mConnected = true;
        afterConnect();
        checkVersion(getClient(), getServiceVersion());
        return;
      } catch (IOException e) {
        if (e.getMessage() != null && FRAME_SIZE_EXCEPTION_PATTERN.matcher(e.getMessage()).find()) {
          // See an error like "Frame size (67108864) larger than max length (16777216)!",
          // pointing to the helper page.
          String message = String.format("Failed to connect with %s @ %s: %s. "
              + "This exception may be caused by incorrect network configuration. "
              + "Please consult %s for common solutions to address this problem.",
              getServiceName(), mAddress, e.getMessage(), RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL);
          throw new UnimplementedException(message, e);
        }
        throw e;
      } catch (TTransportException e) {
        LOG.warn("Failed to connect ({}) with {} @ {}: {}", retryPolicy.getRetryCount(),
            getServiceName(), mAddress, e.getMessage());
        if (e.getCause() instanceof java.net.SocketTimeoutException) {
          // Do not retry if socket timeout.
          String message = "Thrift transport open times out. Please check whether the "
              + "authentication types match between client and server. Note that NOSASL client "
              + "is not able to connect to servers with SIMPLE security mode.";
          throw new UnavailableException(message, e);
        }
        // TODO(peis): Consider closing the connection here as well.
        if (!retryPolicy.attemptRetry()) {
          break;
        }
      }
    }
    // Reaching here indicates that we did not successfully connect.
    throw new UnavailableException(String.format("Failed to connect to %s @ %s after %s attempts",
        getServiceName(), mAddress, retryPolicy.getRetryCount()));
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

  /**
   * Closes the connection, then queries and sets current remote address.
   */
  public synchronized void resetConnection() {
    disconnect();
    mAddress = getAddress();
  }

  /**
   * @return the {@link InetSocketAddress} of the remote
   */
  public synchronized InetSocketAddress getAddress() {
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
  protected synchronized <V> V retryRPC(RpcCallable<V> rpc) throws IOException {
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
    while (!mClosed) {
      Exception ex;
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
      LOG.warn(ex.toString());
      disconnect();
      if (!retryPolicy.attemptRetry()) {
        throw new UnavailableException(
            "Failed after " + retryPolicy.getRetryCount() + " retries: " + ex.toString(), ex);
      }
    }
    throw new FailedPreconditionException("Client is closed");
  }
}
