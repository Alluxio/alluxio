/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio;

import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.AuthenticationUtils;
import alluxio.thrift.AlluxioService;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.ThriftIOException;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The base class for clients.
 */
@ThreadSafe
public abstract class AbstractClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of times to retry a particular RPC. */
  protected static final int RPC_MAX_NUM_RETRY = 30;

  protected final Configuration mConfiguration;
  protected final String mMode;

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

  /**
   * Creates a new client base.
   *
   * @param address the address
   * @param configuration the Alluxio configuration
   * @param mode the mode of the client for display
   */
  public AbstractClient(InetSocketAddress address, Configuration configuration, String mode) {
    mConfiguration = Preconditions.checkNotNull(configuration);
    mAddress = Preconditions.checkNotNull(address);
    mMode = mode;
    mServiceVersion = Constants.UNKNOWN_SERVICE_VERSION;
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
  private void checkVersion(AlluxioService.Client client, long version) throws IOException {
    if (mServiceVersion == Constants.UNKNOWN_SERVICE_VERSION) {
      try {
        mServiceVersion = client.getServiceVersion();
      } catch (TException e) {
        throw new IOException(e.getMessage());
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
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public synchronized void connect() throws IOException, ConnectionFailedException {
    if (mConnected) {
      return;
    }
    disconnect();
    Preconditions.checkState(!mClosed, "Client is closed, will not try to connect.");

    int maxConnectsTry = mConfiguration.getInt(Constants.MASTER_RETRY_COUNT);
    final int BASE_SLEEP_MS = 50;
    RetryPolicy retry =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, Constants.SECOND_MS, maxConnectsTry);
    while (!mClosed) {
      mAddress = getAddress();
      LOG.info("Alluxio client (version {}) is trying to connect with {} {} @ {}", Version.VERSION,
              getServiceName(), mMode, mAddress);

      TProtocol binaryProtocol =
          new TBinaryProtocol(AuthenticationUtils.getClientTransport(mConfiguration, mAddress));
      mProtocol = new TMultiplexedProtocol(binaryProtocol, getServiceName());
      try {
        mProtocol.getTransport().open();
        LOG.info("Client registered with {} {} @ {}", getServiceName(), mMode, mAddress);
        mConnected = true;
        afterConnect();
        checkVersion(getClient(), getServiceVersion());
        return;
      } catch (TTransportException e) {
        LOG.error("Failed to connect (" + retry.getRetryCount() + ") to " + getServiceName() + " "
            + mMode + " @ " + mAddress + " : " + e.getMessage());
        if (!retry.attemptRetry()) {
          break;
        }
      }
    }
    // Reaching here indicates that we did not successfully connect.
    throw new ConnectionFailedException("Failed to connect to " + getServiceName() + " " + mMode
        + " @ " + mAddress + " after " + (retry.getRetryCount()) + " attempts");
  }

  /**
   * Closes the connection with the Alluxio remote and does the necessary cleanup. It should be used
   * if the client has not connected with the remote for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      LOG.debug("Disconnecting from the {} {} {}", getServiceName(), mMode, mAddress);
      mConnected = false;
    }
    try {
      beforeDisconnect();
      if (mProtocol != null) {
        mProtocol.getTransport().close();
      }
    } finally {
      afterDisconnect();
    }
  }

  /**
   * Returns the connected status of the client.
   *
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
   * Returns the {@link InetSocketAddress} of the remote.
   *
   * @return the {@link InetSocketAddress} of the remote
   */
  protected synchronized InetSocketAddress getAddress() {
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
   * Same with {@link RpcCallable} except that this RPC call throws {@link AlluxioTException} and
   * is to be executed in {@link #retryRPC(RpcCallableThrowsAlluxioTException)}.
   *
   * @param <V> the return value of {@link #call()}
   */
  protected interface RpcCallableThrowsAlluxioTException<V> {
    /**
     * The task where RPC happens.
     *
     * @return RPC result
     * @throws AlluxioTException when any {@link AlluxioException} happens during RPC and is wrapped
     *         into {@link AlluxioTException}
     * @throws TException when any exception defined in thrift happens
     */
    V call() throws AlluxioTException, TException;
  }

  /**
   * Tries to execute an RPC defined as a {@link RpcCallable}, if error
   * happens in one execution, a reconnection will be tried through {@link #connect()} and the
   * action will be re-executed.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY} or {@link #close()} has
   *         been called before calling this method or during the retry
   * @throws ConnectionFailedException if network connection failed
   */
  protected synchronized <V> V retryRPC(RpcCallable<V> rpc) throws IOException,
      ConnectionFailedException {
    int retry = 0;
    while (!mClosed && (retry++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return rpc.call();
      } catch (ThriftIOException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }

  /**
   * Similar to {@link #retryRPC(RpcCallable)} except that the RPC call may throw
   * {@link AlluxioTException} and once it is thrown, it will be transformed into
   * {@link AlluxioException} and be thrown.
   *
   * @param rpc the RPC call to be executed
   * @param <V> type of return value of the RPC call
   * @return the return value of the RPC call
   * @throws AlluxioException when {@link AlluxioTException} is thrown by the RPC call
   * @throws IOException when retries exceeds {@link #RPC_MAX_NUM_RETRY} or {@link #close()} has
   *         been called before calling this method or during the retry
   */
  protected synchronized <V> V retryRPC(RpcCallableThrowsAlluxioTException<V> rpc)
      throws AlluxioException, IOException {
    int retry = 0;
    while (!mClosed && (retry++) <= RPC_MAX_NUM_RETRY) {
      connect();
      try {
        return rpc.call();
      } catch (AlluxioTException e) {
        throw AlluxioException.from(e);
      } catch (ThriftIOException e) {
        throw new IOException(e);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    throw new IOException("Failed after " + retry + " retries.");
  }
}
