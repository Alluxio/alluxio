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

package tachyon;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.conf.TachyonConf;
import tachyon.retry.ExponentialBackoffRetry;
import tachyon.retry.RetryPolicy;
import tachyon.security.authentication.AuthenticationUtils;

/**
 * The base class for clients.
 */
public abstract class ClientBase implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of times to retry a particular RPC. */
  protected static final int RPC_MAX_NUM_RETRY = 30;

  protected final ExecutorService mExecutorService;
  protected final TachyonConf mTachyonConf;
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
   * Creates a new client base.
   *
   * @param address the address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   * @param mode the mode of the client for display
   */
  public ClientBase(InetSocketAddress address, ExecutorService executorService,
      TachyonConf tachyonConf, String mode) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mAddress = Preconditions.checkNotNull(address);
    mExecutorService = Preconditions.checkNotNull(executorService);
    mMode = mode;
  }

  /**
   * Returns the name of the service.
   *
   * @return A string representing the specific service
   */
  protected abstract String getServiceName();

  /**
   * This method is called after the connection is made to the remote. Implementations should create
   * internal state to finish the connection process.
   */
  protected void afterConnect() {
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
   * Connects with the remote.
   *
   * @throws IOException if an I/O error occurs
   */
  public synchronized void connect() throws IOException {
    if (mConnected) {
      return;
    }
    disconnect();
    Preconditions.checkState(!mClosed, "Client is closed, will not try to connect.");

    int maxConnectsTry = mTachyonConf.getInt(Constants.MASTER_RETRY_COUNT);
    final int BASE_SLEEP_MS = 50;
    RetryPolicy retry =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, Constants.SECOND_MS, maxConnectsTry);
    while (!mClosed) {
      mAddress = getAddress();
      LOG.info("Tachyon client (version " + Version.VERSION + ") is trying to connect with "
          + getServiceName() + " " + mMode + " @ " + mAddress);

      TProtocol binaryProtocol =
          new TBinaryProtocol(AuthenticationUtils.getClientTransport(mTachyonConf, mAddress));
      mProtocol = new TMultiplexedProtocol(binaryProtocol, getServiceName());
      try {
        mProtocol.getTransport().open();
        LOG.info("Client registered with " + getServiceName() + " " + mMode + " @ " + mAddress);
        mConnected = true;
        afterConnect();
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
    throw new IOException("Failed to connect to " + getServiceName() + " " + mMode + " @ "
        + mAddress + " after " + (retry.getRetryCount()) + " attempts");
  }

  /**
   * Closes the connection with the Tachyon remote and do the necessary cleanup. It should be used
   * if the client has not connected with the remote for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      LOG.debug("Disconnecting from the " + getServiceName() + " " + mMode + " {}", mAddress);
      mConnected = false;
    }
    try {
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
   * Closes the connection, then query and set current remote address.
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

}
