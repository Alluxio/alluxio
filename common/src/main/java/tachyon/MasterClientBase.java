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
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.conf.TachyonConf;
import tachyon.retry.ExponentialBackoffRetry;
import tachyon.retry.RetryPolicy;
import tachyon.util.network.NetworkAddressUtils;

/**
 * The base class for master clients.
 */
public abstract class MasterClientBase implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of times to retry a particular RPC. */
  protected static final int RPC_MAX_NUM_RETRY = 30;

  protected final boolean mUseZookeeper;
  protected final ExecutorService mExecutorService;
  protected final TachyonConf mTachyonConf;

  protected InetSocketAddress mMasterAddress = null;
  protected TProtocol mProtocol = null;
  /** Is true if this client is currently connected to the master. */
  protected boolean mConnected = false;
  /**
   * Is true if this client was closed by the user. No further actions are possible after the client
   * is closed.
   */
  protected boolean mClosed = false;

  /**
   * Creates a new master client base.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public MasterClientBase(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mUseZookeeper = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER);
    mMasterAddress = Preconditions.checkNotNull(masterAddress);
    mExecutorService = Preconditions.checkNotNull(executorService);
  }

  /**
   * Returns the name of the service.
   *
   * @return A string representing the specific master
   */
  protected abstract String getServiceName();

  /**
   * This method is called after the connection is made to the master. Implementations should create
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
   * Connects with the master.
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
      mMasterAddress = getMasterAddress();
      LOG.info("Tachyon client (version " + Version.VERSION + ") is trying to connect with "
          + getServiceName() + " master @ " + mMasterAddress);

      TProtocol binaryProtocol = new TBinaryProtocol(new TFramedTransport(
          new TSocket(NetworkAddressUtils.getFqdnHost(mMasterAddress), mMasterAddress.getPort())));
      mProtocol = new TMultiplexedProtocol(binaryProtocol, getServiceName());
      try {
        mProtocol.getTransport().open();
        LOG.info("Client registered with " + getServiceName() + " master @ " + mMasterAddress);
        mConnected = true;
        afterConnect();
        return;
      } catch (TTransportException e) {
        LOG.error("Failed to connect (" + retry.getRetryCount() + ") to " + getServiceName()
            + " master @ " + mMasterAddress + " : " + e.getMessage());
        if (!retry.attemptRetry()) {
          break;
        }
      }
    }
    // Reaching here indicates that we did not successfully connect.
    throw new IOException("Failed to connect to " + getServiceName() + " master @ " + mMasterAddress
        + " after " + (retry.getRetryCount()) + " attempts");
  }

  /**
   * Closes the connection with the Tachyon Master and do the necessary cleanup. It should be used
   * if the client has not connected with the master for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      LOG.debug("Disconnecting from the " + getServiceName() + " master {}", mMasterAddress);
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
   * @return true if this client is connected to the master
   */
  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * Closes the connection with the master permanently. This instance should be not be reused after
   * closing.
   */
  @Override
  public synchronized void close() {
    disconnect();
    mClosed = true;
  }

  /**
   * Returns the {@link InetSocketAddress} of the master. If zookeeper is used, this will consult
   * the zookeeper instance for the master address.
   *
   * @return the {@link InetSocketAddress} of the master
   */
  private synchronized InetSocketAddress getMasterAddress() {
    if (!mUseZookeeper) {
      return mMasterAddress;
    }

    LeaderInquireClient leaderInquireClient =
        LeaderInquireClient.getClient(mTachyonConf.get(Constants.ZOOKEEPER_ADDRESS, null),
            mTachyonConf.get(Constants.ZOOKEEPER_LEADER_PATH, null));
    try {
      String temp = leaderInquireClient.getMasterAddress();
      return NetworkAddressUtils.parseInetSocketAddress(temp);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }
}
