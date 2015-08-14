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

package tachyon.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.LeaderInquireClient;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.retry.ExponentialBackoffRetry;
import tachyon.retry.RetryPolicy;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.WorkerInfo;
import tachyon.util.network.NetworkAddressUtils;

/**
 * The BlockMaster client, for clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety.
 */
// TODO: better deal with exceptions.
public final class BlockMasterClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final boolean mUseZookeeper;
  private final ExecutorService mExecutorService;
  private final TachyonConf mTachyonConf;

  private BlockMasterService.Client mClient = null;
  private InetSocketAddress mMasterAddress = null;
  private TProtocol mProtocol = null;
  private volatile boolean mConnected;
  private volatile boolean mIsClosed;
  // TODO: implement client heartbeat to the master
  private Future<?> mHeartbeat;

  public BlockMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
    mUseZookeeper = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false);
    if (!mUseZookeeper) {
      mMasterAddress = masterAddress;
    }
    mConnected = false;
    mIsClosed = false;
    mExecutorService = executorService;
  }

  /**
   * Connect with the master; an exception is thrown if this fails.
   *
   * @throws IOException
   */
  public synchronized void connect() throws IOException {
    if (mConnected) {
      return;
    }
    disconnect();

    if (mIsClosed) {
      throw new IOException("Client is closed, will not try to connect");
    }

    Exception lastException = null;
    int maxConnectsTry = mTachyonConf.getInt(Constants.MASTER_RETRY_COUNT, 29);
    RetryPolicy retry = new ExponentialBackoffRetry(50, Constants.SECOND_MS, maxConnectsTry);
    do {
      mMasterAddress = getMasterAddress();

      LOG.info("Tachyon client (version " + Version.VERSION
          + ") is trying to connect with block master @ " + mMasterAddress);

      mProtocol = new TBinaryProtocol(new TFramedTransport(
          new TSocket(NetworkAddressUtils.getFqdnHost(mMasterAddress), mMasterAddress.getPort())));
      mClient = new BlockMasterService.Client(mProtocol);
      try {
        mProtocol.getTransport().open();

        // TODO: get a user id?

        // TODO: start client heartbeat thread, and submit it to the executor service
      } catch (TTransportException e) {
        lastException = e;
        LOG.error("Failed to connect (" + retry.getRetryCount() + ") to block master @ "
            + mMasterAddress + " : " + e.getMessage());
        if (mHeartbeat != null) {
          mHeartbeat.cancel(true);
        }
        continue;
      }

      LOG.info("Client registered with block master @ " + mMasterAddress);

      mConnected = true;
      return;
    } while (retry.attemptRetry() && !mIsClosed);

    // Reaching here indicates that we did not successfully connect.
    throw new IOException("Failed to connect to block master @ " + mMasterAddress + " after "
        + (retry.getRetryCount()) + " attempts", lastException);
  }

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

  /**
   * Close the connection with the Tachyon Master and do the necessary cleanup. It should be used if
   * the client has not connected with the master for a while, for example.
   */
  public synchronized void disconnect() {
    if (mConnected) {
      LOG.debug("Disconnecting from the block master {}", mMasterAddress);
      mConnected = false;
    }
    try {
      if (mProtocol != null) {
        mProtocol.getTransport().close();
      }
    } finally {
      if (mHeartbeat != null) {
        mHeartbeat.cancel(true);
      }
    }
  }

  /**
   * Get the info of a list of workers.
   *
   * @return A list of worker info returned by master
   * @throws IOException
   */
  public synchronized List<WorkerInfo> getWorkerInfoList() throws IOException {
    while (!mIsClosed) {
      connect();

      try {
        return mClient.getWorkerInfoList();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return null;
  }

  /**
   * Get the total capacity in bytes.
   *
   * @return capacity in bytes
   * @throws IOException
   */
  public synchronized long getCapacityBytes() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getCapacityBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  /**
   * Get the amount of used space in bytes.
   *
   * @return amount of used space in bytes
   * @throws IOException
   */
  public synchronized long getUsedBytes() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getUsedBytes();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * Close the connection with the block master permanently. This instance should be reused after
   * closing.
   */
  @Override
  public synchronized void close() {
    disconnect();
    mIsClosed = true;
  }
}
