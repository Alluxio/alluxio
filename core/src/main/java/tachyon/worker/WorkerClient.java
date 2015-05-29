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

package tachyon.worker;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;
import tachyon.util.NetworkUtils;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 * 
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safe.
 */
public class WorkerClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient mMasterClient;
  private static final int CONNECTION_RETRY_TIMES = 5;

  private WorkerService.Client mClient;
  private TProtocol mProtocol;
  private InetSocketAddress mWorkerAddress;
  private boolean mConnected = false;
  private boolean mIsLocal = false;
  private final ExecutorService mExecutorService;
  private Future<?> mHeartbeat;

  private final TachyonConf mTachyonConf;

  /**
   * Create a WorkerClient, with a given MasterClient.
   *
   * @param masterClient
   * @param executorService
   * @throws IOException
   */
  public WorkerClient(MasterClient masterClient, ExecutorService executorService, TachyonConf conf)
      throws IOException {
    mMasterClient = masterClient;
    mExecutorService = executorService;
    mTachyonConf = conf;
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId The id of the block
   * @throws IOException
   */
  public synchronized void accessBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.accessBlock(blockId);
    } catch (TException e) {
      LOG.error("TachyonClient accessLocalBlock(" + blockId + ") failed");
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker that the checkpoint file of the file has been added.
   * 
   * @param fileId The id of the checkpointed file
   * @throws IOException
   */
  public synchronized void addCheckpoint(int fileId) throws IOException {
    mustConnect();

    try {
      mClient.addCheckpoint(mMasterClient.getUserId(), fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (FailedToCheckpointException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker to checkpoint the file asynchronously.
   * 
   * @param fid The id of the file
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean asyncCheckpoint(int fid) throws IOException {
    mustConnect();

    try {
      return mClient.asyncCheckpoint(fid);
    } catch (TachyonException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param blockId The id of the block
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cacheBlock(mMasterClient.getUserId(), blockId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify worker that the block has been cancelled
   * 
   * @param blockId The Id of the block to be cancelled
   * @throws IOException
   */
  public synchronized void cancelBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cancelBlock(mMasterClient.getUserId(), blockId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Close the connection to worker. Shutdown the heartbeat thread.
   */
  @Override
  public synchronized void close() {
    if (mConnected) {
      try {
        mProtocol.getTransport().close();
      } finally {
        if (mHeartbeat != null) {
          mHeartbeat.cancel(true);
        }
      }
      mConnected = false;
    }
  }

  /**
   * Open the connection to the worker. And start the heartbeat thread.
   * 
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  private synchronized boolean connect() throws IOException {
    if (!mConnected) {
      NetAddress workerNetAddress = null;
      try {
        String localHostName = NetworkUtils.getLocalHostName();
        LOG.info("Trying to get local worker host : " + localHostName);
        workerNetAddress = mMasterClient.user_getWorker(false, localHostName);
        mIsLocal = true;
      } catch (NoWorkerException e) {
        LOG.info(e.getMessage());
        workerNetAddress = null;
      } catch (UnknownHostException e) {
        LOG.info(e.getMessage());
        workerNetAddress = null;
      }

      if (workerNetAddress == null) {
        try {
          workerNetAddress = mMasterClient.user_getWorker(true, "");
        } catch (NoWorkerException e) {
          LOG.info("No worker running in the system: " + e.getMessage());
          mClient = null;
          return false;
        }
      }

      String host = NetworkUtils.getFqdnHost(workerNetAddress);
      int port = workerNetAddress.mPort;
      mWorkerAddress = new InetSocketAddress(host, port);
      LOG.info("Connecting " + (mIsLocal ? "local" : "remote") + " worker @ " + mWorkerAddress);

      // TODO: auth connection ???
      mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(host, port)));
      mClient = new WorkerService.Client(mProtocol);

      HeartbeatExecutor heartBeater =
          new WorkerClientHeartbeatExecutor(this, mMasterClient.getUserId());
      String threadName = "worker-heartbeat-" + mWorkerAddress;
      int interval = mTachyonConf.getInt(Constants.USER_HEARTBEAT_INTERVAL_MS,
          Constants.SECOND_MS);
      mHeartbeat =
          mExecutorService.submit(new HeartbeatThread(threadName, heartBeater, interval));

      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mConnected = true;
    }

    return mConnected;
  }

  /**
   * @return the address of the worker.
   */
  public synchronized InetSocketAddress getAddress() {
    return mWorkerAddress;
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   * 
   * @return The user temporary folder in the under file system
   * @throws IOException
   */
  public synchronized String getUserUfsTempFolder() throws IOException {
    mustConnect();

    try {
      return mClient.getUserUfsTempFolder(mMasterClient.getUserId());
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * @return true if it's connected to the worker, false otherwise.
   */
  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * @return true if the worker is local, false otherwise.
   */
  public synchronized boolean isLocal() {
    if (!isConnected()) {
      try {
        connect();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return mIsLocal;
  }

  /**
   * Lock the block, therefore, the worker will not evict the block from the memory until it is
   * unlocked.
   * 
   * @param blockId The id of the block
   * @return the path of the block file locked
   * @throws IOException
   */
  public synchronized String lockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.lockBlock(blockId, mMasterClient.getUserId());
    } catch (FileDoesNotExistException e) {
      return null;
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Connect to the worker.
   * 
   * @throws IOException
   */
  public synchronized void mustConnect() throws IOException {
    int tries = 0;
    while (tries ++ <= CONNECTION_RETRY_TIMES) {
      if (connect()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the worker");
  }

  /**
   * Promote block back to the top StorageTier
   * 
   * @param blockId The id of the block that will be promoted
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean promoteBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.promoteBlock(blockId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Get temporary path for the block from the worker
   * 
   * @param blockId The id of the block
   * @param initialBytes The initial size bytes allocated for the block
   * @return the temporary path of the block
   * @throws IOException
   */
  public synchronized String requestBlockLocation(long blockId, long initialBytes)
      throws IOException {
    mustConnect();

    try {
      return mClient.requestBlockLocation(mMasterClient.getUserId(), blockId, initialBytes);
    } catch (OutOfSpaceException e) {
      throw new IOException(e);
    } catch (FileAlreadyExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Request space for some block from worker
   * 
   * @param blockId The id of the block
   * @param requestBytes The requested space size, in bytes
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long blockId, long requestBytes) throws IOException {
    mustConnect();

    try {
      return mClient.requestSpace(mMasterClient.getUserId(), blockId, requestBytes);
    } catch (OutOfSpaceException e) {
      return false;
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Unlock the block
   * 
   * @param blockId The id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean unlockBlock(long blockId) throws IOException {
    mustConnect();

    try {
      return mClient.unlockBlock(blockId, mMasterClient.getUserId());
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Users' heartbeat to the Worker.
   * 
   * @param userId The id of the user
   * @throws IOException
   */
  public synchronized void userHeartbeat(long userId) throws IOException {
    mustConnect();

    try {
      mClient.userHeartbeat(userId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
