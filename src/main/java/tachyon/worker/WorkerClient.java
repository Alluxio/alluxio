/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.conf.UserConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 * 
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safe.
 */
public class WorkerClient {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final WorkerService.Client CLIENT;

  private TProtocol mProtocol;
  private InetSocketAddress mWorkerAddress;
  private boolean mIsConnected = false;
  private long mUserId;
  private HeartbeatThread mHeartbeatThread = null;

  private String mRootFolder = null;

  public WorkerClient(InetSocketAddress address, long userId) {
    mWorkerAddress = address;
    mProtocol =
        new TBinaryProtocol(new TFramedTransport(new TSocket(mWorkerAddress.getHostName(),
            mWorkerAddress.getPort())));
    CLIENT = new WorkerService.Client(mProtocol);

    mUserId = userId;
    mHeartbeatThread =
        new HeartbeatThread("WorkerClientToWorkerHeartbeat", new WorkerClientHeartbeatExecutor(
            this, mUserId), UserConf.get().HEARTBEAT_INTERVAL_MS);
    mHeartbeatThread.setDaemon(true);
  }

  public synchronized void accessBlock(long blockId) throws TException {
    CLIENT.accessBlock(blockId);
  }

  public synchronized void addCheckpoint(long userId, int fileId) throws IOException, TException {
    try {
      CLIENT.addCheckpoint(userId, fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (FailedToCheckpointException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean asyncCheckpoint(int fid) throws TachyonException, TException {
    return CLIENT.asyncCheckpoint(fid);
  }

  public synchronized void cacheBlock(long userId, long blockId) throws IOException, TException {
    try {
      CLIENT.cacheBlock(userId, blockId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    }
  }

  public synchronized void close() {
    if (mIsConnected) {
      mProtocol.getTransport().close();
      mHeartbeatThread.shutdown();
      mIsConnected = false;
    }
  }

  public synchronized String getDataFolder() throws TException {
    if (mRootFolder == null) {
      mRootFolder = CLIENT.getDataFolder();
    }

    return mRootFolder;
  }

  public synchronized String getUserTempFolder(long userId) throws TException {
    return CLIENT.getUserTempFolder(userId);
  }

  public synchronized String getUserUnderfsTempFolder(long userId) throws TException {
    return CLIENT.getUserUnderfsTempFolder(userId);
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized void lockBlock(long blockId, long userId) throws TException {
    CLIENT.lockBlock(blockId, userId);
  }

  public synchronized boolean open() {
    if (!mIsConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mHeartbeatThread.start();
      mIsConnected = true;
    }

    return mIsConnected;
  }

  public synchronized boolean requestSpace(long userId, long requestBytes) throws TException {
    return CLIENT.requestSpace(userId, requestBytes);
  }

  public synchronized void returnSpace(long userId, long returnSpaceBytes) throws TException {
    CLIENT.returnSpace(userId, returnSpaceBytes);
  }

  public synchronized void unlockBlock(long blockId, long userId) throws TException {
    CLIENT.unlockBlock(blockId, userId);
  }

  public synchronized void userHeartbeat(long userId) throws TException {
    CLIENT.userHeartbeat(userId);
  }
}