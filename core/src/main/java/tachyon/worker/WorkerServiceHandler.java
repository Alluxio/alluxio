/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.IOException;

import org.apache.thrift.TException;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;

/**
 * <code>WorkerServiceHandler</code> handles all the RPC calls to the worker.
 */
public class WorkerServiceHandler implements WorkerService.Iface {
  private WorkerStorage mWorkerStorage;

  public WorkerServiceHandler(WorkerStorage workerStorage) {
    mWorkerStorage = workerStorage;
  }

  @Override
  public void accessBlock(long blockId) throws TException {
    mWorkerStorage.accessBlock(blockId);
  }

  @Override
  public void addCheckpoint(long userId, int fileId) throws FileDoesNotExistException,
      SuspectedFileSizeException, FailedToCheckpointException, BlockInfoException, TException {
    mWorkerStorage.addCheckpoint(userId, fileId);
  }

  @Override
  public boolean asyncCheckpoint(int fileId) throws TachyonException, TException {
    try {
      return mWorkerStorage.asyncCheckpoint(fileId);
    } catch (IOException e) {
      throw new TachyonException(e.getMessage());
    }
  }

  @Override
  public void cacheBlock(long userId, long blockId) throws FileDoesNotExistException,
      SuspectedFileSizeException, BlockInfoException, TException {
    mWorkerStorage.cacheBlock(userId, blockId);
  }

  @Override
  public String getDataFolder() throws TException {
    return mWorkerStorage.getDataFolder();
  }

  @Override
  public String getUserTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserTempFolder(userId);
  }

  @Override
  public String getUserUnderfsTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserUnderfsTempFolder(userId);
  }

  @Override
  public void lockBlock(long blockId, long userId) throws TException {
    mWorkerStorage.lockBlock(blockId, userId);
  }

  @Override
  public boolean requestSpace(long userId, long requestBytes) throws TException {
    return mWorkerStorage.requestSpace(userId, requestBytes);
  }

  @Override
  public void returnSpace(long userId, long returnedBytes) throws TException {
    mWorkerStorage.returnSpace(userId, returnedBytes);
  }

  @Override
  public void unlockBlock(long blockId, long userId) throws TException {
    mWorkerStorage.unlockBlock(blockId, userId);
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }
}