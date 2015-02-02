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

import java.io.IOException;

import org.apache.thrift.TException;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;
import tachyon.worker.hierarchy.StorageDir;

/**
 * <code>WorkerServiceHandler</code> handles all the RPC calls to the worker.
 */
public class WorkerServiceHandler implements WorkerService.Iface {
  private final WorkerStorage mWorkerStorage;

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
    try {
      mWorkerStorage.addCheckpoint(userId, fileId);
    } catch (IOException e) {
      throw new TException(e);
    }
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
    try {
      mWorkerStorage.cacheBlock(userId, blockId);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public void cancelBlock(long userId, long blockId) throws TException {
    mWorkerStorage.cancelBlock(userId, blockId);
  }

  @Override
  public String getUserUfsTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserUfsTempFolder(userId);
  }

  @Override
  public String lockBlock(long blockId, long userId) throws FileDoesNotExistException, TException {
    StorageDir storageDir = mWorkerStorage.lockBlock(blockId, userId);
    if (storageDir == null) {
      throw new FileDoesNotExistException("Block file not found! blockId:" + blockId);
    } else {
      return storageDir.getBlockFilePath(blockId);
    }
  }

  @Override
  public boolean promoteBlock(long blockId) throws TException {
    return mWorkerStorage.promoteBlock(blockId);
  }

  @Override
  public String requestBlockLocation(long userId, long blockId, long initialBytes)
      throws OutOfSpaceException, FileAlreadyExistException, TException {
    return mWorkerStorage.requestBlockLocation(userId, blockId, initialBytes);
  }

  @Override
  public boolean requestSpace(long userId, long blockId, long requestBytes)
      throws FileDoesNotExistException, TException {
    return mWorkerStorage.requestSpace(userId, blockId, requestBytes);
  }

  @Override
  public boolean unlockBlock(long blockId, long userId) throws TException {
    return mWorkerStorage.unlockBlock(blockId, userId);
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }
}
