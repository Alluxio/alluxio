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

import tachyon.StorageDirId;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.ClientLocationInfo;
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
  public void accessBlock(long storageDirId, long blockId) throws TException {
    mWorkerStorage.accessBlock(storageDirId, blockId);
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
  public void cacheBlock(long userId, long storageDirId, long blockId)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    try {
      mWorkerStorage.cacheBlock(userId, storageDirId, blockId);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public ClientLocationInfo getLocalBlockLocation(long blockId)
      throws FileDoesNotExistException, TException {
    StorageDir storageDir = mWorkerStorage.getStorageDirByBlockId(blockId);
    if (storageDir == null) {
      throw new FileDoesNotExistException("Block not found! blockId:" + blockId);
    } else {
      return new ClientLocationInfo(storageDir.getStorageDirId(),
          storageDir.getBlockFilePath(blockId));
    }
  }

  @Override
  public String getUserLocalTempFolder(long userId, long storageDirId) throws TException {
    return mWorkerStorage.getUserLocalTempFolder(userId, storageDirId);
  }

  @Override
  public String getUserUfsTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserUfsTempFolder(userId);
  }

  @Override
  public ClientLocationInfo lockBlock(long blockId, long userId)
      throws FileDoesNotExistException, TException {
    long storageDirId = mWorkerStorage.lockBlock(blockId, userId);
    StorageDir storageDir = mWorkerStorage.getStorageDirById(storageDirId);
    if (storageDir == null) {
      throw new FileDoesNotExistException("Block file not found! blockId" + blockId);
    } else {
      return new ClientLocationInfo(storageDir.getStorageDirId(),
          storageDir.getBlockFilePath(blockId));
    }
  }

  @Override
  public boolean promoteBlock(long userId, long blockId) throws TException {
    return mWorkerStorage.promoteBlock(userId, blockId);
  }

  @Override
  public ClientLocationInfo requestSpace(long userId, long requestBytes)
      throws OutOfSpaceException, TException {
    StorageDir storageDir = mWorkerStorage.requestSpace(userId, requestBytes);
    if (storageDir == null) {
      throw new OutOfSpaceException("Failed to allocate space! requestBytes:" + requestBytes);
    } else {
      return new ClientLocationInfo(storageDir.getStorageDirId(),
          storageDir.getUserTempPath(userId));
    }
  }

  @Override
  public boolean requestSpaceInPlace(long userId, long storageDirId, long requestBytes)
      throws TException {
    return mWorkerStorage.requestSpace(userId, storageDirId, requestBytes);
  }

  @Override
  public void returnSpace(long userId, long storageDirId, long returnedBytes) throws TException {
    mWorkerStorage.returnSpace(userId, storageDirId, returnedBytes);
  }

  @Override
  public boolean unlockBlock(long blockId, long userId) throws TException {
    long storageDirId = mWorkerStorage.unlockBlock(blockId, userId);
    if (StorageDirId.isUnknown(storageDirId)) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }
}
