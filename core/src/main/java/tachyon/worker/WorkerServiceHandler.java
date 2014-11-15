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
import java.util.List;

import org.apache.thrift.TException;

import tachyon.StorageDirId;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerDirInfo;
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
  public String getDataFolder() throws TException {
    return mWorkerStorage.getDataFolder();
  }

  @Override
  public String getUserTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserLocalTempFolder(userId);
  }

  @Override
  public String getUserUfsTempFolder(long userId) throws TException {
    return mWorkerStorage.getUserUfsTempFolder(userId);
  }

  @Override
  public List<WorkerDirInfo> getWorkerDirInfos() throws TException {
    return mWorkerStorage.getWorkerDirInfos();
  }

  @Override
  public long lockBlock(long userId, long storageDirId, long blockId) throws TException {
    return mWorkerStorage.lockBlock(userId, storageDirId, blockId);
  }

  @Override
  public boolean promoteBlock(long userId, long storageDirId, long blockId) throws TException {
    return mWorkerStorage.promoteBlock(userId, storageDirId, blockId);
  }

  @Override
  public long requestSpace(long userId, long requestBytes) throws TException {
    StorageDir storageDir = mWorkerStorage.requestSpace(userId, requestBytes);
    if (storageDir == null) {
      return StorageDirId.unknownId();
    } else {
      return storageDir.getStorageDirId();
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
  public long unlockBlock(long userId, long storageDirId, long blockId) throws TException {
    return mWorkerStorage.unlockBlock(userId, storageDirId, blockId);
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }
}
