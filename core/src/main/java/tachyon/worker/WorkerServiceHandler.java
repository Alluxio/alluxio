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
  public String getDataFolder() throws TException {
    return mWorkerStorage.getDataFolder();
  }

  @Override
  public String getUserTempFolder(long userId) throws TException {
    try {
      return mWorkerStorage.getUserLocalTempFolder(userId);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public String getUserUfsTempFolder(long userId) throws TException {
    try {
      return mWorkerStorage.getUserUfsTempFolder(userId);
    } catch (IOException e) {
      throw new TException(e);
    }
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
