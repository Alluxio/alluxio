package tachyon;

import org.apache.thrift.TException;

import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.WorkerService;

/**
 * <code>WorkerServiceHandler</code> handles all the RPC call to the worker.
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
  public void addCheckpoint(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, 
      FailedToCheckpointException, TException {
    mWorkerStorage.addCheckpoint(userId, fileId);
  }

  @Override
  public void cacheBlock(long userId, long blockId)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
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
  public void returnSpace(long userId, long returnedBytes) throws TException {
    mWorkerStorage.returnSpace(userId, returnedBytes);
  }

  @Override
  public boolean requestSpace(long userId, long requestBytes) throws TException {
    return mWorkerStorage.requestSpace(userId, requestBytes);
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