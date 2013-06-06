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
  public void accessFile(int fileId) throws TException {
    mWorkerStorage.accessFile(fileId);
  }

  @Override
  public void addCheckpoint(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, 
      FailedToCheckpointException, TException {
    mWorkerStorage.addCheckpoint(userId, fileId);
  }

  @Override
  public void cacheFile(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    mWorkerStorage.cacheFile(userId, fileId);
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
  public void lockFile(int fileId, long userId) throws TException {
    mWorkerStorage.lockFile(fileId, userId);
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
  public void unlockFile(int fileId, long userId) throws TException {
    mWorkerStorage.unlockFile(fileId, userId);
  }

  @Override
  public void userHeartbeat(long userId) throws TException {
    mWorkerStorage.userHeartbeat(userId);
  }
}