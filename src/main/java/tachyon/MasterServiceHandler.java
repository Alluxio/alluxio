package tachyon;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.Command;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * The Master server program.
 * 
 * It maintains the state of each worker. It never keeps the state of any user.
 * 
 * @author haoyuan
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final Logger LOG = LoggerFactory.getLogger(MasterServiceHandler.class);

  private final MasterInfo mMasterInfo;

  public MasterServiceHandler(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public List<String> cmd_ls(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
    return mMasterInfo.ls(path);
  }

  @Override
  public int user_createFile(String filePath)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(filePath, false);
  }

  @Override
  public int user_createRawTable(String path, int columns, List<Byte> metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException, TException {
    return mMasterInfo.createRawTable(path, columns, metadata);
  }

  @Override
  public void user_deleteById(int id) throws TException {
    mMasterInfo.delete(id);
  }

  @Override
  public void user_deleteByPath(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
    mMasterInfo.delete(path);
  }

  @Override
  public NetAddress user_getLocalWorker(String host) throws NoLocalWorkerException, TException {
    return mMasterInfo.getLocalWorker(host);
  }

  @Override
  public ClientFileInfo user_getClientFileInfoById(int id)
      throws FileDoesNotExistException, TException {
    return mMasterInfo.getClientFileInfo(id);
  }

  @Override
  public ClientFileInfo user_getClientFileInfoByPath(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getClientFileInfo(path);
  }

  @Override
  public List<NetAddress> user_getFileLocationsById(int fileId)
      throws FileDoesNotExistException, TException {
    return mMasterInfo.getFileLocations(fileId);
  }

  @Override
  public List<NetAddress> user_getFileLocationsByPath(String filePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getFileLocations(filePath);
  }

  @Override
  public int user_getFileId(String filePath) throws InvalidPathException, TException {
    return mMasterInfo.getFileId(filePath);
  }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return mMasterInfo.getRawTableId(path);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws TableDoesNotExistException, TException {
    return mMasterInfo.getClientRawTableInfo(id);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getClientRawTableInfo(path);
  }

  @Override
  public long user_getUserId() throws TException {
    return mMasterInfo.getNewUserId();
  }

  @Override
  public int user_getNumberOfFiles(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.getNumberOfFiles(path);
  }

  @Override
  public int user_mkdir(String path) 
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(path, true);
  }

  @Override
  public void user_outOfMemoryForPinFile(int fileId) throws TException {
    LOG.error("The user can not allocate enough space for PIN list File " + fileId);
  }

  @Override
  public void user_renameFile(String srcFilePath, String dstFilePath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException, TException{
    mMasterInfo.renameFile(srcFilePath, dstFilePath);
  }

  @Override
  public void user_unpinFile(int fileId) throws FileDoesNotExistException, TException {
    mMasterInfo.unpinFile(fileId);
  }

  @Override
  public void worker_addCheckpoint(long workerId, int fileId, long fileSizeBytes, 
      String checkpointPath) 
          throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    mMasterInfo.addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
  }

  @Override
  public void worker_cacheFile(long workerId, long workerUsedBytes, int fileId,
      long fileSizeBytes) throws FileDoesNotExistException,
      SuspectedFileSizeException, TException {
    mMasterInfo.cachedFile(workerId, workerUsedBytes, fileId, fileSizeBytes);
  }

  @Override
  public Set<Integer> worker_getPinIdList() throws TException {
    List<Integer> ret = mMasterInfo.getPinIdList();
    return new HashSet<Integer>(ret);
  }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Integer> removedFileIds)
      throws TException {
    return mMasterInfo.workerHeartbeat(workerId, usedBytes, removedFileIds);
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Integer> currentFileIds) throws TException {
    return mMasterInfo.registerWorker(workerNetAddress, totalBytes, usedBytes, currentFileIds);
  }
}