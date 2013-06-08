package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
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
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final MasterInfo mMasterInfo;

  public MasterServiceHandler(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public boolean addCheckpoint(long workerId, int fileId, long fileSizeBytes, String checkpointPath) 
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    return mMasterInfo.addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
  }

  @Override
  public List<ClientFileInfo> liststatus(String path)
      throws InvalidPathException, FileDoesNotExistException, TException {
    return mMasterInfo.getFilesInfo(path);
  }

  @Override
  public List<ClientWorkerInfo> getWorkersInfo() throws TException {
    return mMasterInfo.getWorkersInfo();
  }

  @Override
  public int user_createFile(String filePath)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(filePath, false);
  }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException, TException {
    return mMasterInfo.createRawTable(
        path, columns, CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public boolean user_deleteById(int id, boolean recursive) throws TException {
    return mMasterInfo.delete(id, recursive);
  }

  @Override
  public boolean user_deleteByPath(String path, boolean recursive) throws TException {
    return mMasterInfo.delete(path, recursive);
  }

  @Override
  public NetAddress user_getWorker(boolean random, String host) 
      throws NoLocalWorkerException, TException {
    return mMasterInfo.getWorker(random, host);
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
    List<NetAddress> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(fileId);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public List<NetAddress> user_getFileLocationsByPath(String filePath)
      throws FileDoesNotExistException, InvalidPathException, TException {
    List<NetAddress> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(filePath);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
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
  public String user_getUnderfsAddress() throws TException {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  @Override
  public List<Integer> user_listFiles(String path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.listFiles(path, recursive);
  }

  @Override
  public List<String> user_ls(String path, boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, TException {
    return mMasterInfo.ls(path, recursive);
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
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TException {
    mMasterInfo.updateRawTableMetadata(tableId, 
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
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