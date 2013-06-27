package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
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
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
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
  public void user_completeFile(int fileId) throws FileDoesNotExistException,
  TException {
    mMasterInfo.completeFile(fileId);
  }

  @Override
  public int user_createFile(String path, long blockSizeByte)
      throws FileAlreadyExistException, InvalidPathException, TException {
    return mMasterInfo.createFile(path, blockSizeByte);
  }

  @Override
  public int user_createFileOnCheckpoint(String path, String checkpointPath)
      throws FileAlreadyExistException, InvalidPathException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long user_createNewBlock(int fileId) throws FileDoesNotExistException, TException {
    return mMasterInfo.createNewBlock(fileId);
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
  public ClientBlockInfo user_getClientBlockInfo(long blockId)
      throws FileDoesNotExistException, BlockInfoException, TException {
    ClientBlockInfo ret = null;
    try {
      ret = mMasterInfo.getClientBlockInfo(blockId);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocksById(int fileId)
      throws FileDoesNotExistException, TException {
    List<ClientBlockInfo> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(fileId);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocksByPath(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    List<ClientBlockInfo> ret = null;
    try {
      ret = mMasterInfo.getFileLocations(path);
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
    return mMasterInfo.mkdir(path);
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
  public void worker_cacheBlock(long workerId, long workerUsedBytes, long blockId, long length)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    mMasterInfo.cacheBlock(workerId, workerUsedBytes, blockId, length);
  }

  @Override
  public Set<Integer> worker_getPinIdList() throws TException {
    List<Integer> ret = mMasterInfo.getPinIdList();
    return new HashSet<Integer>(ret);
  }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Long> removedBlockIds)
      throws BlockInfoException, TException {
    return mMasterInfo.workerHeartbeat(workerId, usedBytes, removedBlockIds);
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Long> currentBlockIds) throws BlockInfoException, TException {
    return mMasterInfo.registerWorker(workerNetAddress, totalBytes, usedBytes, currentBlockIds);
  }

  @Override
  public long user_getBlockId(int fileId, int index)
      throws FileDoesNotExistException, TException {
    return BlockInfo.computeBlockId(fileId, index);
  }
}