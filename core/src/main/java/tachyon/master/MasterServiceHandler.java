package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import tachyon.UnderFileSystem;
import tachyon.conf.CommonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.Command;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.MasterService;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;
import tachyon.util.CommonUtils;

/**
 * The Master server program.
 * 
 * It maintains the state of each worker. It never keeps the state of any user.
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final MasterInfo MASTER_INFO;

  public MasterServiceHandler(MasterInfo masterInfo) {
    MASTER_INFO = masterInfo;
  }

  @Override
  public boolean
      addCheckpoint(long workerId, int fileId, long fileSizeBytes, String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException,
          TException {
    try {
      return MASTER_INFO.addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
    } catch (FileNotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, String path) throws FileDoesNotExistException,
      InvalidPathException, TException {
    if (fileId != -1) {
      return MASTER_INFO.getClientFileInfo(fileId);

    }

    return MASTER_INFO.getClientFileInfo(path);
  }

  @Override
  public List<ClientWorkerInfo> getWorkersInfo() throws TException {
    return MASTER_INFO.getWorkersInfo();
  }

  @Override
  public List<ClientFileInfo> liststatus(String path) throws InvalidPathException,
      FileDoesNotExistException, TException {
    return MASTER_INFO.getFilesInfo(path);
  }

  @Override
  public void user_completeFile(int fileId) throws FileDoesNotExistException, TException {
    MASTER_INFO.completeFile(fileId);
  }

  @Override
  public int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException,
      BlockInfoException, TachyonException, TException {
    try {
      for (int k = 0; k < children.size(); k ++) {
        MASTER_INFO.createFile(children.get(k), childrenBlockSizeByte);
      }
      return MASTER_INFO.createDependency(parents, children, commandPrefix, data, comment,
          framework, frameworkVersion, DependencyType.getDependencyType(dependencyType));
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
  }

  @Override
  public int user_createFile(String path, String ufsPath, long blockSizeByte, boolean recursive)
      throws FileAlreadyExistException, InvalidPathException, BlockInfoException,
      SuspectedFileSizeException, TachyonException, TException {
    if (!ufsPath.isEmpty()) {
      UnderFileSystem underfs = UnderFileSystem.get(ufsPath);
      try {
        long ufsBlockSizeByte = underfs.getBlockSizeByte(ufsPath);
        long fileSizeByte = underfs.getFileSize(ufsPath);
        int fileId = MASTER_INFO.createFile(path, ufsBlockSizeByte, recursive);
        if (fileId != -1 && MASTER_INFO.addCheckpoint(-1, fileId, fileSizeByte, ufsPath)) {
          return fileId;
        }
      } catch (IOException e) {
        throw new TachyonException(e.getMessage());
      }
    }

    return MASTER_INFO.createFile(path, blockSizeByte, recursive);
  }

  @Override
  public long user_createNewBlock(int fileId) throws FileDoesNotExistException, TException {
    return MASTER_INFO.createNewBlock(fileId);
  }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException, TException {
    return MASTER_INFO.createRawTable(path, columns,
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public boolean user_delete(int fileId, String path, boolean recursive) throws TachyonException,
      TException {
    if (fileId != -1) {
      return MASTER_INFO.delete(fileId, recursive);
    }
    return MASTER_INFO.delete(path, recursive);
  }

  @Override
  public long user_getBlockId(int fileId, int index) throws FileDoesNotExistException, TException {
    return BlockInfo.computeBlockId(fileId, index);
  }

  @Override
  public ClientBlockInfo user_getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      BlockInfoException, TException {
    ClientBlockInfo ret = null;
    try {
      ret = MASTER_INFO.getClientBlockInfo(blockId);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public ClientDependencyInfo user_getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException, TException {
    return MASTER_INFO.getClientDependencyInfo(dependencyId);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoById(int id)
      throws TableDoesNotExistException, TException {
    return MASTER_INFO.getClientRawTableInfo(id);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfoByPath(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    return MASTER_INFO.getClientRawTableInfo(path);
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocksById(int fileId)
      throws FileDoesNotExistException, TException {
    List<ClientBlockInfo> ret = null;
    try {
      ret = MASTER_INFO.getFileLocations(fileId);
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
      ret = MASTER_INFO.getFileLocations(path);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return MASTER_INFO.getRawTableId(path);
  }

  @Override
  public String user_getUfsAddress() throws TException {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  @Override
  public long user_getUserId() throws TException {
    return MASTER_INFO.getNewUserId();
  }

  @Override
  public NetAddress user_getWorker(boolean random, String host) throws NoWorkerException,
      TException {
    NetAddress ret = null;
    try {
      ret = MASTER_INFO.getWorker(random, host);
    } catch (UnknownHostException e) {
      throw new NoWorkerException(e.getMessage());
    }
    if (ret == null) {
      if (random) {
        throw new NoWorkerException("No worker in the system");
      } else {
        throw new NoWorkerException("No local worker on " + host);
      }
    }
    return ret;
  }

  @Override
  public boolean user_mkdirs(String path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException, TachyonException, TException {
    return MASTER_INFO.mkdirs(path, recursive);
  }

  @Override
  public boolean user_rename(int fileId, String srcPath, String dstPath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException,
      TException {
    if (fileId != -1) {
      return MASTER_INFO.rename(fileId, dstPath);
    }

    return MASTER_INFO.rename(srcPath, dstPath);
  }

  @Override
  public void user_reportLostFile(int fileId) throws FileDoesNotExistException, TException {
    MASTER_INFO.reportLostFile(fileId);
  }

  @Override
  public void user_requestFilesInDependency(int depId) throws DependencyDoesNotExistException,
      TException {
    MASTER_INFO.requestFilesInDependency(depId);
  }

  @Override
  public void user_setPinned(int fileId, boolean pinned) throws FileDoesNotExistException,
      TException {
    MASTER_INFO.setPinned(fileId, pinned);
  }

  @Override
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException, TException {
    MASTER_INFO.updateRawTableMetadata(tableId,
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public void worker_cacheBlock(long workerId, long workerUsedBytes, long blockId, long length)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    MASTER_INFO.cacheBlock(workerId, workerUsedBytes, blockId, length);
  }

  @Override
  public Set<Integer> worker_getPinIdList() throws TException {
    List<Integer> ret = MASTER_INFO.getPinIdList();
    return new HashSet<Integer>(ret);
  }

  @Override
  public List<Integer> worker_getPriorityDependencyList() throws TException {
    return MASTER_INFO.getPriorityDependencyList();
  }

  @Override
  public Command worker_heartbeat(long workerId, long usedBytes, List<Long> removedBlockIds)
      throws BlockInfoException, TException {
    return MASTER_INFO.workerHeartbeat(workerId, usedBytes, removedBlockIds);
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Long> currentBlockIds) throws BlockInfoException, TException {
    return MASTER_INFO.registerWorker(workerNetAddress, totalBytes, usedBytes, currentBlockIds);
  }
}