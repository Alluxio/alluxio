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
  private final MasterInfo mMasterInfo;

  public MasterServiceHandler(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  @Override
  public boolean
      addCheckpoint(long workerId, int fileId, long fileSizeBytes, String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException,
          TException {
    try {
      return mMasterInfo.addCheckpoint(workerId, fileId, fileSizeBytes, checkpointPath);
    } catch (FileNotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, String path) throws InvalidPathException,
      TException {
    if (fileId != -1) {
      return mMasterInfo.getClientFileInfo(fileId);

    }

    return mMasterInfo.getClientFileInfo(path);
  }

  @Override
  public List<ClientWorkerInfo> getWorkersInfo() throws TException {
    return mMasterInfo.getWorkersInfo();
  }

  @Override
  public List<ClientFileInfo> liststatus(String path) throws InvalidPathException,
      FileDoesNotExistException, TException {
    return mMasterInfo.getFilesInfo(path);
  }

  @Override
  public void user_completeFile(int fileId) throws FileDoesNotExistException, TException {
    mMasterInfo.completeFile(fileId);
  }

  @Override
  public int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException,
      BlockInfoException, TachyonException, TException {
    try {
      for (int k = 0; k < children.size(); k ++) {
        mMasterInfo.createFile(children.get(k), childrenBlockSizeByte);
      }
      return mMasterInfo.createDependency(parents, children, commandPrefix, data, comment,
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
        int fileId = mMasterInfo.createFile(path, ufsBlockSizeByte, recursive);
        if (fileId != -1 && mMasterInfo.addCheckpoint(-1, fileId, fileSizeByte, ufsPath)) {
          return fileId;
        }
      } catch (IOException e) {
        throw new TachyonException(e.getMessage());
      }
    }

    return mMasterInfo.createFile(path, blockSizeByte, recursive);
  }

  @Override
  public long user_createNewBlock(int fileId) throws FileDoesNotExistException, TException {
    return mMasterInfo.createNewBlock(fileId);
  }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException, TException {
    return mMasterInfo.createRawTable(path, columns,
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public boolean user_delete(int fileId, String path, boolean recursive) throws TachyonException,
      TException {
    if (fileId != -1) {
      return mMasterInfo.delete(fileId, recursive);
    }
    return mMasterInfo.delete(path, recursive);
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
      ret = mMasterInfo.getClientBlockInfo(blockId);
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public ClientDependencyInfo user_getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException, TException {
    return mMasterInfo.getClientDependencyInfo(dependencyId);
  }

  @Override
  public ClientRawTableInfo user_getClientRawTableInfo(int id, String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    if (id != -1) {
      return mMasterInfo.getClientRawTableInfo(id);
    }

    return mMasterInfo.getClientRawTableInfo(path);
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocks(int fileId, String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    List<ClientBlockInfo> ret = null;
    try {
      if (fileId != -1) {
        ret = mMasterInfo.getFileBlocks(fileId);
      } else {
        ret = mMasterInfo.getFileBlocks(path);
      }
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
    return ret;
  }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return mMasterInfo.getRawTableId(path);
  }

  @Override
  public String user_getUfsAddress() throws TException {
    return CommonConf.get().UNDERFS_ADDRESS;
  }

  @Override
  public long user_getUserId() throws TException {
    return mMasterInfo.getNewUserId();
  }

  @Override
  public NetAddress user_getWorker(boolean random, String host) throws NoWorkerException,
      TException {
    NetAddress ret = null;
    try {
      ret = mMasterInfo.getWorker(random, host);
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
    return mMasterInfo.mkdirs(path, recursive);
  }

  @Override
  public boolean user_rename(int fileId, String srcPath, String dstPath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException,
      TException {
    if (fileId != -1) {
      return mMasterInfo.rename(fileId, dstPath);
    }

    return mMasterInfo.rename(srcPath, dstPath);
  }

  @Override
  public void user_reportLostFile(int fileId) throws FileDoesNotExistException, TException {
    mMasterInfo.reportLostFile(fileId);
  }

  @Override
  public void user_requestFilesInDependency(int depId) throws DependencyDoesNotExistException,
      TException {
    mMasterInfo.requestFilesInDependency(depId);
  }

  @Override
  public void user_setPinned(int fileId, boolean pinned) throws FileDoesNotExistException,
      TException {
    mMasterInfo.setPinned(fileId, pinned);
  }

  @Override
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException, TException {
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
  public List<Integer> worker_getPriorityDependencyList() throws TException {
    return mMasterInfo.getPriorityDependencyList();
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
}