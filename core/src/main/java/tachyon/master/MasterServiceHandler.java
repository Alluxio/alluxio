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

package tachyon.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.thrift.AccessControlException;
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
  public boolean addCheckpoint(long workerId, int fileId, long fileSizeBytes, String checkpointPath)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    try {
      return mMasterInfo.addCheckpoint(workerId, fileId, fileSizeBytes, new TachyonURI(
          checkpointPath));
    } catch (FileNotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, String path) throws InvalidPathException,
  AccessControlException, TException {
    if (fileId != -1) {
      return mMasterInfo.getClientFileInfo(fileId);

    }

    return mMasterInfo.getClientFileInfo(new TachyonURI(path));
  }

  @Override
  public List<ClientWorkerInfo> getWorkersInfo() throws TException {
    return mMasterInfo.getWorkersInfo();
  }

  @Override
  public List<ClientFileInfo> liststatus(String path) throws InvalidPathException,
      FileDoesNotExistException, AccessControlException, TException {
    return mMasterInfo.getFilesInfo(new TachyonURI(path));
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
      List<TachyonURI> childrenUris = new ArrayList<TachyonURI>(children.size());
      for (int k = 0; k < children.size(); k ++) {
        mMasterInfo.createFile(new TachyonURI(children.get(k)), childrenBlockSizeByte);
        childrenUris.add(new TachyonURI(children.get(k)));
      }
      List<TachyonURI> parentUris = new ArrayList<TachyonURI>(parents.size());
      for (int k = 0; k < parents.size(); k ++) {
        parentUris.add(new TachyonURI(parents.get(k)));
      }
      return mMasterInfo.createDependency(parentUris, childrenUris, commandPrefix, data, comment,
          framework, frameworkVersion, DependencyType.getDependencyType(dependencyType));
    } catch (IOException e) {
      throw new FileDoesNotExistException(e.getMessage());
    }
  }

  @Override
  public int user_createFile(String path, String ufsPath, long blockSizeByte, boolean recursive)
      throws FileAlreadyExistException, InvalidPathException, BlockInfoException,
      SuspectedFileSizeException, TachyonException, AccessControlException, TException {
    if (!ufsPath.isEmpty()) {
      UnderFileSystem underfs = UnderFileSystem.get(ufsPath, mMasterInfo.getTachyonConf());
      try {
        long ufsBlockSizeByte = underfs.getBlockSizeByte(ufsPath);
        long fileSizeByte = underfs.getFileSize(ufsPath);
        int fileId = mMasterInfo.createFile(new TachyonURI(path), ufsBlockSizeByte, recursive);
        if (fileId != -1
            && mMasterInfo.addCheckpoint(-1, fileId, fileSizeByte, new TachyonURI(ufsPath))) {
          return fileId;
        }
      } catch (IOException e) {
        throw new TachyonException(e.getMessage());
      }
    }

    return mMasterInfo.createFile(new TachyonURI(path), blockSizeByte, recursive);
  }

  @Override
  public long user_createNewBlock(int fileId) throws FileDoesNotExistException, TException {
    return mMasterInfo.createNewBlock(fileId);
  }

  @Override
  public int user_createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException, TException {
    return mMasterInfo.createRawTable(new TachyonURI(path), columns,
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public boolean user_delete(int fileId, String path, boolean recursive) throws TachyonException,
      AccessControlException, TException {
    if (fileId != -1) {
      return mMasterInfo.delete(fileId, recursive);
    }
    return mMasterInfo.delete(new TachyonURI(path), recursive);
  }

  @Override
  public long user_getBlockId(int fileId, int index) throws FileDoesNotExistException, TException {
    return BlockInfo.computeBlockId(fileId, index);
  }

  @Override
  public ClientBlockInfo user_getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      BlockInfoException, TException {
    return mMasterInfo.getClientBlockInfo(blockId);
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

    return mMasterInfo.getClientRawTableInfo(new TachyonURI(path));
  }

  @Override
  public List<ClientBlockInfo> user_getFileBlocks(int fileId, String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    List<ClientBlockInfo> ret = null;
    if (fileId != -1) {
      ret = mMasterInfo.getFileBlocks(fileId);
    } else {
      ret = mMasterInfo.getFileBlocks(new TachyonURI(path));
    }
    return ret;
  }

  @Override
  public int user_getRawTableId(String path) throws InvalidPathException, TException {
    return mMasterInfo.getRawTableId(new TachyonURI(path));
  }

  @Override
  public String user_getUfsAddress() throws TException {
    return mMasterInfo.getTachyonConf().get(Constants.UNDERFS_ADDRESS, "/underfs");
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
  public void user_heartbeat() throws TException {
    return;
  }

  @Override
  public boolean user_mkdirs(String path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException, AccessControlException, TachyonException, TException {
    return mMasterInfo.mkdirs(new TachyonURI(path), recursive);
  }

  @Override
  public boolean user_rename(int fileId, String srcPath, String dstPath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, TException {
    if (fileId != -1) {
      return mMasterInfo.rename(fileId, new TachyonURI(dstPath));
    }

    return mMasterInfo.rename(new TachyonURI(srcPath), new TachyonURI(dstPath));
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
      AccessControlException, TException {
    mMasterInfo.setPinned(fileId, pinned);
  }

  @Override
  public boolean user_freepath(int fileId, String path, boolean recursive) throws TachyonException,
      TException {
    if (fileId != -1) {
      return mMasterInfo.freepath(fileId, recursive);
    }
    return mMasterInfo.freepath(new TachyonURI(path), recursive);
  }

  @Override
  public void user_updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException, TException {
    mMasterInfo.updateRawTableMetadata(tableId,
        CommonUtils.generateNewByteBufferFromThriftRPCResults(metadata));
  }

  @Override
  public void worker_cacheBlock(long workerId, long usedBytesOnTier, long storageDirId,
      long blockId, long length) throws FileDoesNotExistException, BlockInfoException, TException {
    mMasterInfo.cacheBlock(workerId, usedBytesOnTier, storageDirId, blockId, length);
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
  public Command worker_heartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlockIds)
      throws BlockInfoException, TException {
    return mMasterInfo.workerHeartbeat(workerId, usedBytesOnTiers, removedBlockIds, addedBlockIds);
  }

  @Override
  public long worker_register(NetAddress workerNetAddress, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlockIds)
          throws BlockInfoException, TException {
    return mMasterInfo.registerWorker(workerNetAddress, totalBytesOnTiers, usedBytesOnTiers,
        currentBlockIds);
  }

  @Override
  public boolean user_setPermission(int fileId, String path, int permission,
      boolean recursive) throws FileDoesNotExistException, InvalidPathException,
      AccessControlException, TachyonException, TException {
    if (fileId != -1) {
      return mMasterInfo.setPermission(fileId, (short)permission, recursive);
    }
    return mMasterInfo.setPermission(new TachyonURI(path), (short)permission, recursive);
  }

  @Override
  public boolean user_setOwner(int fileId, String path, String username,
      String groupname, boolean recursive) throws FileDoesNotExistException,
      InvalidPathException, AccessControlException, TachyonException, TException {
    if (fileId != -1) {
      return mMasterInfo.setOwner(fileId, username, groupname, recursive);
    }
    return mMasterInfo.setOwner(new TachyonURI(path), username, groupname, recursive);
  }
}
