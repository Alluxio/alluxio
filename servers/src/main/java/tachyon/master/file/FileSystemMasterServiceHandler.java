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

package tachyon.master.file;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import tachyon.TachyonURI;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.NotFoundException;
import tachyon.master.MasterContext;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;

public final class FileSystemMasterServiceHandler implements FileSystemMasterService.Iface {
  private final FileSystemMaster mFileSystemMaster;

  public FileSystemMasterServiceHandler(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public Set<Long> workerGetPinIdList() {
    return mFileSystemMaster.getPinIdList();
  }

  @Override
  public List<Integer> workerGetPriorityDependencyList() {
    return mFileSystemMaster.getPriorityDependencyList();
  }

  @Override
  public boolean addCheckpoint(long workerId, long fileId, long length, String checkpointPath)
      throws BlockInfoException, FileDoesNotExistException, SuspectedFileSizeException {
    return mFileSystemMaster.completeFileCheckpoint(workerId, fileId, length, new TachyonURI(
        checkpointPath));
  }

  @Override
  public long getFileId(String path) throws InvalidPathException {
    return mFileSystemMaster.getFileId(new TachyonURI(path));
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException, InvalidPathException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  @Override
  public List<FileInfo> getFileInfoList(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getFileInfoList(fileId);
  }

  @Override
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws FileDoesNotExistException, BlockInfoException {
    return mFileSystemMaster.getFileBlockInfo(fileId, fileBlockIndex);
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getFileBlockInfoList(fileId);
  }

  @Override
  public long getNewBlockIdForFile(long fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getNewBlockIdForFile(fileId);
  }

  @Override
  public String getUfsAddress() {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public long createFile(String path, long blockSizeBytes, boolean recursive)
      throws FileAlreadyExistException, BlockInfoException, InvalidPathException {
    return mFileSystemMaster.createFile(new TachyonURI(path), blockSizeBytes, recursive);
  }

  @Override
  public boolean completeFileCheckpoint(long workerId, long fileId, long length,
      String checkpointPath) throws FileDoesNotExistException, SuspectedFileSizeException,
      BlockInfoException {
    return mFileSystemMaster.completeFileCheckpoint(workerId, fileId, length, new TachyonURI(
        checkpointPath));
  }

  @Override
  public void completeFile(long fileId) throws FileDoesNotExistException, BlockInfoException {
    mFileSystemMaster.completeFile(fileId);
  }

  @Override
  public boolean deleteFile(long fileId, boolean recursive) throws TachyonException,
      FileDoesNotExistException {
    return mFileSystemMaster.deleteFile(fileId, recursive);
  }

  @Override
  public boolean renameFile(long fileId, String dstPath) throws FileAlreadyExistException,
      FileDoesNotExistException, InvalidPathException {
    return mFileSystemMaster.rename(fileId, new TachyonURI(dstPath));
  }

  @Override
  public void setPinned(long fileId, boolean pinned) throws FileDoesNotExistException {
    mFileSystemMaster.setPinned(fileId, pinned);
  }

  @Override
  public boolean createDirectory(String path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException {
    mFileSystemMaster.mkdirs(new TachyonURI(path), recursive);
    return true;
  }

  @Override
  public boolean free(long fileId, boolean recursive) throws FileDoesNotExistException {
    return mFileSystemMaster.free(fileId, recursive);
  }

  @Override
  public int createDependency(List<String> parents, List<String> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      int dependencyType, long childrenBlockSizeByte) throws InvalidPathException,
      FileDoesNotExistException, FileAlreadyExistException, BlockInfoException, TachyonException {
    // TODO(gene): Implement lineage.
    return 0;
  }

  @Override
  public DependencyInfo getDependencyInfo(int dependencyId) throws DependencyDoesNotExistException {
    return mFileSystemMaster.getClientDependencyInfo(dependencyId);
  }

  @Override
  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    mFileSystemMaster.reportLostFile(fileId);
  }

  @Override
  public void requestFilesInDependency(int depId) throws DependencyDoesNotExistException {
    mFileSystemMaster.requestFilesInDependency(depId);
  }

  @Override
  public long loadFileFromUfs(String ufsPath, boolean recursive) throws TachyonException {
    return mFileSystemMaster.loadFileFromUfs(new TachyonURI(ufsPath), recursive);
  }

  @Override
  public void mount(String tachyonPath, String ufsPath) throws TachyonException {
    try {
      mFileSystemMaster.mount(new TachyonURI(tachyonPath), new TachyonURI(ufsPath));
    } catch (AlreadyExistsException aee) {
      throw new TachyonException(aee.getMessage());
    } catch (FileAlreadyExistException faee) {
      throw new TachyonException(faee.getMessage());
    } catch (InvalidPathException ipe) {
      throw new TachyonException(ipe.getMessage());
    }
  }

  @Override
  public void unmount(String tachyonPath) throws TachyonException {
    try {
      mFileSystemMaster.unmount(new TachyonURI(tachyonPath));
    } catch (FileDoesNotExistException fdnee) {
      throw new TachyonException(fdnee.getMessage());
    } catch (InvalidPathException ipe) {
      throw new TachyonException(ipe.getMessage());
    } catch (NotFoundException nfe) {
      throw new TachyonException(nfe.getMessage());
    }
  }
}
