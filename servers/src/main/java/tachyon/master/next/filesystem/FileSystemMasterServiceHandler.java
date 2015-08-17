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

package tachyon.master.next.filesystem;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import tachyon.TachyonURI;
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

public class FileSystemMasterServiceHandler implements FileSystemMasterService.Iface {
  private final FileSystemMaster mFileSystemMaster;

  public FileSystemMasterServiceHandler(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public Set<Long> workerGetPinIdList() throws TException {
    return mFileSystemMaster.getPinIdList();
  }

  @Override
  public List<Integer> workerGetPriorityDependencyList() throws TException {
    return mFileSystemMaster.getPriorityDependencyList();
  }

  @Override
  public long getFileId(String path) throws InvalidPathException, TException {
    return mFileSystemMaster.getFileId(new TachyonURI(path));
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException, TException {
    return mFileSystemMaster.getFileInfo(fileId);
  }

  @Override
  public List<FileInfo> getFileInfoList(long fileId) throws FileDoesNotExistException, TException {
    return mFileSystemMaster.getFileInfoList(fileId);
  }

  @Override
  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws FileDoesNotExistException, BlockInfoException, TException {
    return mFileSystemMaster.getFileBlockInfo(fileId, fileBlockIndex);
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(long fileId)
      throws FileDoesNotExistException, TException {
    return mFileSystemMaster.getFileBlockInfoList(fileId);
  }

  @Override
  public long getUserId() throws TException {
    // TODO
    return 0;
  }

  @Override
  public long getNewBlockIdForFile(long fileId)
      throws FileDoesNotExistException, BlockInfoException, TException {
    return mFileSystemMaster.getNewBlockIdForFile(fileId);
  }

  @Override
  public String getUfsAddress() throws TException {
    return mFileSystemMaster.getUfsAddress();
  }

  @Override
  public long createFile(long fileId, long blockSizeBytes, boolean recursive)
      throws FileAlreadyExistException, BlockInfoException, SuspectedFileSizeException,
      TachyonException, TException {
    // TODO: should this take a string path?
    return 0;
  }

  @Override
  public long loadFileFromUfs(long fileId, String ufsPath, boolean recursive)
      throws FileAlreadyExistException, BlockInfoException, SuspectedFileSizeException,
      TachyonException, TException {
    // TODO: should this take a string path?
    return 0;
  }

  @Override
  public void completeFile(long fileId)
      throws FileDoesNotExistException, BlockInfoException, TException {
    mFileSystemMaster.completeFile(fileId);
  }

  @Override
  public boolean deleteFile(long fileId, boolean recursive) throws TachyonException, TException {
    return mFileSystemMaster.deleteFile(fileId, recursive);
  }

  @Override
  public boolean renameFile(long fileId, String dstPath) throws FileAlreadyExistException,
      FileDoesNotExistException, InvalidPathException, TException {
    return mFileSystemMaster.rename(fileId, new TachyonURI(dstPath));
  }

  @Override
  public void setPinned(long fileId, boolean pinned) throws FileDoesNotExistException, TException {
    mFileSystemMaster.setPinned(fileId, pinned);
  }

  @Override
  public boolean createDirectory(long fileId, boolean recursive)
      throws FileAlreadyExistException, TachyonException, TException {
    // TODO
    return false;
  }

  @Override
  public boolean freePath(long fileId, boolean recursive)
      throws FileDoesNotExistException, TException {
    // TODO
    return false;
  }

  @Override
  public boolean addCheckpoint(long workerId, long fileId, long length, String checkpointPath)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException, TException {
    // TODO
    return false;
  }

  @Override
  public void userHeartbeat() throws TException {
    // TODO
  }

  @Override
  public int createDependency(List<String> parents, List<String> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      int dependencyType, long childrenBlockSizeByte)
          throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistException,
          BlockInfoException, TachyonException, TException {
    // TODO
    return 0;
  }

  @Override
  public DependencyInfo getDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException, TException {
    // TODO
    return null;
  }

  @Override
  public void reportLostFile(long fileId) throws FileDoesNotExistException, TException {
    // TODO
  }

  @Override
  public void requestFilesInDependency(int depId)
      throws DependencyDoesNotExistException, TException {
    // TODO
  }

}
