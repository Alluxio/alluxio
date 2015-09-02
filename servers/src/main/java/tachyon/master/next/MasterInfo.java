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

package tachyon.master.next;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.next.block.BlockMaster;
import tachyon.master.next.filesystem.FileSystemMaster;
import tachyon.master.next.filesystem.meta.InodeFile;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.WorkerInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.underfs.UnderFileSystem.SpaceType;

/**
 * A wrapper class of BlockMaster, FileSystemMaster and other master modules to provide a view of
 * their containing information.
 */
public final class MasterInfo {
  private final BlockMaster mBlockMaster;
  private final FileSystemMaster mFileSystemMaster;
  private final InetSocketAddress mMasterAddress;
  private final long mStartTimeMs;
  private final TachyonConf mTachyonConf;
  private final String mUFSDataFolder;

  public MasterInfo(BlockMaster blockMaster, FileSystemMaster fileSystemMaster,
      InetSocketAddress address, TachyonConf tachyonConf) {
    mBlockMaster = Preconditions.checkNotNull(blockMaster);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mMasterAddress = Preconditions.checkNotNull(address);
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mUFSDataFolder = mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    mStartTimeMs = System.currentTimeMillis();
  }

  /**
   * Gets the capacity of the whole system.
   *
   * @return the system's capacity in bytes.
   */
  public long getCapacityBytes() {
    return mBlockMaster.getCapacityBytes();
  }

  /**
   * Gets the list of block info of an InodeFile determined by path.
   *
   * @param path path to the file
   * @return The list of the block info of the file
   * @throws InvalidPathException when the path is invalid
   * @throws FileDoesNotExistException when the file does not exist
   */
  public List<BlockInfo> getBlockInfoList(TachyonURI path)
      throws InvalidPathException, FileDoesNotExistException {
    return mFileSystemMaster.getBlockInfoList(path);
  }

  /**
   * Gets the list of blocks of an {@link InodeFile} determined by the file id.
   *
   * @param fileId id of the file
   * @return The list of the blocks of the file
   * @throws InvalidPathException when the path is invalid
   * @throws FileDoesNotExistException when the file does not exist
   */
  public List<FileBlockInfo> getFileBlockList(long fileId)
      throws InvalidPathException, FileDoesNotExistException {
    return mFileSystemMaster.getFileBlockInfoList(fileId);
  }

  /**
   * Gets the dependency info associated with the given id.
   *
   * @param dependencyId id of the dependency
   * @return the dependency info
   * @throws DependencyDoesNotExistException when the dependency does not exist
   */
  public DependencyInfo getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException {
    return mFileSystemMaster.getClientDependencyInfo(dependencyId);
  }

  /**
   * Gets the file info for the file at the given path
   *
   * @param path path of the file
   * @return the file info
   * @throws InvalidPathException when the path is invalid
   * @throws FileDoesNotExistException when the file does not exist
   */
  public FileInfo getFileInfo(TachyonURI path)
      throws InvalidPathException, FileDoesNotExistException {
    long fileId = mFileSystemMaster.getFileId(path);
    return mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its FileInfo.
   *
   * @param path the target directory/file path
   * @return A list of FileInfo
   * @throws FileDoesNotExistException when the file does not exist
   * @throws InvalidPathException when an invalid path is encountered
   */
  public List<FileInfo> getFileInfoList(TachyonURI path)
      throws FileDoesNotExistException, InvalidPathException {
    long fileId = mFileSystemMaster.getFileId(path);
    return mFileSystemMaster.getFileInfoList(fileId);
  }

  /**
   * Gets absolute paths of all in memory files.
   *
   * @return absolute paths of all in memory files.
   */
  public List<TachyonURI> getInMemoryFiles() {
    return mFileSystemMaster.getInMemoryFiles();
  }

  /**
   * Gets info about the lost workers
   *
   * @return a list of worker info
   */
  public List<WorkerInfo> getLostWorkersInfo() {
    return mBlockMaster.getLostWorkersInfo();
  }

  /**
   * Gets the master address.
   *
   * @return the master address
   */
  public InetSocketAddress getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * Gets the path of a file with the given id
   *
   * @param fileId The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist.
   */
  public TachyonURI getPath(int fileId) throws FileDoesNotExistException {
    return mFileSystemMaster.getPath(fileId);
  }

  /**
   * Gets the master start time in milliseconds.
   *
   * @return the master start time in milliseconds
   */
  public long getStarttimeMs() {
    return mStartTimeMs;
  }

  /**
   * @return the total bytes on each storage tier.
   */
  public List<Long> getTotalBytesOnTiers() {
    return mBlockMaster.getTotalBytesOnTiers();
  }

  /**
   * Gets the capacity of the under file system.
   *
   * @return the capacity in bytes
   * @throws IOException when the operation fails
   */
  public long getUnderFsCapacityBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_TOTAL);
  }

  /**
   * Gets the amount of free space in the under file system.
   *
   * @return the free space in bytes
   * @throws IOException when the operation fails
   */
  public long getUnderFsFreeBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_FREE);
  }

  /**
   * Gets the amount of space used in the under file system.
   *
   * @return the space used in bytes
   * @throws IOException when the operation fails
   */
  public long getUnderFsUsedBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_USED);
  }

  /**
   * Gets the amount of space used by the workers.
   *
   * @return the amount of space used in bytes
   */
  public long getUsedBytes() {
    return mBlockMaster.getUsedBytes();
  }

  /**
   * @return the used bytes on each storage tier.
   */
  public List<Long> getUsedBytesOnTiers() {
    return mBlockMaster.getUsedBytesOnTiers();
  }

  /**
   * Gets the white list.
   *
   * @return the white list
   */
  public List<String> getWhiteList() {
    return mFileSystemMaster.getWhiteList();
  }

  /**
   * Gets the number of workers.
   *
   * @return the number of workers
   */
  public int getWorkerCount() {
    return mBlockMaster.getWorkerCount();
  }

  /**
   * Gets info about all the workers.
   *
   * @return a list of worker infos
   */
  public List<WorkerInfo> getWorkerInfoList() {
    return mBlockMaster.getWorkerInfoList();
  }
}
