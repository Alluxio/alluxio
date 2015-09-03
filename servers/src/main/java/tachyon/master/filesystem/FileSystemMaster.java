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

package tachyon.master.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.StorageLevelAlias;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterBase;
import tachyon.master.block.BlockId;
import tachyon.master.block.BlockMaster;
import tachyon.master.block.ContainerIdGenerator;
import tachyon.master.filesystem.journal.AddCheckpointEntry;
import tachyon.master.filesystem.journal.CompleteFileEntry;
import tachyon.master.filesystem.journal.DeleteFileEntry;
import tachyon.master.filesystem.journal.DependencyEntry;
import tachyon.master.filesystem.journal.FreeEntry;
import tachyon.master.filesystem.journal.InodeDirectoryIdGeneratorEntry;
import tachyon.master.filesystem.journal.InodeEntry;
import tachyon.master.filesystem.journal.RenameEntry;
import tachyon.master.filesystem.journal.SetPinnedEntry;
import tachyon.master.filesystem.meta.Dependency;
import tachyon.master.filesystem.meta.DependencyMap;
import tachyon.master.filesystem.meta.Inode;
import tachyon.master.filesystem.meta.InodeDirectory;
import tachyon.master.filesystem.meta.InodeDirectoryIdGenerator;
import tachyon.master.filesystem.meta.InodeFile;
import tachyon.master.filesystem.meta.InodeTree;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.FormatUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.io.PathUtils;

public final class FileSystemMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;
  private final BlockMaster mBlockMaster;

  // This state must be journaled.
  private final InodeTree mInodeTree;
  // This state must be journaled.
  private final DependencyMap mDependencyMap = new DependencyMap();
  // This state must be journaled.
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  private final PrefixList mWhitelist;

  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.FILE_SYSTEM_MASTER_SERVICE_NAME);
  }

  public FileSystemMaster(TachyonConf tachyonConf, BlockMaster blockMaster,
      Journal journal) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("file-system-master-%d", true)));
    mTachyonConf = tachyonConf;
    mBlockMaster = blockMaster;

    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator);

    // TODO: handle default config value for whitelist.
    mWhitelist = new PrefixList(mTachyonConf.getList(Constants.MASTER_WHITELIST, ","));
  }

  @Override
  public TProcessor getProcessor() {
    return new FileSystemMasterService.Processor<FileSystemMasterServiceHandler>(
        new FileSystemMasterServiceHandler(this));
  }

  @Override
  public String getProcessorName() {
    return Constants.FILE_SYSTEM_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO
    if (entry instanceof InodeEntry) {
      mInodeTree.addInodeFromJournal((InodeEntry) entry);
    } else if (entry instanceof DependencyEntry) {
      DependencyEntry dependencyEntry = (DependencyEntry) entry;
      Dependency dependency = new Dependency(dependencyEntry.mId, dependencyEntry.mParentFiles,
          dependencyEntry.mChildrenFiles, dependencyEntry.mCommandPrefix, dependencyEntry.mData,
          dependencyEntry.mComment, dependencyEntry.mFramework, dependencyEntry.mFrameworkVersion,
          dependencyEntry.mDependencyType, dependencyEntry.mParentDependencies,
          dependencyEntry.mCreationTimeMs, mTachyonConf);
      for (int childDependencyId : dependencyEntry.mChildrenDependencies) {
        dependency.addChildrenDependency(childDependencyId);
      }
      for (long lostFileId : dependencyEntry.mLostFileIds) {
        dependency.addLostFile(lostFileId);
      }
      dependency.resetUncheckpointedChildrenFiles(dependencyEntry.mUncheckpointedFiles);
      mDependencyMap.addDependency(dependency);
    } else if (entry instanceof CompleteFileEntry) {
      completeFileFromEntry((CompleteFileEntry) entry);
    } else if (entry instanceof AddCheckpointEntry) {
      completeFileCheckpointFromEntry((AddCheckpointEntry) entry);
    } else if (entry instanceof FreeEntry) {
      freeFromEntry((FreeEntry) entry);
    } else if (entry instanceof SetPinnedEntry) {
      setPinnedFromEntry((SetPinnedEntry) entry);
    } else if (entry instanceof DeleteFileEntry) {
      deleteFileFromEntry((DeleteFileEntry) entry);
    } else if (entry instanceof RenameEntry) {
      renameFromEntry((RenameEntry) entry);
    } else if (entry instanceof InodeDirectoryIdGeneratorEntry) {
      mDirectoryIdGenerator.fromJournalEntry((InodeDirectoryIdGeneratorEntry) entry);
    } else {
      throw new IOException("unexpected entry in journal: " + entry);
    }
  }

  @Override
  public void writeToJournal(JournalOutputStream outputStream) throws IOException {
    mInodeTree.writeToJournal(outputStream);
    mDependencyMap.writeToJournal(outputStream);
    mDirectoryIdGenerator.writeToJournal(outputStream);
  }

  @Override
  public void start(boolean asMaster) throws IOException {
    mInodeTree.initializeRoot();
    startMaster(asMaster);
    if (isMasterMode()) {
      // TODO: start periodic heartbeat threads.
    }
  }

  @Override
  public void stop() throws IOException {
    stopMaster();
    if (isMasterMode()) {
      // TODO: stop heartbeat threads.
    }
  }

  public boolean completeFileCheckpoint(long workerId, long fileId, long length,
      TachyonURI checkpointPath)
          throws SuspectedFileSizeException, BlockInfoException, FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      LOG.info(FormatUtils.parametersToString(workerId, fileId, length, checkpointPath));
      if (completeFileCheckpointInternal(workerId, fileId, length, checkpointPath, opTimeMs)) {
        writeJournalEntry(
            new AddCheckpointEntry(workerId, fileId, length, checkpointPath, opTimeMs));
        flushJournal();
      }
    }
    return true;
  }

  /**
   * @return whether the operation needs to be written to journal
   */
  // TODO(cc) Make it protected instead of public for use in tests,
  // after splitting up MasterInfoIntegrationTest
  public boolean completeFileCheckpointInternal(long workerId, long fileId,
      long length, TachyonURI checkpointPath, long opTimeMs)
          throws SuspectedFileSizeException, BlockInfoException, FileDoesNotExistException {
    if (workerId != -1) {
      // TODO: how to update worker timestamp?
      // workerInfo.updateLastUpdatedTimeMs();
    }

    Inode inode = mInodeTree.getInodeById(fileId);
    if (inode.isDirectory()) {
      throw new FileDoesNotExistException("File id " + fileId + " is a directory, not a file.");
    }

    InodeFile tFile = (InodeFile) inode;
    boolean needLog = false;

    if (tFile.isComplete()) {
      if (tFile.getLength() != length) {
        throw new SuspectedFileSizeException(
            fileId + ". Original Size: " + tFile.getLength() + ". New Size: " + length);
      }
    } else {
      tFile.setLength(length);
      // Commit all the file blocks (without locations) so the metadata for the block exists.
      long currLength = length;
      for (long blockId : tFile.getBlockIds()) {
        long blockSize = Math.min(currLength, tFile.getBlockSizeBytes());
        mBlockMaster.commitBlock(blockId, blockSize);
        currLength -= blockSize;
      }

      needLog = true;
    }

    if (!tFile.hasCheckpointed()) {
      tFile.setUfsPath(checkpointPath.toString());
      needLog = true;

      synchronized (mDependencyMap) {
        Dependency dep = mDependencyMap.getFromFileId(fileId);
        if (dep != null) {
          dep.childCheckpointed(fileId);
          if (dep.hasCheckpointed()) {
            mDependencyMap.removeUncheckpointedDependency(dep);
            mDependencyMap.removePriorityDependency(dep);
          }
        }
      }
    }
    mDependencyMap.addFileCheckpoint(fileId);
    tFile.setLastModificationTimeMs(opTimeMs);
    tFile.setComplete(length);
    // TODO: This probably should always be true since the last mod time is updated
    return needLog;
  }

  private void completeFileCheckpointFromEntry(AddCheckpointEntry entry) {
    try {
      completeFileCheckpointInternal(entry.getWorkerId(), entry.getFileId(), entry.getFileLength(),
          entry.getCheckpointPath(), entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    } catch (SuspectedFileSizeException sfse) {
      throw new RuntimeException(sfse);
    } catch (BlockInfoException bie) {
      throw new RuntimeException(bie);
    }
  }

  public TachyonConf getTachyonConf() {
    return mTachyonConf;
  }

  /**
   * Whether the filesystem contains a directory with the id.
   *
   * @param id id of the directory
   * @return true if there is such a directory, otherwise false
   */
  public boolean isDirectory(long id) {
    synchronized (mInodeTree) {
      Inode inode;
      try {
        inode = mInodeTree.getInodeById(id);
      } catch (FileDoesNotExistException fne) {
        return false;
      }
      return inode.isDirectory();
    }
  }

  /**
   * Gets the list of block info of an InodeFile determined by path.
   *
   * TODO: get rid of this after FileBlockInfo contains MasterBlockInfo
   *
   * @param path path to the file
   * @return The list of the block info of the file
   * @throws InvalidPathException when the path is invalid
   * @throws FileDoesNotExistException when the file does not exist
   */
  public List<BlockInfo> getBlockInfoList(TachyonURI path)
      throws InvalidPathException, FileDoesNotExistException {
    long fileId = getFileId(path);
    Inode inode = mInodeTree.getInodeById(fileId);
    if (inode == null) {
      throw new FileDoesNotExistException(path + " does not exist.");
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(path + " is not a file.");
    }
    InodeFile inodeFile = (InodeFile) inode;
    return mBlockMaster.getBlockInfoList(inodeFile.getBlockIds());
  }

  public long getFileId(TachyonURI path) throws InvalidPathException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      return inode.getId();
    }
  }

  public FileInfo getFileInfo(long fileId) throws FileDoesNotExistException, InvalidPathException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      return getFileInfo(inode);
    }
  }

  private FileInfo getFileInfo(Inode inode) throws FileDoesNotExistException, InvalidPathException {
    FileInfo fileInfo = inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    fileInfo.inMemoryPercentage = getInMemoryPercentage(inode);
    return fileInfo;
  }

  public List<FileInfo> getFileInfoList(long fileId)
      throws FileDoesNotExistException, InvalidPathException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);

      List<FileInfo> ret = new ArrayList<FileInfo>();
      if (inode.isDirectory()) {
        for (Inode child : ((InodeDirectory) inode).getChildren()) {
          ret.add(getFileInfo(child));
        }
      } else {
        ret.add(getFileInfo(inode));
      }
      return ret;
    }
  }

  public void completeFile(long fileId) throws FileDoesNotExistException, BlockInfoException {
    // TODO: metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      Inode inode = mInodeTree.getInodeById(fileId);
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File id " + fileId + " is not a file.");
      }

      InodeFile fileInode = (InodeFile) inode;
      List<Long> blockIdList = fileInode.getBlockIds();
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
      if (blockInfoList.size() != blockIdList.size()) {
        throw new BlockInfoException("Cannot complete file without all the blocks committed");
      }

      // Verify that all the blocks (except the last one) is the same size as the file block size.
      long fileLength = 0;
      long fileBlockSize = fileInode.getBlockSizeBytes();
      for (int i = 0; i < blockInfoList.size(); i ++) {
        BlockInfo blockInfo = blockInfoList.get(i);
        fileLength += blockInfo.getLength();
        if (i < blockInfoList.size() - 1 && blockInfo.getLength() != fileBlockSize) {
          throw new BlockInfoException(
              "Block index " + i + " has a block size smaller than the file block size ("
                  + fileInode.getBlockSizeBytes() + ")");
        }
      }

      completeFileInternal(fileInode.getBlockIds(), fileId, fileLength, opTimeMs);
      writeJournalEntry(
          new CompleteFileEntry(fileInode.getBlockIds(), fileId, fileLength, opTimeMs));
      flushJournal();
    }
  }

  // TODO(cc) Make it protected instead of public for use in tests,
  // after splitting up MasterInfoIntegrationTest
  public void completeFileInternal(List<Long> blockIds, long fileId, long fileLength, long opTimeMs)
      throws FileDoesNotExistException {
    mDependencyMap.addFileCheckpoint(fileId);
    InodeFile inodeFile = (InodeFile)mInodeTree.getInodeById(fileId);
    inodeFile.setBlockIds(blockIds);
    inodeFile.setComplete(fileLength);
    inodeFile.setLastModificationTimeMs(opTimeMs);
  }

  private void completeFileFromEntry(CompleteFileEntry entry) {
    try {
      completeFileInternal(entry.getBlockIds(), entry.getFileId(), entry.getFileLength(),
          entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }

  public long createFile(TachyonURI path, long blockSizeBytes, boolean recursive)
      throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    // TODO: metrics
    synchronized (mInodeTree) {
      List<Inode> created =
          createFileInternal(path, blockSizeBytes, recursive, System.currentTimeMillis());

      // Writing the first created inode to the journal will also write its children.
      writeJournalEntry(created.get(0));
      writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
      flushJournal();

      return created.get(created.size() - 1).getId();
    }
  }

  // TODO(cc) Make it protected instead of public for use in tests,
  // after splitting up MasterInfoIntegrationTest
  public List<Inode> createFileInternal(TachyonURI path, long blockSizeBytes, boolean recursive,
      long opTimeMs) throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    List<Inode> created = mInodeTree.createPath(path, blockSizeBytes, recursive, false, opTimeMs);
    // If the create succeeded, the list of created inodes will not be empty.
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(path.toString())) {
      inode.setCache(true);
    }
    return created;
  }

  public long getNewBlockIdForFile(long fileId) throws FileDoesNotExistException {
    Inode inode = null;
    synchronized (mInodeTree) {
      inode = mInodeTree.getInodeById(fileId);
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException("File id " + fileId + " is not a file.");
    }

    return ((InodeFile) inode).getNewBlockId();
  }

  public boolean deleteFile(long fileId, boolean recursive)
      throws TachyonException, FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      boolean ret = deleteFileInternal(fileId, recursive, opTimeMs);
      writeJournalEntry(new DeleteFileEntry(fileId, recursive, opTimeMs));
      flushJournal();
      return ret;
    }
  }

  private void deleteFileFromEntry(DeleteFileEntry entry) {
    try {
      deleteFileInternal(entry.mFileId, entry.mRecursive, entry.mOpTimeMs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO(cc) Make it protected instead of public for use in tests,
  // after splitting up MasterInfoIntegrationTest
  public boolean deleteFileInternal(long fileId, boolean recursive, long opTimeMs)
      throws TachyonException, FileDoesNotExistException {
    Inode inode = mInodeTree.getInodeById(fileId);
    return deleteInodeInternal(inode, recursive, System.currentTimeMillis());
  }

  private boolean deleteInodeInternal(Inode inode, boolean recursive, long opTimeMs)
      throws TachyonException, FileDoesNotExistException {
    if (inode == null) {
      return true;
    }
    if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
      // true
      return false;
    }
    if (mInodeTree.isRootId(inode.getId())) {
      // The root cannot be deleted.
      return false;
    }

    List<Inode> delInodes = new ArrayList<Inode>();
    delInodes.add(inode);
    if (inode.isDirectory()) {
      delInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
    }

    // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
    // file, we deal with the checkpoints and blocks as well.
    for (int i = delInodes.size() - 1; i >= 0; i --) {
      Inode delInode = delInodes.get(i);

      if (delInode.isFile()) {
        // Delete the ufs checkpoint.
        String checkpointPath = ((InodeFile) delInode).getUfsPath();
        if (!checkpointPath.equals("")) {
          UnderFileSystem ufs = UnderFileSystem.get(checkpointPath, mTachyonConf);
          try {
            if (!ufs.exists(checkpointPath)) {
              LOG.warn("File does not exist the underfs: " + checkpointPath);
            } else if (!ufs.delete(checkpointPath, true)) {
              return false;
            }
          } catch (IOException e) {
            throw new TachyonException(e.getMessage());
          }
        }

        // Remove corresponding blocks from workers.
        mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds());
      }

      mInodeTree.deleteInode(delInode, opTimeMs);
    }
    return true;
  }

  public FileBlockInfo getFileBlockInfo(long fileId, int fileBlockIndex)
      throws FileDoesNotExistException, BlockInfoException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " is not a file.");
      }
      InodeFile tFile = (InodeFile) inode;
      List<Long> blockIdList = new ArrayList<Long>(1);
      blockIdList.add(tFile.getBlockIdByIndex(fileBlockIndex));
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
      if (blockInfoList.size() != 1) {
        throw new BlockInfoException(
            "FileId " + fileId + " BlockIndex " + fileBlockIndex + " is not a valid block.");
      }
      return generateFileBlockInfo(tFile, blockInfoList.get(0));
    }
  }

  public List<FileBlockInfo> getFileBlockInfoList(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " is not a file.");
      }
      InodeFile tFile = (InodeFile) inode;
      List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(tFile.getBlockIds());

      List<FileBlockInfo> ret = new ArrayList<FileBlockInfo>();
      for (BlockInfo blockInfo : blockInfoList) {
        ret.add(generateFileBlockInfo(tFile, blockInfo));
      }
      return ret;
    }
  }

  /**
   * Returns whether the inodeFile is fully in memory or not. The file is fully in memory only if
   * all the blocks of the file are in memory, in other words, the in memory percentage is 100.
   *
   * @return true if the file is fully in memory, false otherwise
   */
  private boolean isFullyInMemory(InodeFile inode) {
    return getInMemoryPercentage(inode) == 100;
  }

  /**
   * Gets absolute paths of all in memory files.
   *
   * @return absolute paths of all in memory files.
   */
  public List<TachyonURI> getInMemoryFiles() {
    List<TachyonURI> ret = new ArrayList<TachyonURI>();
    LOG.info("getInMemoryFiles()");
    Queue<Pair<InodeDirectory, TachyonURI>> nodesQueue =
        new LinkedList<Pair<InodeDirectory, TachyonURI>>();
    synchronized (mInodeTree) {
      // TODO: Verify we want to use absolute path.
      nodesQueue.add(new Pair<InodeDirectory, TachyonURI>(mInodeTree.getRoot(),
          new TachyonURI(TachyonURI.SEPARATOR)));
      while (!nodesQueue.isEmpty()) {
        Pair<InodeDirectory, TachyonURI> tPair = nodesQueue.poll();
        InodeDirectory tDir = tPair.getFirst();
        TachyonURI curUri = tPair.getSecond();

        Set<Inode> children = tDir.getChildren();
        for (Inode tInode : children) {
          TachyonURI newUri = curUri.join(tInode.getName());
          if (tInode.isDirectory()) {
            nodesQueue.add(new Pair<InodeDirectory, TachyonURI>((InodeDirectory) tInode, newUri));
          } else if (isFullyInMemory((InodeFile) tInode)) {
            ret.add(newUri);
          }
        }
      }
    }
    return ret;
  }

  /**
   * Get the in-memory percentage of an Inode. For a file that has all blocks in memory, it returns
   * 100; for a file that has no block in memory, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in memory percentage
   */
  private int getInMemoryPercentage(Inode inode) {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = (InodeFile) inode;

    long length = inodeFile.getLength();
    if (length == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (BlockInfo info : mBlockMaster.getBlockInfoList(inodeFile.getBlockIds())) {
      if (isInMemory(info)) {
        inMemoryLength += info.getLength();
      }
    }
    return (int) (inMemoryLength * 100 / length);
  }

  /**
   * @return true if the given block is in some worker's memory, false otherwise
   */
  private boolean isInMemory(BlockInfo blockInfo) {
    for (BlockLocation location : blockInfo.getLocations()) {
      if (location.getTier() == StorageLevelAlias.MEM.getValue()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create a directory at path.
   *
   * @param path the path of the directory
   * @param recursive if it is true, create necessary but nonexistent parent directories, otherwise,
   *        the parent directories must already exist
   * @throws InvalidPathException when the path is invalid, please see documentation on
   *         {@link InodeTree#createPath} for more details
   * @throws FileAlreadyExistException when there is already a file at path
   */
  public void mkdirs(TachyonURI path, boolean recursive) throws InvalidPathException,
      FileAlreadyExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      try {
        List<Inode> created = mInodeTree.createPath(path, 0, recursive, true);
        for (Inode inode : created) {
          writeJournalEntry(inode);
        }
        writeJournalEntry(mDirectoryIdGenerator.toJournalEntry());
        flushJournal();
      } catch (BlockInfoException bie) {
        // Since we are creating a directory, the block size is ignored, no such exception should
        // happen.
        Throwables.propagate(bie);
      }
    }
  }

  public boolean rename(long fileId, TachyonURI dstPath)
      throws InvalidPathException, FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode srcInode = mInodeTree.getInodeById(fileId);
      TachyonURI srcPath = mInodeTree.getPath(srcInode);
      if (srcPath.equals(dstPath)) {
        return true;
      }
      if (srcPath.isRoot() || dstPath.isRoot()) {
        return false;
      }
      String[] srcComponents = PathUtils.getPathComponents(srcPath.toString());
      String[] dstComponents = PathUtils.getPathComponents(dstPath.toString());
      // We can't rename a path to one of its subpaths, so we check for that, by making sure
      // srcComponents isn't a prefix of dstComponents.
      if (srcComponents.length < dstComponents.length) {
        boolean isPrefix = true;
        for (int prefixInd = 0; prefixInd < srcComponents.length; prefixInd ++) {
          if (!srcComponents[prefixInd].equals(dstComponents[prefixInd])) {
            isPrefix = false;
            break;
          }
        }
        if (isPrefix) {
          throw new InvalidPathException(
              "Failed to rename: " + srcPath + " is a prefix of " + dstPath);
        }
      }

      TachyonURI dstParentURI = dstPath.getParent();

      // Get the inodes of the src and dst parents.
      Inode srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
      if (!srcParentInode.isDirectory()) {
        return false;
      }
      Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
      if (!dstParentInode.isDirectory()) {
        return false;
      }

      InodeDirectory dstParentDirectory = (InodeDirectory) dstParentInode;

      // Make sure destination path does not exist
      if (dstParentDirectory.getChild(dstComponents[dstComponents.length - 1]) != null) {
        return false;
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      long opTimeMs = System.currentTimeMillis();
      renameInternal(fileId, dstPath, opTimeMs);

      writeJournalEntry(new RenameEntry(fileId, dstPath.getPath(), opTimeMs));
      flushJournal();

      return true;
    }
  }

  // TODO(cc) Make it protected instead of public for use in tests,
  // after splitting up MasterInfoIntegrationTest
  public void renameInternal(long fileId, TachyonURI dstPath, long opTimeMs)
      throws InvalidPathException, FileDoesNotExistException {
    Inode srcInode = mInodeTree.getInodeById(fileId);
    Inode srcParentInode = mInodeTree.getInodeById(srcInode.getParentId());
    TachyonURI dstParentURI = dstPath.getParent();
    Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
    ((InodeDirectory) srcParentInode).removeChild(srcInode);
    srcParentInode.setLastModificationTimeMs(opTimeMs);
    srcInode.setParentId(dstParentInode.getId());
    srcInode.setName(dstPath.getName());
    ((InodeDirectory) dstParentInode).addChild(srcInode);
    dstParentInode.setLastModificationTimeMs(opTimeMs);
  }

  private void renameFromEntry(RenameEntry entry) {
    try {
      renameInternal(entry.mFileId, new TachyonURI(entry.mDstPath), entry.mOpTimeMs);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setPinned(long fileId, boolean pinned) throws FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      setPinnedInternal(fileId, pinned, opTimeMs);
      writeJournalEntry(new SetPinnedEntry(fileId, pinned, opTimeMs));
      flushJournal();
    }
  }

  private void setPinnedInternal(long fileId, boolean pinned, long opTimeMs)
      throws FileDoesNotExistException {
    Inode inode = mInodeTree.getInodeById(fileId);
    mInodeTree.setPinned(inode, pinned, opTimeMs);
  }

  private void setPinnedFromEntry(SetPinnedEntry entry) {
    try {
      setPinnedInternal(entry.getId(), entry.getPinned(), entry.getOperationTimeMs());
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }

  public boolean free(long fileId, boolean recursive)
      throws InvalidPathException, FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);

      if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
        // true
        return false;
      }
      freeInternal(inode);
      writeJournalEntry(new FreeEntry(fileId));
      flushJournal();
    }

    return true;
  }

  private void freeInternal(Inode inode) {
    List<Inode> freeInodes = new ArrayList<Inode>();
    freeInodes.add(inode);
    if (inode.isDirectory()) {
      freeInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
    }

    // We go through each inode.
    for (int i = freeInodes.size() - 1; i >= 0; i--) {
      Inode freeInode = freeInodes.get(i);

      if (freeInode.isFile()) {
        // Remove corresponding blocks from workers.
        mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds());
      }
    }
  }

  private void freeFromEntry(FreeEntry entry) {
    try {
      freeInternal(mInodeTree.getInodeById(entry.getId()));
    } catch (FileDoesNotExistException fdnee) {
      throw new RuntimeException(fdnee);
    }
  }


  /**
   * Gets the path of a file with the given id
   *
   * @param fileId The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist.
   */
  public TachyonURI getPath(long fileId) throws FileDoesNotExistException {
    return mInodeTree.getPath(mInodeTree.getInodeById(fileId));
  }

  public Set<Long> getPinIdList() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinIdSet();
    }
  }

  public String getUfsAddress() {
    return mTachyonConf.get(Constants.UNDERFS_ADDRESS, "/underFSStorage");
  }

  /**
   * Gets the white list.
   *
   * @return the white list
   */
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  public void reportLostFile(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory " + inode);
        return;
      }
      InodeFile iFile = (InodeFile) inode;

      if (mDependencyMap.addLostFile(fileId) == null) {
        LOG.error("There is no dependency info for " + iFile + " . No recovery on that");
      } else {
        LOG.info("Reported file loss. Tachyon will recompute it: " + iFile);
      }
    }
  }

  public void createDependency() {
    // TODO
  }

  public DependencyInfo getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException {
    Dependency dependency = mDependencyMap.getFromDependencyId(dependencyId);
    if (dependency == null) {
      throw new DependencyDoesNotExistException("No dependency with id " + dependencyId);
    }
    return dependency.generateClientDependencyInfo();
  }

  public void requestFilesInDependency(int dependencyId) {
    synchronized (mDependencyMap) {
      Dependency dependency = mDependencyMap.getFromDependencyId(dependencyId);
      if (dependency != null) {
        LOG.info("Request files in dependency " + dependency);
        if (dependency.hasLostFile()) {
          mDependencyMap.recomputeDependency(dependencyId);
        }
      } else {
        LOG.error("There is no dependency with id " + dependencyId);
      }
    }
  }

  public List<Integer> getPriorityDependencyList() {
    synchronized (mDependencyMap) {
      return mDependencyMap.getPriorityDependencyList();
    }
  }

  private FileBlockInfo generateFileBlockInfo(InodeFile file, BlockInfo blockInfo) {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();

    fileBlockInfo.blockInfo = blockInfo;
    fileBlockInfo.underFsLocations = new ArrayList<NetAddress>();

    // The sequence number part of the block id is the block index.
    fileBlockInfo.offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.blockId);

    if (fileBlockInfo.blockInfo.locations.isEmpty() && file.hasCheckpointed()) {
      // No tachyon locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      UnderFileSystem ufs = UnderFileSystem.get(file.getUfsPath(), mTachyonConf);
      List<String> locs = null;
      try {
        locs = ufs.getFileLocations(file.getUfsPath(), fileBlockInfo.offset);
      } catch (IOException e) {
        return fileBlockInfo;
      }
      if (locs != null) {
        for (String loc : locs) {
          String resolvedHost = loc;
          int resolvedPort = -1;
          try {
            String[] ipport = loc.split(":");
            if (ipport.length == 2) {
              resolvedHost = ipport[0];
              resolvedPort = Integer.parseInt(ipport[1]);
            }
          } catch (NumberFormatException nfe) {
            continue;
          }
          // The resolved port is the data transfer port not the rpc port
          fileBlockInfo.underFsLocations.add(new NetAddress(resolvedHost, -1, resolvedPort));
        }
      }
    }
    return fileBlockInfo;
  }
}
