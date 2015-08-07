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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.PrefixList;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.Dependency;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

class FileSystemMaster implements Master {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mTachyonConf;

  private final BlockMaster mBlockMaster;
  private final InodeTree mInodeTree;
  private final DependencyMap mDependencyMap;

  private final PrefixList mWhitelist;

  public FileSystemMaster(TachyonConf tachyonConf, BlockMaster blockMaster) {
    mTachyonConf = tachyonConf;
    mBlockMaster = blockMaster;

    mInodeTree = new InodeTree(mBlockMaster);
    mDependencyMap = new DependencyMap();

    mWhitelist =
        new PrefixList(mTachyonConf.getList(Constants.MASTER_WHITELIST, ",",
            new LinkedList<String>()));
  }

  @Override
  public TProcessor getProcessor() {
    return null;
  }

  @Override
  public String getProcessorName() {
    return "FileSystemMaster";
  }

  public boolean addCheckpoint(long workerId, int fileId, long length, TachyonURI checkpointPath)
      throws FileNotFoundException, SuspectedFileSizeException, BlockInfoException {
    // TODO: metrics
    long opTimeMs = System.currentTimeMillis();
    LOG.info(FormatUtils.parametersToString(workerId, fileId, length, checkpointPath));

    if (workerId != -1) {
      BlockWorkerInfo workerInfo = mBlockMaster.getWorkerInfo(workerId);
      workerInfo.updateLastUpdatedTimeMs();
    }

    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);

      if (inode == null) {
        throw new FileNotFoundException("File id " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileNotFoundException("File id " + fileId + " is a directory, not a file.");
      }

      InodeFile tFile = (InodeFile) inode;
      boolean needLog = false;

      if (tFile.isComplete()) {
        if (tFile.getLength() != length) {
          throw new SuspectedFileSizeException(fileId + ". Original Size: " + tFile.getLength()
              + ". New Size: " + length);
        }
      } else {
        tFile.setLength(length);
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
      tFile.setComplete();

      if (needLog) {
        tFile.setLastModificationTimeMs(opTimeMs);
        // TODO: write to journal.
      }
      return true;
    }

  }

  public ClientFileInfo getFileStatus(long fileId) {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode == null) {
        ClientFileInfo info = new ClientFileInfo();
        info.id = -1;
        return info;
      }
      return inode.generateClientFileInfo(mInodeTree.getPath(inode).toString());
    }

  }

  public ClientFileInfo getFileStatus(TachyonURI path) throws InvalidPathException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      if (inode == null) {
        ClientFileInfo info = new ClientFileInfo();
        info.id = -1;
        return info;
      }
      return inode.generateClientFileInfo(path.toString());
    }
  }

  public List<ClientFileInfo> listStatus(TachyonURI path) throws FileDoesNotExistException,
      InvalidPathException {
    Inode inode = null;
    synchronized (mInodeTree) {
      inode = mInodeTree.getInodeByPath(path);
    }
    if (inode == null) {
      throw new FileDoesNotExistException(path.toString());
    }

    List<ClientFileInfo> ret = new ArrayList<ClientFileInfo>();
    if (inode.isDirectory()) {
      for (Inode child : ((InodeDirectory) inode).getChildren()) {
        ret.add(child.generateClientFileInfo(PathUtils.concatPath(path, child.getName())));
      }
    } else {
      ret.add(inode.generateClientFileInfo(path.toString()));
    }
    return ret;
  }

  public void completeFile(long fileId) throws FileDoesNotExistException {
    // TODO: metrics
    synchronized (mInodeTree) {
      long opTimeMs = System.currentTimeMillis();
      Inode inode = mInodeTree.getInodeById(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File id " + fileId + " does not exist.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File id " + fileId + " is not a file.");
      }

      mDependencyMap.addFileCheckpoint(fileId);
      ((InodeFile) inode).setComplete();
      inode.setLastModificationTimeMs(opTimeMs);
      // TODO: write to journal
    }
  }

  public long createFile(TachyonURI path, long blockSizeBytes, boolean recursive)
      throws InvalidPathException, FileAlreadyExistException, BlockInfoException {
    // TODO: metrics
    synchronized (mInodeTree) {
      InodeFile inode = (InodeFile) mInodeTree.createPath(path, blockSizeBytes, recursive, false);
      if (mWhitelist.inList(path.toString())) {
        inode.setCache(true);
      }
      return inode.getId();

      // TODO: write to journal
    }
  }

  public long getNewBlockIdForFile(long fileId) throws FileDoesNotExistException {
    Inode inode = null;
    synchronized (mInodeTree) {
      inode = mInodeTree.getInodeById(fileId);
    }

    if (inode == null) {
      throw new FileDoesNotExistException("File id " + fileId + " does not exist.");
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException("File id " + fileId + " is not a file.");
    }

    return ((InodeFile) inode).getNewBlockId();
  }

  public void commitFileBlock(long workerId, long usedBytesOnTier, int tierAlias, long blockId,
      long length) throws FileDoesNotExistException, BlockInfoException {
    // TODO: metrics
    synchronized (mInodeTree) {
      mBlockMaster.commitBlock(workerId, usedBytesOnTier, tierAlias, blockId, length);

      long fileId = BlockId.getContainerId(blockId);
      int blockIndex = BlockId.getSequenceNumber(blockId);
      Inode inode = mInodeTree.getInodeById(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a directory.");
      }

      InodeFile fileInode = (InodeFile) inode;

      // Try to commit this block.
      fileInode.commitBlock(new FileBlockInfo(fileInode, blockIndex, length));

      // TODO: write to journal
    }
  }

  public boolean deleteFileId(long fileId, boolean recursive) throws TachyonException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      deleteInodeInternal(inode, recursive);
      return true;
      // TODO: write to journal
    }
  }

  public boolean deletePath(TachyonURI path, boolean recursive) throws InvalidPathException,
      TachyonException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      deleteInodeInternal(inode, recursive);
      return true;
      // TODO: write to journal
    }
  }

  private boolean deleteInodeInternal(Inode inode, boolean recursive) throws TachyonException {
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

      mInodeTree.deleteInode(delInode);
    }
    return true;
  }

  public long getBlockId(long fileId, int blockIndex) {
    // TODO
    return 0;
  }

  public ClientBlockInfo getClientBlockInfo(long blockId) {
    // TODO: remove???
    return null;
  }

  public List<ClientBlockInfo> getFileBlockInfoList(long fileId) throws FileDoesNotExistException {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      InodeFile tFile = (InodeFile) inode;
      List<UserBlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(tFile.getBlockIds());
      List<ClientBlockInfo> ret = new ArrayList<ClientBlockInfo>();

      for (UserBlockInfo blockInfo : blockInfoList) {
        // Construct an file block info object to return.
        ClientBlockInfo clientBlockInfo = new ClientBlockInfo();

        clientBlockInfo.blockId = blockInfo.mBlockId;
        clientBlockInfo.length = blockInfo.mLength;
        // TODO: change ClientBlockInfo to return the richer worker location info.
        List<NetAddress> addressList = new ArrayList<NetAddress>();
        for (UserBlockLocation blockLocation : blockInfo.mLocations) {
          addressList.add(blockLocation.mAddress);
        }
        clientBlockInfo.locations = addressList;

        // The sequence number part of the block id is the offset.
        clientBlockInfo.offset =
            tFile.getBlockSizeByte() * BlockId.getSequenceNumber(blockInfo.mBlockId);

        if (clientBlockInfo.locations.isEmpty() && tFile.hasCheckpointed()) {
          // No tachyon locations, but there is a checkpoint in the under storage system. Add the
          // locations from the under storage system.
          UnderFileSystem ufs = UnderFileSystem.get(tFile.getUfsPath(), mTachyonConf);
          List<String> locs = null;
          try {
            locs = ufs.getFileLocations(tFile.getUfsPath(), clientBlockInfo.offset);
          } catch (IOException e) {
            ret.add(clientBlockInfo);
            continue;
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
              clientBlockInfo.locations.add(new NetAddress(resolvedHost, -1, resolvedPort));
            }
          }
        }
        ret.add(clientBlockInfo);
      }

      LOG.debug("getFileLocations: {} {}", fileId, ret);
      return ret;
    }
  }

  public boolean mkdirs(TachyonURI path, boolean recursive) throws InvalidPathException,
      FileAlreadyExistException, BlockInfoException {
    // TODO: metrics
    synchronized (mInodeTree) {
      mInodeTree.createPath(path, 0, recursive, true);
      return true;
      // TODO: write to journal
    }

  }

  public boolean rename(TachyonURI srcPath, TachyonURI dstPath) throws InvalidPathException {
    // TODO: metrics
    synchronized (mInodeTree) {
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
          throw new InvalidPathException("Failed to rename: " + srcPath + " is a prefix of "
              + dstPath);
        }
      }

      TachyonURI srcParentURI = srcPath.getParent();
      TachyonURI dstParentURI = dstPath.getParent();

      // We traverse down to the source and destinations' parent paths
      Inode srcParentInode = mInodeTree.getInodeByPath(srcParentURI);
      if (srcParentInode == null || !srcParentInode.isDirectory()) {
        return false;
      }

      Inode dstParentInode = mInodeTree.getInodeByPath(dstParentURI);
      if (dstParentInode == null || !dstParentInode.isDirectory()) {
        return false;
      }

      InodeDirectory srcParentDirectory = (InodeDirectory) srcParentInode;
      InodeDirectory dstParentDirectory = (InodeDirectory) dstParentInode;

      // We make sure that the source path exists and the destination path doesn't
      Inode srcInode = srcParentDirectory.getChild(srcComponents[srcComponents.length - 1]);
      if (srcInode == null) {
        return false;
      }
      if (dstParentDirectory.getChild(dstComponents[dstComponents.length - 1]) != null) {
        return false;
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      long opTimeMs = System.currentTimeMillis();
      srcParentDirectory.removeChild(srcInode);
      srcParentInode.setLastModificationTimeMs(opTimeMs);
      srcInode.setParentId(dstParentInode.getId());
      srcInode.setName(dstComponents[dstComponents.length - 1]);
      dstParentDirectory.addChild(srcInode);
      dstParentInode.setLastModificationTimeMs(opTimeMs);
      return true;
      // TODO: write to journal
    }

  }

  public void setPinned(long fileId, boolean pinned) {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      mInodeTree.setPinned(inode, pinned);
      // TODO: write to journal
    }
  }

  public boolean freePath(TachyonURI path, boolean recursive) throws InvalidPathException {
    // TODO: metrics
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeByPath(path);
      if (inode == null) {
        LOG.error("File path: " + path + " does not exist");
        return true;
      }

      if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
        // true
        return false;
      }
      if (mInodeTree.isRootId(inode.getId())) {
        // The root cannot be freed.
        return false;
      }

      List<Inode> freeInodes = new ArrayList<Inode>();
      freeInodes.add(inode);
      if (inode.isDirectory()) {
        freeInodes.addAll(mInodeTree.getInodeChildrenRecursive((InodeDirectory) inode));
      }

      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i --) {
        Inode freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds());
        }
      }
      // TODO: write to journal
    }
    return true;
  }

  public Set<Integer> getPinIdList() {
    synchronized (mInodeTree) {
      return mInodeTree.getPinIdSet();
    }
  }

  // where to put this???
  public String getUfsAddress() {
    return mTachyonConf.get(Constants.UNDERFS_ADDRESS, "/underFSStorage");
  }

  // maybe do this with listStatus???
  public void listFiles(String path, boolean recursive) {

  }

  public void reportLostFile(long fileId) {
    synchronized (mInodeTree) {
      Inode inode = mInodeTree.getInodeById(fileId);
      if (inode == null) {
        LOG.warn("Tachyon does not have file id " + fileId);
        return;
      }
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

  public ClientDependencyInfo getClientDependencyInfo(int dependencyId)
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
}
