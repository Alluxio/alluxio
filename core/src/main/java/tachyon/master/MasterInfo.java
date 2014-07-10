/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystem.SpaceType;
import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;
import tachyon.thrift.TachyonException;
import tachyon.util.CommonUtils;

/**
 * A global view of filesystem in master.
 */
public class MasterInfo extends ImageWriter {
  /**
   * Master info periodical status check.
   */
  public class MasterInfoHeartbeatExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      LOG.debug("System status checking.");

      Set<Long> lostWorkers = new HashSet<Long>();

      synchronized (mWorkers) {
        for (Entry<Long, MasterWorkerInfo> worker : mWorkers.entrySet()) {
          if (CommonUtils.getCurrentMs() - worker.getValue().getLastUpdatedTimeMs() > MASTER_CONF.WORKER_TIMEOUT_MS) {
            LOG.error("The worker " + worker.getValue() + " got timed out!");
            mLostWorkers.add(worker.getValue());
            lostWorkers.add(worker.getKey());
          }
        }
        for (long workerId : lostWorkers) {
          MasterWorkerInfo workerInfo = mWorkers.get(workerId);
          mWorkerAddressToId.remove(workerInfo.getAddress());
          mWorkers.remove(workerId);
        }
      }

      boolean hadFailedWorker = false;

      while (mLostWorkers.size() != 0) {
        hadFailedWorker = true;
        MasterWorkerInfo worker = mLostWorkers.poll();

        // TODO these two locks are not efficient. Since node failure is rare, this is fine for now.
        synchronized (mRoot) {
          synchronized (mDependencies) {
            try {
              for (long blockId : worker.getBlocks()) {
                int fileId = BlockInfo.computeInodeId(blockId);
                InodeFile tFile = (InodeFile) mInodes.get(fileId);
                if (tFile != null) {
                  int blockIndex = BlockInfo.computeBlockIndex(blockId);
                  tFile.removeLocation(blockIndex, worker.getId());
                  if (!tFile.hasCheckpointed() && tFile.getBlockLocations(blockIndex).size() == 0) {
                    LOG.info("Block " + blockId + " got lost from worker " + worker.getId() + " .");
                    int depId = tFile.getDependencyId();
                    if (depId == -1) {
                      LOG.error("Permanent Data loss: " + tFile);
                    } else {
                      mLostFiles.add(tFile.getId());
                      Dependency dep = mDependencies.get(depId);
                      dep.addLostFile(tFile.getId());
                      LOG.info("File " + tFile.getId() + " got lost from worker " + worker.getId()
                          + " . Trying to recompute it using dependency " + dep.ID);
                      if (!getPath(tFile).startsWith(MASTER_CONF.TEMPORARY_FOLDER)) {
                        mMustRecomputeDependencies.add(depId);
                      }
                    }
                  } else {
                    LOG.info("Block " + blockId + " only lost an in memory copy from worker "
                        + worker.getId());
                  }
                }
              }
            } catch (BlockInfoException e) {
              LOG.error(e);
            }
          }
        }
      }

      if (hadFailedWorker) {
        LOG.warn("Restarting failed workers.");
        try {
          java.lang.Runtime.getRuntime().exec(
              CommonConf.get().TACHYON_HOME + "/bin/tachyon-start.sh restart_workers");
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  public class RecomputationScheduler implements Runnable {
    @Override
    public void run() {
      while (true) {
        boolean hasLostFiles = false;
        boolean launched = false;
        List<String> cmds = new ArrayList<String>();
        synchronized (mRoot) {
          synchronized (mDependencies) {
            if (!mMustRecomputeDependencies.isEmpty()) {
              List<Integer> recomputeList = new ArrayList<Integer>();
              Queue<Integer> checkQueue = new LinkedList<Integer>();

              checkQueue.addAll(mMustRecomputeDependencies);
              while (!checkQueue.isEmpty()) {
                int depId = checkQueue.poll();
                Dependency dep = mDependencies.get(depId);
                boolean canLaunch = true;
                for (int k = 0; k < dep.PARENT_FILES.size(); k ++) {
                  int fildId = dep.PARENT_FILES.get(k);
                  if (mLostFiles.contains(fildId)) {
                    canLaunch = false;
                    InodeFile iFile = (InodeFile) mInodes.get(fildId);
                    if (!mBeingRecomputedFiles.contains(fildId)) {
                      int tDepId = iFile.getDependencyId();
                      if (tDepId != -1 && !mMustRecomputeDependencies.contains(tDepId)) {
                        mMustRecomputeDependencies.add(tDepId);
                        checkQueue.add(tDepId);
                      }
                    }
                  }
                }
                if (canLaunch) {
                  recomputeList.add(depId);
                }
              }
              hasLostFiles = !mMustRecomputeDependencies.isEmpty();
              launched = (recomputeList.size() > 0);

              for (int k = 0; k < recomputeList.size(); k ++) {
                mMustRecomputeDependencies.remove(recomputeList.get(k));
                Dependency dep = mDependencies.get(recomputeList.get(k));
                mBeingRecomputedFiles.addAll(dep.getLostFiles());
                cmds.add(dep.getCommand());
              }
            }
          }
        }

        for (String cmd : cmds) {
          String filePath =
              CommonConf.get().TACHYON_HOME + "/logs/rerun-" + mRerunCounter.incrementAndGet();
          new Thread(new RecomputeCommand(cmd, filePath)).start();
        }

        if (!launched) {
          if (hasLostFiles) {
            LOG.info("HasLostFiles, but no job can be launched.");
          }
          CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        }
      }
    }
  }

  public static final String COL = "COL_";

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final InetSocketAddress MASTER_ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;
  private final MasterConf MASTER_CONF;
  private final Counters mCheckpointInfo = new Counters(0, 0, 0);

  private final AtomicInteger mInodeCounter = new AtomicInteger(0);
  private final AtomicInteger mDependencyCounter = new AtomicInteger(0);
  private final AtomicInteger mRerunCounter = new AtomicInteger(0);

  private final AtomicInteger mUserCounter = new AtomicInteger(0);
  private final AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // Root Inode's id must be 1.
  private InodeFolder mRoot;

  // A map from file ID's to Inodes. All operations on it are currently synchronized on mRoot.
  private final Map<Integer, Inode> mInodes = new HashMap<Integer, Inode>();
  private final Map<Integer, Dependency> mDependencies = new HashMap<Integer, Dependency>();
  private final RawTables mRawTables = new RawTables();

  // TODO add initialization part for master failover or restart. All operations on these members
  // are synchronized on mDependencies.
  private final Set<Integer> mUncheckpointedDependencies = new HashSet<Integer>();
  private final Set<Integer> mPriorityDependencies = new HashSet<Integer>();
  private final Set<Integer> mLostFiles = new HashSet<Integer>();

  private final Set<Integer> mBeingRecomputedFiles = new HashSet<Integer>();
  private final Set<Integer> mMustRecomputeDependencies = new HashSet<Integer>();
  private final Map<Long, MasterWorkerInfo> mWorkers = new HashMap<Long, MasterWorkerInfo>();

  private final Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();

  private final BlockingQueue<MasterWorkerInfo> mLostWorkers = new ArrayBlockingQueue<MasterWorkerInfo>(32);

  // TODO Check the logic related to this two lists.
  private final PrefixList mWhiteList;
  // Synchronized set containing all InodeFile ids that are currently pinned.
  private final Set<Integer> mFileIdPinList;

  private final Journal mJournal;

  private HeartbeatThread mHeartbeatThread;

  private Thread mRecomputeThread;

  public MasterInfo(InetSocketAddress address, Journal journal) throws IOException {
    MASTER_CONF = MasterConf.get();

    mRoot = new InodeFolder("", mInodeCounter.incrementAndGet(), -1, System.currentTimeMillis());
    mInodes.put(mRoot.getId(), mRoot);

    MASTER_ADDRESS = address;
    START_TIME_MS = System.currentTimeMillis();
    // TODO This name need to be changed.
    START_TIME_NS_PREFIX = START_TIME_MS - (START_TIME_MS % 1000000);
    mJournal = journal;

    mWhiteList = new PrefixList(MASTER_CONF.WHITELIST);
    mFileIdPinList = Collections.synchronizedSet(new HashSet<Integer>());

    mJournal.loadImage(this);
  }

  int _createDependency(List<Integer> parentsIds, List<Integer> childrenIds, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType dependencyType, int dependencyId, long creationTimeMs)
      throws InvalidPathException, FileDoesNotExistException {
    Dependency dep = null;
    synchronized (mRoot) {
      Set<Integer> parentDependencyIds = new HashSet<Integer>();
      for (int k = 0; k < parentsIds.size(); k ++) {
        int parentId = parentsIds.get(k);
        Inode inode = mInodes.get(parentId);
        if (inode.isFile()) {
          LOG.info("PARENT DEPENDENCY ID IS " + ((InodeFile) inode).getDependencyId() + " "
              + (inode));
          if (((InodeFile) inode).getDependencyId() != -1) {
            parentDependencyIds.add(((InodeFile) inode).getDependencyId());
          }
        } else {
          throw new InvalidPathException("Parent " + parentId + " is not a file.");
        }
      }

      dep =
          new Dependency(dependencyId, parentsIds, childrenIds, commandPrefix, data, comment,
              framework, frameworkVersion, dependencyType, parentDependencyIds, creationTimeMs);

      List<Inode> childrenInodes = new ArrayList<Inode>();
      for (int k = 0; k < childrenIds.size(); k ++) {
        InodeFile inode = (InodeFile) mInodes.get(childrenIds.get(k));
        inode.setDependencyId(dep.ID);
        childrenInodes.add(inode);
        if (inode.hasCheckpointed()) {
          dep.childCheckpointed(inode.getId());
        }
      }
    }

    synchronized (mDependencies) {
      mDependencies.put(dep.ID, dep);
      if (!dep.hasCheckpointed()) {
        mUncheckpointedDependencies.add(dep.ID);
      }
      for (int parentDependencyId : dep.PARENT_DEPENDENCIES) {
        mDependencies.get(parentDependencyId).addChildrenDependency(dep.ID);
      }
    }

    mJournal.getEditLog().createDependency(parentsIds, childrenIds, commandPrefix, data, comment,
        framework, frameworkVersion, dependencyType, dependencyId, creationTimeMs);
    mJournal.getEditLog().flush();

    LOG.info("Dependency created: " + dep);

    return dep.ID;
  }

  // TODO Make this API better.
  /**
   * Internal API.
   * 
   * @param recursive
   *          If recursive is true and the filesystem tree is not filled in all the way to path yet,
   *          it fills in the missing components.
   * @param path
   *          The path to create
   * @param directory
   *          If true, creates an InodeFolder instead of an Inode
   * @param blockSizeByte
   *          If it's a file, the block size for the Inode
   * @param creationTimeMs
   *          The time the file was created
   * @return the id of the inode created at the given path
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws BlockInfoException
   * @throws TachyonException
   */
  int _createFile(boolean recursive, String path, boolean directory, long blockSizeByte,
      long creationTimeMs) throws FileAlreadyExistException, InvalidPathException,
      BlockInfoException, TachyonException {
    if (path.equals(Constants.PATH_SEPARATOR)) {
      LOG.info("FileAlreadyExistException: " + path);
      throw new FileAlreadyExistException(path);
    }

    if (!directory && blockSizeByte < 1) {
      throw new BlockInfoException("Invalid block size " + blockSizeByte);
    } else if (CommonUtils.isRoot(path)) {
      throw new InvalidPathException("Cannot create the root path");
    }

    LOG.debug("createFile" + CommonUtils.parametersToString(path));

    String[] pathNames = CommonUtils.getPathComponents(path);
    String name = pathNames[pathNames.length - 1];

    String[] parentPath = new String[pathNames.length - 1];
    System.arraycopy(pathNames, 0, parentPath, 0, parentPath.length);

    synchronized (mRoot) {
      Pair<Inode, Integer> inodeTraversal = traverseToInode(parentPath);
      // pathIndex is the index into pathNames where we start filling in the path from the inode.
      int pathIndex = parentPath.length;
      if (!traversalSucceeded(inodeTraversal)) {
        // Then the path component at errorInd k doesn't exist. If it's not recursive, we throw an
        // exception here. Otherwise we add the remaining path components to the list of components
        // to create.
        if (!recursive) {
          final String msg =
              "File " + path + " creation failed. Component " + inodeTraversal.getSecond() + "("
                  + parentPath[inodeTraversal.getSecond()] + ") does not exist";
          LOG.info("InvalidPathException: " + msg);
          throw new InvalidPathException(msg);
        } else {
          // We will start filling in the path from inodeTraversal.getSecond()
          pathIndex = inodeTraversal.getSecond();
        }
      }

      if (!inodeTraversal.getFirst().isDirectory()) {
        throw new InvalidPathException("Could not traverse to parent folder of path " + path
            + ". Component " + pathNames[pathIndex - 1] + " is not a directory.");
      }
      InodeFolder currentInodeFolder = (InodeFolder) inodeTraversal.getFirst();
      // Fill in the directories that were missing.
      for (int k = pathIndex; k < parentPath.length; k ++) {
        Inode dir =
            new InodeFolder(pathNames[k], mInodeCounter.incrementAndGet(),
                currentInodeFolder.getId(), creationTimeMs);
        dir.setPinned(currentInodeFolder.isPinned());
        currentInodeFolder.addChild(dir);
        mInodes.put(dir.getId(), dir);
        currentInodeFolder = (InodeFolder) dir;
      }

      // Create the final path component. First we need to make sure that there isn't already a file
      // here with that name. If there is an existing file that is a directory and we're creating a
      // directory, we just return the existing directory's id.
      Inode ret = currentInodeFolder.getChild(name);
      if (ret != null) {
        if (ret.isDirectory() && directory) {
          return ret.getId();
        }
        LOG.info("FileAlreadyExistException: " + path);
        throw new FileAlreadyExistException(path);
      }
      if (directory) {
        ret =
            new InodeFolder(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                creationTimeMs);
        ret.setPinned(currentInodeFolder.isPinned());
      } else {
        ret =
            new InodeFile(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                blockSizeByte, creationTimeMs);
        ret.setPinned(currentInodeFolder.isPinned());
        if (ret.isPinned()) {
          mFileIdPinList.add(ret.getId());
        }
        if (mWhiteList.inList(path)) {
          ((InodeFile) ret).setCache(true);
        }
      }

      mInodes.put(ret.getId(), ret);
      currentInodeFolder.addChild(ret);

      LOG.debug("createFile: File Created: " + ret.toString() + " parent: "
          + currentInodeFolder.toString());
      return ret.getId();
    }
  }

  void _createRawTable(int tableId, int columns, ByteBuffer metadata) throws TachyonException {
    synchronized (mRawTables) {
      if (!mRawTables.addRawTable(tableId, columns, metadata)) {
        throw new TachyonException("Failed to create raw table.");
      }
      mJournal.getEditLog().createRawTable(tableId, columns, metadata);
    }
  }

  /**
   * Inner delete function. Return true if the file does not exist in the first place.
   * 
   * @param fileId
   *          The inode to delete
   * @param recursive
   *          True if the file and it's subdirectories should be deleted
   * @return true if the deletion succeeded and false otherwise.
   * @throws TachyonException
   */
  boolean _delete(int fileId, boolean recursive) throws TachyonException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        return true;
      }

      if (inode.isDirectory() && !recursive && ((InodeFolder) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
        // true
        return false;
      }

      if (inode.getId() == mRoot.getId()) {
        // The root cannot be deleted.
        return false;
      }

      List<Inode> delInodes = new ArrayList<Inode>();
      delInodes.add(inode);
      if (inode.isDirectory()) {
        delInodes.addAll(getInodeChildrenRecursive((InodeFolder) inode));
      }

      // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (int i = delInodes.size() - 1; i >= 0; i --) {
        Inode delInode = delInodes.get(i);

        if (delInode.isFile()) {
          String checkpointPath = ((InodeFile) delInode).getUfsPath();
          if (!checkpointPath.equals("")) {
            UnderFileSystem ufs = UnderFileSystem.get(checkpointPath);
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

          List<Pair<Long, Long>> blockIdWorkerIdList =
              ((InodeFile) delInode).getBlockIdWorkerIdPairs();
          synchronized (mWorkers) {
            for (Pair<Long, Long> blockIdWorkerId : blockIdWorkerIdList) {
              MasterWorkerInfo workerInfo = mWorkers.get(blockIdWorkerId.getSecond());
              if (workerInfo != null) {
                workerInfo.updateToRemovedBlock(true, blockIdWorkerId.getFirst());
              }
            }
          }

          mFileIdPinList.remove(delInode.getId());
        }

        InodeFolder parent = (InodeFolder) mInodes.get(delInode.getParentId());
        parent.removeChild(delInode);

        if (mRawTables.exist(delInode.getId()) && !mRawTables.delete(delInode.getId())) {
          return false;
        }

        mInodes.remove(delInode.getId());
        delInode.reverseId();
      }

      return true;
    }
  }

  /**
   * Get the raw table info associated with the given id.
   * 
   * @param path
   *          The path of the table
   * @param inode
   *          The inode at the path
   * @return the table info
   * @throws TableDoesNotExistException
   */
  public ClientRawTableInfo _getClientRawTableInfo(String path, Inode inode)
      throws TableDoesNotExistException {
    LOG.info("getClientRawTableInfo(" + path + ")");
    if (!mRawTables.exist(inode.getId())) {
      throw new TableDoesNotExistException("Table " + inode.getId() + " does not exist.");
    }
    ClientRawTableInfo ret = new ClientRawTableInfo();
    ret.id = inode.getId();
    ret.name = inode.getName();
    ret.path = path;
    ret.columns = mRawTables.getColumns(ret.id);
    ret.metadata = mRawTables.getMetadata(ret.id);
    return ret;
  }

  /**
   * Get the names of the sub-directories at the given path.
   * 
   * @param inode
   *          The inode to list
   * @param path
   *          The path of the given inode
   * @param recursive
   *          If true, recursively add the paths of the sub-directories
   * @return the list of paths
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  private List<String> _ls(Inode inode, String path, boolean recursive)
      throws InvalidPathException, FileDoesNotExistException {
    synchronized (mRoot) {
      List<String> ret = new ArrayList<String>();
      ret.add(path);
      if (inode.isDirectory()) {
        for (Inode child : ((InodeFolder) inode).getChildren()) {
          String childPath = CommonUtils.concat(path, child.getName());
          if (recursive) {
            ret.addAll(_ls(child, childPath, recursive));
          } else {
            ret.add(childPath);
          }
        }
      }
      return ret;
    }
  }

  /**
   * Rename a file to the given path, inner method.
   * 
   * @param fileId
   *          The id of the file to rename
   * @param dstPath
   *          The new path of the file
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException
   *           If the id doesn't point to an inode
   * @throws InvalidPathException
   *           if the source path is a prefix of the destination
   */
  public boolean _rename(int fileId, String dstPath) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      String srcPath = getPath(fileId);
      if (srcPath.equals(dstPath)) {
        return true;
      }
      if (srcPath.equals(Constants.PATH_SEPARATOR) || dstPath.equals(Constants.PATH_SEPARATOR)) {
        return false;
      }
      String[] srcComponents = CommonUtils.getPathComponents(srcPath);
      String[] dstComponents = CommonUtils.getPathComponents(dstPath);
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

      String srcParent = CommonUtils.getParent(srcPath);
      String dstParent = CommonUtils.getParent(dstPath);

      // We traverse down to the source and destinations' parent paths
      Inode srcParentInode = getInode(srcParent);
      if (srcParentInode == null || !srcParentInode.isDirectory()) {
        return false;
      }

      Inode dstParentInode = getInode(dstParent);
      if (dstParentInode == null || !dstParentInode.isDirectory()) {
        return false;
      }

      // We make sure that the source path exists and the destination path doesn't
      Inode srcInode =
          ((InodeFolder) srcParentInode).getChild(srcComponents[srcComponents.length - 1]);
      if (srcInode == null) {
        return false;
      }
      if (((InodeFolder) dstParentInode).getChild(dstComponents[dstComponents.length - 1]) != null) {
        return false;
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      ((InodeFolder) srcParentInode).removeChild(srcInode);
      srcInode.setParentId(dstParentInode.getId());
      srcInode.setName(dstComponents[dstComponents.length - 1]);
      ((InodeFolder) dstParentInode).addChild(srcInode);
      return true;
    }
  }

  private void addBlock(InodeFile tFile, BlockInfo blockInfo) throws BlockInfoException {
    tFile.addBlock(blockInfo);
    mJournal.getEditLog().addBlock(tFile.getId(), blockInfo.BLOCK_INDEX, blockInfo.LENGTH);
    mJournal.getEditLog().flush();
  }

  /**
   * Add a checkpoint to a file.
   * 
   * @param workerId
   *          The worker which submitted the request. -1 if the request is not from a worker.
   * @param fileId
   *          The file to add the checkpoint.
   * @param length
   *          The length of the checkpoint.
   * @param checkpointPath
   *          The path of the checkpoint.
   * @return true if the checkpoint is added successfully, false if not.
   * @throws FileNotFoundException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public boolean addCheckpoint(long workerId, int fileId, long length, String checkpointPath)
      throws FileNotFoundException, SuspectedFileSizeException, BlockInfoException {
    LOG.info(CommonUtils.parametersToString(workerId, fileId, length, checkpointPath));

    if (workerId != -1) {
      MasterWorkerInfo tWorkerInfo = getWorkerInfo(workerId);
      tWorkerInfo.updateLastUpdatedTimeMs();
    }

    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileNotFoundException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileNotFoundException("File " + fileId + " is a folder.");
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
        tFile.setUfsPath(checkpointPath);
        needLog = true;

        synchronized (mDependencies) {
          int depId = tFile.getDependencyId();
          if (depId != -1) {
            Dependency dep = mDependencies.get(depId);
            dep.childCheckpointed(tFile.getId());
            if (dep.hasCheckpointed()) {
              mUncheckpointedDependencies.remove(dep.ID);
              mPriorityDependencies.remove(dep.ID);
            }
          }
        }
      }
      addFile(fileId, tFile.getDependencyId());
      tFile.setComplete();

      if (needLog) {
        mJournal.getEditLog().addCheckpoint(fileId, length, checkpointPath);
        mJournal.getEditLog().flush();
      }
      return true;
    }
  }

  /**
   * Removes a checkpointed file from the set of lost or being-recomputed files if it's there
   * 
   * @param fileId
   *          The file to examine
   */
  private void addFile(int fileId, int dependencyId) {
    synchronized (mDependencies) {
      if (mLostFiles.contains(fileId)) {
        mLostFiles.remove(fileId);
      }
      if (mBeingRecomputedFiles.contains(fileId)) {
        mBeingRecomputedFiles.remove(fileId);
      }
    }
  }

  /**
   * Recomputes mFileIdPinList at the given Inode, recursively recomputing for children.
   * Optionally will set the "pinned" flag as we go.
   * 
   * @param inode
   *          The inode to start traversal from
   * @param setPinState
   *          An optional parameter indicating whether we should also set the "pinned"
   *          flag on each inode we traverse. If absent, the "isPinned" flag is unchanged.
   */
  private void recomputePinnedFiles(Inode inode, Optional<Boolean> setPinState) {
    if (setPinState.isPresent()) {
      inode.setPinned(setPinState.get());
    }

    if (inode.isFile()) {
      if (inode.isPinned()) {
        mFileIdPinList.add(inode.getId());
      } else {
        mFileIdPinList.remove(inode.getId());
      }
    } else if (inode.isDirectory()) {
      for (Inode child : ((InodeFolder) inode).getChildren()) {
        recomputePinnedFiles(child, setPinState);
      }
    }
  }

  /**
   * While loading an image, addToInodeMap will map the various ids to their inodes.
   * 
   * @param inode
   *          The inode to add
   * @param map
   *          The map to add the inodes to
   */
  private void addToInodeMap(Inode inode, Map<Integer, Inode> map) {
    map.put(inode.getId(), inode);
    if (inode.isDirectory()) {
      InodeFolder inodeFolder = (InodeFolder) inode;
      for (Inode child : inodeFolder.getChildren()) {
        addToInodeMap(child, map);
      }
    }
  }

  /**
   * A worker cache a block in its memory.
   * 
   * @param workerId
   * @param workerUsedBytes
   * @param blockId
   * @param length
   * @return the dependency id of the file if it has not been checkpointed. -1
   *         means the file either does not have dependency or has already been checkpointed.
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public int cacheBlock(long workerId, long workerUsedBytes, long blockId, long length)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException {
    LOG.debug(CommonUtils.parametersToString(workerId, workerUsedBytes, blockId, length));

    MasterWorkerInfo tWorkerInfo = getWorkerInfo(workerId);
    tWorkerInfo.updateBlock(true, blockId);
    tWorkerInfo.updateUsedBytes(workerUsedBytes);
    tWorkerInfo.updateLastUpdatedTimeMs();

    int fileId = BlockInfo.computeInodeId(blockId);
    int blockIndex = BlockInfo.computeBlockIndex(blockId);
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      InodeFile tFile = (InodeFile) inode;
      if (tFile.getNumberOfBlocks() <= blockIndex) {
        addBlock(tFile, new BlockInfo(tFile, blockIndex, length));
      }

      InetSocketAddress address = tWorkerInfo.ADDRESS;
      tFile.addLocation(blockIndex, workerId, new NetAddress(address.getAddress()
          .getCanonicalHostName(), address.getPort()));

      if (tFile.hasCheckpointed()) {
        return -1;
      } else {
        return tFile.getDependencyId();
      }
    }
  }

  /**
   * Completes the checkpointing of a file.
   * 
   * @param fileId
   *          The id of the file
   * @throws FileDoesNotExistException
   */
  public void completeFile(int fileId) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exit.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File " + fileId + " is not a file.");
      }

      addFile(fileId, ((InodeFile) inode).getDependencyId());

      ((InodeFile) inode).setComplete();
      mJournal.getEditLog().completeFile(fileId);
      mJournal.getEditLog().flush();
    }
  }

  public int createDependency(List<String> parents, List<String> children, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType dependencyType) throws InvalidPathException, FileDoesNotExistException {
    synchronized (mRoot) {
      LOG.info("ParentList: " + CommonUtils.listToString(parents));
      List<Integer> parentsIdList = getFilesIds(parents);
      List<Integer> childrenIdList = getFilesIds(children);

      int depId = mDependencyCounter.incrementAndGet();
      long creationTimeMs = System.currentTimeMillis();
      int ret =
          _createDependency(parentsIdList, childrenIdList, commandPrefix, data, comment,
              framework, frameworkVersion, dependencyType, depId, creationTimeMs);

      return ret;
    }
  }

  /**
   * Create a file. // TODO Make this API better.
   * 
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws BlockInfoException
   * @throws TachyonException
   */
  public int createFile(boolean recursive, String path, boolean directory, long blockSizeByte)
      throws FileAlreadyExistException, InvalidPathException, BlockInfoException, TachyonException {
    long creationTimeMs = System.currentTimeMillis();
    synchronized (mRoot) {
      int ret = _createFile(recursive, path, directory, blockSizeByte, creationTimeMs);
      mJournal.getEditLog().createFile(recursive, path, directory, blockSizeByte, creationTimeMs);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  public int createFile(String path, long blockSizeByte) throws FileAlreadyExistException,
      InvalidPathException, BlockInfoException, TachyonException {
    return createFile(true, path, false, blockSizeByte);
  }

  /**
   * Creates a new block for the given file.
   * 
   * @param fileId
   *          The id of the file
   * @return the block id.
   * @throws FileDoesNotExistException
   */
  public long createNewBlock(int fileId) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exit.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File " + fileId + " is not a file.");
      }

      return ((InodeFile) inode).getNewBlockId();
    }
  }

  /**
   * Creates a raw table.
   * 
   * @param path
   *          The path to place the table at
   * @param columns
   *          The number of columns in the table
   * @param metadata
   *          Additional metadata about the table
   * @return the file id of the table
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws TableColumnException
   * @throws TachyonException
   */
  public int createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException {
    LOG.info("createRawTable" + CommonUtils.parametersToString(path, columns));

    if (columns <= 0 || columns >= CommonConf.get().MAX_COLUMNS) {
      throw new TableColumnException("Column " + columns + " should between 0 to "
          + CommonConf.get().MAX_COLUMNS);
    }

    int id;
    try {
      id = createFile(true, path, true, 0);
      _createRawTable(id, columns, metadata);
    } catch (BlockInfoException e) {
      throw new FileAlreadyExistException(e.getMessage());
    }

    for (int k = 0; k < columns; k ++) {
      mkdir(CommonUtils.concat(path, COL + k));
    }

    return id;
  }

  /**
   * Delete a file based on the file's ID.
   * 
   * @param fileId
   *          the file to be deleted.
   * @param recursive
   *          whether delete the file recursively or not.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean delete(int fileId, boolean recursive) throws TachyonException {
    synchronized (mRoot) {
      boolean ret = _delete(fileId, recursive);
      mJournal.getEditLog().delete(fileId, recursive);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  /**
   * Delete files based on the path.
   * 
   * @param path
   *          The file to be deleted.
   * @param recursive
   *          whether delete the file recursively or not.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean delete(String path, boolean recursive) throws TachyonException {
    LOG.info("delete(" + path + ")");
    synchronized (mRoot) {
      Inode inode = null;
      try {
        inode = getInode(path);
      } catch (InvalidPathException e) {
        return false;
      }
      if (inode == null) {
        return true;
      }
      return delete(inode.getId(), recursive);
    }
  }

  public long getBlockIdBasedOnOffset(int fileId, long offset) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException(fileId + " is not a file.");
      }

      return ((InodeFile) inode).getBlockIdBasedOnOffset(offset);
    }
  }

  /**
   * Get the list of blocks of an InodeFile determined by path.
   * 
   * @param path
   *          The file.
   * @return The list of the blocks of the file.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<BlockInfo> getBlockList(String path) throws InvalidPathException,
      FileDoesNotExistException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new FileDoesNotExistException(path + " does not exist.");
    }
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(path + " is not a file.");
    }
    InodeFile inodeFile = (InodeFile) inode;
    return inodeFile.getBlockList();
  }

  /**
   * Get the capacity of the whole system.
   * 
   * @return the system's capacity in bytes.
   */
  public long getCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  /**
   * Get the block info associated with the given id.
   * 
   * @param blockId
   *          The id of the block return
   * @return the block info
   * @throws FileDoesNotExistException
   * @throws IOException
   * @throws BlockInfoException
   */
  public ClientBlockInfo getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      IOException, BlockInfoException {
    int fileId = BlockInfo.computeInodeId(blockId);
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      ClientBlockInfo ret =
          ((InodeFile) inode).getClientBlockInfo(BlockInfo.computeBlockIndex(blockId));
      LOG.debug("getClientBlockInfo: " + blockId + ret);
      return ret;
    }
  }

  /**
   * Get the dependency info associated with the given id.
   * 
   * @param dependencyId
   *          The id of the dependency
   * @return the dependency info
   * @throws DependencyDoesNotExistException
   */
  public ClientDependencyInfo getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException {
    Dependency dep = null;
    synchronized (mDependencies) {
      dep = mDependencies.get(dependencyId);
      if (dep == null) {
        throw new DependencyDoesNotExistException("No dependency with id " + dependencyId);
      }
    }
    return dep.generateClientDependencyInfo();
  }

  /**
   * Get the file info associated with the given id.
   * 
   * @param fid
   *          The id of the file
   * @return the file info
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public ClientFileInfo getClientFileInfo(int fid) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fid);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to get client file info: " + fid
            + " does not exist");
      }
      return inode.generateClientFileInfo(getPath(inode));
    }
  }

  /**
   * Get the file info for the file at the given path
   * 
   * @param path
   *          The path of the file
   * @return the file info
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public ClientFileInfo getClientFileInfo(String path) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to getClientFileInfo: " + path
            + " does not exist");
      }
      return inode.generateClientFileInfo(path);
    }
  }

  /**
   * Get the raw table info associated with the given id.
   * 
   * @param id
   *          The id of the table
   * @return the table info
   * @throws TableDoesNotExistException
   */
  public ClientRawTableInfo getClientRawTableInfo(int id) throws TableDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(id);
      if (inode == null || !inode.isDirectory()) {
        throw new TableDoesNotExistException("Table " + id + " does not exist.");
      }
      return _getClientRawTableInfo(getPath(inode), inode);
    }
  }

  /**
   * Get the raw table info for the table at the given path
   * 
   * @param path
   *          The path of the table
   * @return the table info
   * @throws TableDoesNotExistException
   * @throws InvalidPathException
   */
  public ClientRawTableInfo getClientRawTableInfo(String path) throws TableDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new TableDoesNotExistException("Table " + path + " does not exist.");
      }
      return _getClientRawTableInfo(path, inode);
    }
  }

  /**
   * Get the file id of the file.
   * 
   * @param path
   *          The path of the file
   * @return The file id of the file. -1 if the file does not exist.
   * @throws InvalidPathException
   */
  public int getFileId(String path) throws InvalidPathException {
    LOG.debug("getFileId(" + path + ")");
    Inode inode = getInode(path);
    int ret = -1;
    if (inode != null) {
      ret = inode.getId();
    }
    LOG.debug("getFileId(" + path + "): " + ret);
    return ret;
  }

  /**
   * Get the block infos of a file with the given id. Throws an exception if the id names a
   * directory.
   * 
   * @param fileId
   *          The id of the file to look up
   * @return the block infos of the file
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  public List<ClientBlockInfo> getFileLocations(int fileId) throws FileDoesNotExistException,
      IOException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      List<ClientBlockInfo> ret = ((InodeFile) inode).getClientBlockInfos();
      LOG.debug("getFileLocations: " + fileId + ret);
      return ret;
    }
  }

  /**
   * Get the block infos of a file with the given path. Throws an exception if the path names a
   * directory.
   * 
   * @param path
   *          The path of the file to look up
   * @return the block infos of the file
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   * @throws IOException
   */
  public List<ClientBlockInfo> getFileLocations(String path) throws FileDoesNotExistException,
      InvalidPathException, IOException {
    LOG.info("getFileLocations: " + path);
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return getFileLocations(inode.getId());
    }
  }

  /**
   * Get the file id's of the given paths. It recursively scans directories for the file id's inside
   * of them.
   * 
   * @param pathList
   *          The list of paths to look at
   * @return the file id's of the files.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  private List<Integer> getFilesIds(List<String> pathList) throws InvalidPathException,
      FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>(pathList.size());
    for (int k = 0; k < pathList.size(); k ++) {
      ret.addAll(listFiles(pathList.get(k), true));
    }
    return ret;
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in
   * it. If the <code>path</code> is a file, return its ClientFileInfo.
   * 
   * @param path
   *          the target directory/file path
   * @return A list of ClientFileInfo
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public List<ClientFileInfo> getFilesInfo(String path) throws FileDoesNotExistException,
      InvalidPathException {
    List<ClientFileInfo> ret = new ArrayList<ClientFileInfo>();

    Inode inode = getInode(path);
    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }

    if (inode.isDirectory()) {
      for (Inode child : ((InodeFolder) inode).getChildren()) {
        ret.add(child.generateClientFileInfo(CommonUtils.concat(path, child.getName())));
      }
    } else {
      ret.add(inode.generateClientFileInfo(path));
    }
    return ret;
  }

  /**
   * Get absolute paths of all in memory files.
   * 
   * @return absolute paths of all in memory files.
   */
  public List<String> getInMemoryFiles() {
    List<String> ret = new ArrayList<String>();
    LOG.info("getInMemoryFiles()");
    Queue<Pair<InodeFolder, String>> nodesQueue = new LinkedList<Pair<InodeFolder, String>>();
    synchronized (mRoot) {
      nodesQueue.add(new Pair<InodeFolder, String>(mRoot, ""));
      while (!nodesQueue.isEmpty()) {
        Pair<InodeFolder, String> tPair = nodesQueue.poll();
        InodeFolder tFolder = tPair.getFirst();
        String curPath = tPair.getSecond();

        Set<Inode> children = tFolder.getChildren();
        for (Inode tInode : children) {
          String newPath = CommonUtils.concat(curPath, tInode.getName());
          if (tInode.isDirectory()) {
            nodesQueue.add(new Pair<InodeFolder, String>((InodeFolder) tInode, newPath));
          } else if (((InodeFile) tInode).isFullyInMemory()) {
            ret.add(newPath);
          }
        }
      }
    }
    return ret;
  }

  /**
   * Same as {@link #getInode(String[] pathNames)} except that it takes a path string.
   */
  private Inode getInode(String path) throws InvalidPathException {
    return getInode(CommonUtils.getPathComponents(path));
  }

  /**
   * Get the inode of the file at the given path.
   * 
   * @param pathNames
   *          The path components of the path to search for
   * @return the inode of the file at the given path, or null if the file does not exist
   * @throws InvalidPathException
   */
  private Inode getInode(String[] pathNames) throws InvalidPathException {
    Pair<Inode, Integer> inodeTraversal = traverseToInode(pathNames);
    if (!traversalSucceeded(inodeTraversal)) {
      return null;
    }
    return inodeTraversal.getFirst();
  }

  /**
   * Returns a list of the given folder's children, recursively scanning subdirectories. It adds the
   * parent of a node before adding its children.
   * 
   * @param inodeFolder
   *          The folder to start looking at
   * @return a list of the children inodes.
   */
  private List<Inode> getInodeChildrenRecursive(InodeFolder inodeFolder) {
    synchronized (mRoot) {
      List<Inode> ret = new ArrayList<Inode>();
      for (Inode i : inodeFolder.getChildren()) {
        ret.add(i);
        if (i.isDirectory()) {
          ret.addAll(getInodeChildrenRecursive((InodeFolder) i));
        }
      }
      return ret;
    }
  }

  /**
   * Get Journal instance for MasterInfo for Unit test only
   * 
   * @return Journal instance
   */
  public Journal getJournal() {
    return mJournal;
  }

  /**
   * Get the master address.
   * 
   * @return the master address
   */
  public InetSocketAddress getMasterAddress() {
    return MASTER_ADDRESS;
  }

  /**
   * Get a new user id
   * 
   * @return a new user id
   */
  public long getNewUserId() {
    return mUserCounter.incrementAndGet();
  }

  /**
   * Get the number of files at a given path.
   * 
   * @param path
   *          The path to look at
   * @return The number of files at the path. Returns 1 if the path specifies a file. If it's a
   *         directory, returns the number of items in the directory.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public int getNumberOfFiles(String path) throws InvalidPathException, FileDoesNotExistException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }
    if (inode.isFile()) {
      return 1;
    }
    return ((InodeFolder) inode).getNumberOfChildren();
  }

  /**
   * Get the file path specified by a given inode.
   * 
   * @param inode
   *          The inode
   * @return the path of the inode
   */
  private String getPath(Inode inode) {
    synchronized (mRoot) {
      if (inode.getId() == 1) {
        return Constants.PATH_SEPARATOR;
      }
      if (inode.getParentId() == 1) {
        return Constants.PATH_SEPARATOR + inode.getName();
      }
      return CommonUtils.concat(getPath(mInodes.get(inode.getParentId())), inode.getName());
    }
  }

  /**
   * Get the path of a file with the given id
   * 
   * @param fileId
   *          The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException
   *           raise if the file does not exist.
   */
  public String getPath(int fileId) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist");
      }
      return getPath(inode);
    }
  }

  /**
   * Get a list of the pin id's.
   * 
   * @return a list of pin id's
   */
  public List<Integer> getPinIdList() {
    synchronized (mFileIdPinList) {
      return Lists.newArrayList(mFileIdPinList);
    }
  }

  /**
   * Creates a list of high priority dependencies, which don't yet have checkpoints.
   * 
   * @return the list of dependency ids
   */
  public List<Integer> getPriorityDependencyList() {
    synchronized (mDependencies) {
      int earliestDepId = -1;
      if (mPriorityDependencies.isEmpty()) {
        long earliest = Long.MAX_VALUE;
        for (int depId : mUncheckpointedDependencies) {
          Dependency dep = mDependencies.get(depId);
          if (!dep.hasChildrenDependency()) {
            mPriorityDependencies.add(dep.ID);
          }

          if (dep.CREATION_TIME_MS < earliest) {
            earliest = dep.CREATION_TIME_MS;
            earliestDepId = dep.ID;
          }
        }

        if (!mPriorityDependencies.isEmpty()) {
          LOG.info("New computed priority dependency list " + mPriorityDependencies);
        }
      }

      if (mPriorityDependencies.isEmpty() && earliestDepId != -1) {
        mPriorityDependencies.add(earliestDepId);
        LOG.info("Priority dependency list by earliest creation time: " + mPriorityDependencies);
      }

      List<Integer> ret = new ArrayList<Integer>(mPriorityDependencies.size());
      ret.addAll(mPriorityDependencies);
      return ret;
    }
  }

  /**
   * Get the id of the table at the given path.
   * 
   * @param path
   *          The path of the table
   * @return the id of the table
   * @throws InvalidPathException
   * @throws TableDoesNotExistException
   */
  public int getRawTableId(String path) throws InvalidPathException, TableDoesNotExistException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new TableDoesNotExistException(path);
    }
    if (inode.isDirectory()) {
      int id = inode.getId();
      if (mRawTables.exist(id)) {
        return id;
      }
    }
    return -1;
  }

  /**
   * Get the master start time in milliseconds.
   * 
   * @return the master start time in milliseconds
   */
  public long getStarttimeMs() {
    return START_TIME_MS;
  }

  /**
   * Get the capacity of the under file system.
   * 
   * @return the capacity in bytes
   * @throws IOException
   */
  public long getUnderFsCapacityBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(CommonConf.get().UNDERFS_DATA_FOLDER);
    return ufs.getSpace(CommonConf.get().UNDERFS_DATA_FOLDER, SpaceType.SPACE_TOTAL);
  }

  /**
   * Get the amount of free space in the under file system.
   * 
   * @return the free space in bytes
   * @throws IOException
   */
  public long getUnderFsFreeBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(CommonConf.get().UNDERFS_DATA_FOLDER);
    return ufs.getSpace(CommonConf.get().UNDERFS_DATA_FOLDER, SpaceType.SPACE_FREE);
  }

  /**
   * Get the amount of space used in the under file system.
   * 
   * @return the space used in bytes
   * @throws IOException
   */
  public long getUnderFsUsedBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(CommonConf.get().UNDERFS_DATA_FOLDER);
    return ufs.getSpace(CommonConf.get().UNDERFS_DATA_FOLDER, SpaceType.SPACE_USED);
  }

  /**
   * Get the amount of space used by the workers.
   * 
   * @return the amount of space used in bytes
   */
  public long getUsedBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  /**
   * Get the white list.
   * 
   * @return the white list
   */
  public List<String> getWhiteList() {
    return mWhiteList.getList();
  }

  /**
   * Get the address of a worker.
   * 
   * @param random
   *          If true, select a random worker
   * @param host
   *          If <code>random</code> is false, select a worker on this host
   * @return the address of the selected worker, or null if no address could be found
   */
  public NetAddress getWorker(boolean random, String host) {
    synchronized (mWorkers) {
      if (mWorkerAddressToId.isEmpty()) {
        return null;
      }
      if (random) {
        int index = new Random(mWorkerAddressToId.size()).nextInt(mWorkerAddressToId.size());
        for (InetSocketAddress address : mWorkerAddressToId.keySet()) {
          if (index == 0) {
            LOG.debug("getRandomWorker: " + address);
            return new NetAddress(address.getHostName(), address.getPort());
          }
          index --;
        }
        for (InetSocketAddress address : mWorkerAddressToId.keySet()) {
          LOG.debug("getRandomWorker: " + address);
          return new NetAddress(address.getHostName(), address.getPort());
        }
      } else {
        for (InetSocketAddress address : mWorkerAddressToId.keySet()) {
          if (address.getHostName().equals(host)
              || address.getAddress().getHostAddress().equals(host)
              || address.getAddress().getCanonicalHostName().equals(host)) {
            LOG.debug("getLocalWorker: " + address);
            return new NetAddress(address.getHostName(), address.getPort());
          }
        }
      }
    }
    LOG.info("getLocalWorker: no local worker on " + host);
    return null;
  }

  /**
   * Get the number of workers.
   * 
   * @return the number of workers
   */
  public int getWorkerCount() {
    synchronized (mWorkers) {
      return mWorkers.size();
    }
  }

  /**
   * Get info about a worker.
   * 
   * @param workerId
   *          The id of the worker to look at
   * @return the info about the worker
   */
  private MasterWorkerInfo getWorkerInfo(long workerId) {
    MasterWorkerInfo ret = null;
    synchronized (mWorkers) {
      ret = mWorkers.get(workerId);

      if (ret == null) {
        LOG.error("No worker: " + workerId);
      }
    }
    return ret;
  }

  /**
   * Get info about all the workers.
   * 
   * @return a list of worker infos
   */
  public List<ClientWorkerInfo> getWorkersInfo() {
    List<ClientWorkerInfo> ret = new ArrayList<ClientWorkerInfo>();

    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret.add(worker.generateClientWorkerInfo());
      }
    }

    return ret;
  }

  public void init() throws IOException {
    mCheckpointInfo.updateEditTransactionCounter(mJournal.loadEditLog(this));

    mJournal.createImage(this);
    mJournal.createEditLog(mCheckpointInfo.getEditTransactionCounter());

    mHeartbeatThread =
        new HeartbeatThread("Master Heartbeat", new MasterInfoHeartbeatExecutor(),
            MASTER_CONF.HEARTBEAT_INTERVAL_MS);
    mHeartbeatThread.start();

    mRecomputeThread = new Thread(new RecomputationScheduler());
    mRecomputeThread.start();
  }

  /**
   * Get the id of the file at the given path. If recursive, it scans the subdirectories as well.
   * 
   * @param path
   *          The path to start looking at
   * @param recursive
   *          If true, recursively scan the subdirectories at the given path as well
   * @return the list of the inode id's at the path
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<Integer> listFiles(String path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>();
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }

      if (inode.isFile()) {
        ret.add(inode.getId());
      } else if (recursive) {
        Queue<Inode> queue = new LinkedList<Inode>();
        queue.addAll(((InodeFolder) inode).getChildren());

        while (!queue.isEmpty()) {
          Inode qinode = queue.poll();
          if (qinode.isDirectory()) {
            queue.addAll(((InodeFolder) qinode).getChildren());
          } else {
            ret.add(qinode.getId());
          }
        }
      }
    }

    return ret;
  }

  /**
   * Load the image from <code>parser</code>, which is created based on the <code>path</code>.
   * Assume this blocks the whole MasterInfo.
   * 
   * @param parser
   *          the JsonParser to load the image
   * @param path
   *          the file to load the image
   * @throws IOException
   */
  public void loadImage(JsonParser parser, String path) throws IOException {
    while (true) {
      ImageElement ele;
      try {
        ele = parser.readValueAs(ImageElement.class);
        LOG.debug("Read Element: " + ele);
      } catch (IOException e) {
        // Unfortunately brittle, but Jackson rethrows EOF with this message.
        if (e.getMessage().contains("end-of-input")) {
          break;
        } else {
          throw e;
        }
      }

      switch (ele.type) {
      case Version: {
        if (ele.getInt("version") != Constants.JOURNAL_VERSION) {
          throw new IOException("Image " + path + " has journal version " + ele.getInt("version")
              + " . The system has verion " + Constants.JOURNAL_VERSION);
        }
        break;
      }
      case Checkpoint: {
        mInodeCounter.set(ele.getInt("inodeCounter"));
        mCheckpointInfo.updateEditTransactionCounter(ele.getLong("editTransactionCounter"));
        mCheckpointInfo.updateDependencyCounter(ele.getInt("dependencyCounter"));
        break;
      }
      case Dependency: {
        Dependency dep = Dependency.loadImage(ele);

        mDependencies.put(dep.ID, dep);
        if (!dep.hasCheckpointed()) {
          mUncheckpointedDependencies.add(dep.ID);
        }
        for (int parentDependencyId : dep.PARENT_DEPENDENCIES) {
          mDependencies.get(parentDependencyId).addChildrenDependency(dep.ID);
        }
        break;
      }
      case InodeFile: {
        // This element should not be loaded here. It should be loaded by InodeFolder.
        throw new IOException("Invalid element type " + ele);
      }
      case InodeFolder: {
        Inode inode = InodeFolder.loadImage(parser, ele);
        addToInodeMap(inode, mInodes);
        recomputePinnedFiles(inode, Optional.<Boolean> absent());

        if (inode.getId() != 1) {
          throw new IOException("Invalid element type " + ele);
        }
        mRoot = (InodeFolder) inode;

        break;
      }
      case RawTable: {
        mRawTables.loadImage(ele);
        break;
      }
      default:
        throw new IOException("Invalid element type " + ele);
      }
    }
  }

  /**
   * Get the names of the sub-directories at the given path.
   * 
   * @param path
   *          The path to look at
   * @param recursive
   *          If true, recursively add the paths of the sub-directories
   * @return the list of paths
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<String> ls(String path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return _ls(inode, path, recursive);
    }
  }

  /**
   * Create a directory at the given path.
   * 
   * @param path
   *          The path to create a directory at
   * @return true if and only if the directory was created; false otherwise
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws TachyonException
   */
  public boolean mkdir(String path) throws FileAlreadyExistException, InvalidPathException,
      TachyonException {
    try {
      return createFile(true, path, true, 0) > 0;
    } catch (BlockInfoException e) {
      throw new FileAlreadyExistException(e.getMessage());
    }
  }

  /**
   * Called by edit log only.
   * 
   * @param fileId
   * @param blockIndex
   * @param blockLength
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  void opAddBlock(int fileId, int blockIndex, long blockLength) throws FileDoesNotExistException,
      BlockInfoException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      addBlock((InodeFile) inode, new BlockInfo((InodeFile) inode, blockIndex, blockLength));
    }
  }

  /**
   * Register a worker at the given address, setting it up and associating it with a given list of
   * blocks.
   * 
   * @param workerNetAddress
   *          The address of the worker to register
   * @param totalBytes
   *          The capacity of the worker in bytes
   * @param usedBytes
   *          The number of bytes already used in the worker
   * @param currentBlockIds
   *          The id's of the blocks held by the worker
   * @return the new id of the registered worker
   * @throws BlockInfoException
   */
  public long registerWorker(NetAddress workerNetAddress, long totalBytes, long usedBytes,
      List<Long> currentBlockIds) throws BlockInfoException {
    long id = 0;
    InetSocketAddress workerAddress =
        new InetSocketAddress(workerNetAddress.mHost, workerNetAddress.mPort);
    LOG.info("registerWorker(): WorkerNetAddress: " + workerAddress);

    synchronized (mWorkers) {
      if (mWorkerAddressToId.containsKey(workerAddress)) {
        id = mWorkerAddressToId.get(workerAddress);
        mWorkerAddressToId.remove(workerAddress);
        LOG.warn("The worker " + workerAddress + " already exists as id " + id + ".");
      }
      if (id != 0 && mWorkers.containsKey(id)) {
        MasterWorkerInfo tWorkerInfo = mWorkers.get(id);
        mWorkers.remove(id);
        mLostWorkers.add(tWorkerInfo);
        LOG.warn("The worker with id " + id + " has been removed.");
      }
      id = START_TIME_NS_PREFIX + mWorkerCounter.incrementAndGet();
      MasterWorkerInfo tWorkerInfo = new MasterWorkerInfo(id, workerAddress, totalBytes);
      tWorkerInfo.updateUsedBytes(usedBytes);
      tWorkerInfo.updateBlocks(true, currentBlockIds);
      tWorkerInfo.updateLastUpdatedTimeMs();
      mWorkers.put(id, tWorkerInfo);
      mWorkerAddressToId.put(workerAddress, id);
      LOG.info("registerWorker(): " + tWorkerInfo);
    }

    synchronized (mRoot) {
      for (long blockId : currentBlockIds) {
        int fileId = BlockInfo.computeInodeId(blockId);
        int blockIndex = BlockInfo.computeBlockIndex(blockId);
        Inode inode = mInodes.get(fileId);
        if (inode != null && inode.isFile()) {
          ((InodeFile) inode).addLocation(blockIndex, id, workerNetAddress);
        } else {
          LOG.warn("registerWorker failed to add fileId " + fileId + " blockIndex " + blockIndex);
        }
      }
    }

    return id;
  }

  /**
   * Rename a file to the given path.
   * 
   * @param fileId
   *          The id of the file to rename
   * @param dstPath
   *          The new path of the file
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public boolean rename(int fileId, String dstPath) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      boolean ret = _rename(fileId, dstPath);
      mJournal.getEditLog().rename(fileId, dstPath);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  /**
   * Rename a file to the given path.
   * 
   * @param srcPath
   *          The path of the file to rename
   * @param dstPath
   *          The new path of the file
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public boolean rename(String srcPath, String dstPath) throws FileDoesNotExistException,
      InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(srcPath);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + srcPath + " does not exist");
      }
      return rename(inode.getId(), dstPath);
    }
  }

  /**
   * Logs a lost file and sets it to be recovered.
   * 
   * @param fileId
   *          The id of the file to be recovered
   */
  public void reportLostFile(int fileId) {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        LOG.warn("Tachyon does not have file " + fileId);
      } else if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory " + inode);
      } else {
        InodeFile iFile = (InodeFile) inode;
        int depId = iFile.getDependencyId();
        synchronized (mDependencies) {
          mLostFiles.add(fileId);
          if (depId == -1) {
            LOG.error("There is no dependency info for " + iFile + " . No recovery on that");
          } else {
            LOG.info("Reported file loss. Tachyon will recompute it: " + iFile.toString());

            Dependency dep = mDependencies.get(depId);
            dep.addLostFile(fileId);
            mMustRecomputeDependencies.add(depId);
          }
        }
      }
    }
  }

  /**
   * Request that the files for the given dependency be recomputed.
   * 
   * @param depId
   *          The dependency whose files are to be recomputed
   */
  public void requestFilesInDependency(int depId) {
    synchronized (mDependencies) {
      if (mDependencies.containsKey(depId)) {
        Dependency dep = mDependencies.get(depId);
        LOG.info("Request files in dependency " + dep);
        if (dep.hasLostFile()) {
          mMustRecomputeDependencies.add(depId);
        }
      } else {
        LOG.error("There is no dependency with id " + depId);
      }
    }
  }

  /**
   * Stops the heartbeat thread.
   */
  public void stop() {
    mHeartbeatThread.shutdown();
  }

  /**
   * Returns whether the traversal was successful or not.
   * 
   * @return true if the traversal was successful, or false otherwise.
   */
  private boolean traversalSucceeded(Pair<Inode, Integer> inodeTraversal) {
    return inodeTraversal.getSecond() == -1;
  }

  /**
   * Traverse to the inode at the given path.
   * 
   * @param pathNames
   *          The path to search for, broken into components
   * @return the inode of the file at the given path. If it was not able to traverse down the entire
   *         path, it will set the second field to the first path component it didn't find. It never
   *         returns null.
   * @throws InvalidPathException
   */
  private Pair<Inode, Integer> traverseToInode(String[] pathNames) throws InvalidPathException {
    synchronized (mRoot) {
      if (pathNames == null || pathNames.length == 0) {
        throw new InvalidPathException("passed-in pathNames is null or empty");
      }
      if (pathNames.length == 1) {
        if (pathNames[0].equals("")) {
          return new Pair<Inode, Integer>(mRoot, -1);
        } else {
          final String msg = "File name starts with " + pathNames[0];
          LOG.info("InvalidPathException: " + msg);
          throw new InvalidPathException(msg);
        }
      }

      Pair<Inode, Integer> ret = new Pair<Inode, Integer>(mRoot, -1);

      for (int k = 1; k < pathNames.length; k ++) {
        Inode next = ((InodeFolder) ret.getFirst()).getChild(pathNames[k]);
        if (next == null) {
          // The user might want to create the nonexistent directories, so we leave ret.getFirst()
          // as the last Inode taken. We set nonexistentInd to k, to indicate that the kth path
          // component was the first one that couldn't be found.
          ret.setSecond(k);
          break;
        }
        ret.setFirst(next);
        if (!ret.getFirst().isDirectory()) {
          // The inode can't have any children. If this is the last path component, we're good.
          // Otherwise, we can't traverse further, so we clean up and throw an exception.
          if (k == pathNames.length - 1) {
            break;
          } else {
            final String msg =
                "Traversal failed. Component " + k + "(" + ret.getFirst().getName()
                    + ") is a file";
            LOG.info("InvalidPathException: " + msg);
            throw new InvalidPathException(msg);
          }
        }
      }
      return ret;
    }
  }

  /** Sets the isPinned flag on the given inode and all of its children. */
  public void setPinned(int fileId, boolean pinned) throws FileDoesNotExistException {
    LOG.info("setPinned(" + fileId + ", " + pinned + ")");
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("Failed to find inode" + fileId);
      }

      recomputePinnedFiles(inode, Optional.of(pinned));

      mJournal.getEditLog().setPinned(fileId, pinned);
      mJournal.getEditLog().flush();
    }
  }

  /**
   * Update the metadata of a table.
   * 
   * @param tableId
   *          The id of the table to update
   * @param metadata
   *          The new metadata to update the table with
   * @throws TableDoesNotExistException
   * @throws TachyonException
   */
  public void updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(tableId);

      if (inode == null || !inode.isDirectory() || !mRawTables.exist(tableId)) {
        throw new TableDoesNotExistException("Table " + tableId + " does not exist.");
      }

      mRawTables.updateMetadata(tableId, metadata);

      mJournal.getEditLog().updateRawTableMetadata(tableId, metadata);
      mJournal.getEditLog().flush();
    }
  }

  /**
   * The heartbeat of the worker. It updates the information of the worker and removes the given
   * block id's.
   * 
   * @param workerId
   *          The id of the worker to deal with
   * @param usedBytes
   *          The number of bytes used in the worker
   * @param removedBlockIds
   *          The id's of the blocks that have been removed
   * @return a command specifying an action to take
   * @throws BlockInfoException
   */
  public Command workerHeartbeat(long workerId, long usedBytes, List<Long> removedBlockIds)
      throws BlockInfoException {
    LOG.debug("WorkerId: " + workerId);
    synchronized (mRoot) {
      synchronized (mWorkers) {
        MasterWorkerInfo tWorkerInfo = mWorkers.get(workerId);

        if (tWorkerInfo == null) {
          LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId
              + " . Send command to let it re-register.");
          return new Command(CommandType.Register, new ArrayList<Long>());
        }

        tWorkerInfo.updateUsedBytes(usedBytes);
        tWorkerInfo.updateBlocks(false, removedBlockIds);
        tWorkerInfo.updateToRemovedBlocks(false, removedBlockIds);
        tWorkerInfo.updateLastUpdatedTimeMs();

        for (long blockId : removedBlockIds) {
          int fileId = BlockInfo.computeInodeId(blockId);
          int blockIndex = BlockInfo.computeBlockIndex(blockId);
          Inode inode = mInodes.get(fileId);
          if (inode == null) {
            LOG.error("File " + fileId + " does not exist");
          } else if (inode.isFile()) {
            ((InodeFile) inode).removeLocation(blockIndex, workerId);
            LOG.debug("File " + fileId + " block " + blockIndex + " was evicted from worker "
                + workerId);
          }
        }

        List<Long> toRemovedBlocks = tWorkerInfo.getToRemovedBlocks();
        if (toRemovedBlocks.size() != 0) {
          return new Command(CommandType.Free, toRemovedBlocks);
        }
      }
    }

    return new Command(CommandType.Nothing, new ArrayList<Long>());
  }

  /**
   * Create an image of the dependencies and filesystem tree.
   * 
   * @param os
   *          The output stream to write the image to
   */
  @Override
  public void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.Version).withParameter("version",
            Constants.JOURNAL_VERSION);

    writeElement(objWriter, dos, ele);

    synchronized (mRoot) {
      synchronized (mDependencies) {
        for (Dependency dep : mDependencies.values()) {
          dep.writeImage(objWriter, dos);
        }
      }
      mRoot.writeImage(objWriter, dos);
      mRawTables.writeImage(objWriter, dos);

      ele =
          new ImageElement(ImageElementType.Checkpoint)
              .withParameter("inodeCounter", mInodeCounter.get())
              .withParameter("editTransactionCounter", mCheckpointInfo.getEditTransactionCounter())
              .withParameter("dependencyCounter", mCheckpointInfo.getDependencyCounter());

      writeElement(objWriter, dos, ele);
    }
  }
}
