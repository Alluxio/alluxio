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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
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
public class MasterInfo implements ImageWriter {
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
          CommonUtils.sleepMs(LOG, 1000);
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
  private Counters mCheckpointInfo = new Counters(0, 0, 0);

  private AtomicInteger mInodeCounter = new AtomicInteger(0);
  private AtomicInteger mDependencyCounter = new AtomicInteger(0);
  private AtomicInteger mRerunCounter = new AtomicInteger(0);

  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);
  // Root Inode's id must be 1.
  private InodeFolder mRoot;
  // A map from file ID's to Inodes. All operations on it are currently synchronized on mRoot.
  private Map<Integer, Inode> mInodes = new HashMap<Integer, Inode>();
  private Map<Integer, Dependency> mDependencies = new HashMap<Integer, Dependency>();
  private RawTables mRawTables = new RawTables();

  // TODO add initialization part for master failover or restart. All operations on these members
  // are synchronized on mDependencies.
  private Set<Integer> mUncheckpointedDependencies = new HashSet<Integer>();
  private Set<Integer> mPriorityDependencies = new HashSet<Integer>();
  private Set<Integer> mLostFiles = new HashSet<Integer>();

  private Set<Integer> mBeingRecomputedFiles = new HashSet<Integer>();
  private Set<Integer> mMustRecomputeDependencies = new HashSet<Integer>();
  private Map<Long, MasterWorkerInfo> mWorkers = new HashMap<Long, MasterWorkerInfo>();

  private Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();

  private BlockingQueue<MasterWorkerInfo> mLostWorkers = new ArrayBlockingQueue<MasterWorkerInfo>(
      32);

  // TODO Check the logic related to this two lists.
  private PrefixList mWhiteList;
  private PrefixList mPinList;
  private Set<Integer> mFileIdPinList;

  private Journal mJournal;

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
    mPinList = new PrefixList(MASTER_CONF.PINLIST);
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
   * @param id
   *          If not -1, use this id as the id to the inode we create at the given path.
   *          Any intermediate directories created will use mInodeCounter, so using this parameter
   *          assumes no intermediate directories will be created.
   * @return the id of the inode created at the given path
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws BlockInfoException
   * @throws TachyonException
   */
  int _createFile(boolean recursive, String path, boolean directory, long blockSizeByte,
      long creationTimeMs) throws FileAlreadyExistException, InvalidPathException,
      BlockInfoException, TachyonException {

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
        final String msg = "File " + path + " already exist.";
        LOG.info("FileAlreadyExistException: " + msg);
        throw new FileAlreadyExistException(msg);
      }
      if (directory) {
        ret =
            new InodeFolder(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                creationTimeMs);
      } else {
        ret =
            new InodeFile(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                blockSizeByte, creationTimeMs);
        if (mPinList.inList(path)) {
          synchronized (mFileIdPinList) {
            mFileIdPinList.add(ret.getId());
            ((InodeFile) ret).setPin(true);
          }
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
   * Inner delete function.
   * 
   * @param inode
   *          The inode to delete
   * @param recursive
   *          True if the file and it's subdirectories should be deleted
   * @return true if the deletion succeeded and false otherwise.
   */
  boolean _delete(int fileId, boolean recursive) throws TachyonException {
    Inode inode = mInodes.get(fileId);
    if (inode == null) {
      return true;
    }
    boolean succeed = true;
    synchronized (mRoot) {
      Set<Inode> delInodes = new HashSet<Inode>();
      if (inode.isDirectory()) {
        delInodes.addAll(getInodeChildrenRecursive((InodeFolder) inode));
      }
      delInodes.add(inode);

      if (inode.isDirectory() && !recursive && delInodes.size() > 1) {
        // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
        // true
        return false;
      }

      // We go through each inode, removing it from it's parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (Inode delInode : delInodes) {
        if (delInode.equals(mRoot)) {
          continue;
        }
        InodeFolder parent = (InodeFolder) mInodes.get(delInode.getParentId());
        parent.removeChild(delInode);
        if (delInode.isFile()) {
          String checkpointPath = ((InodeFile) delInode).getCheckpointPath();
          if (!checkpointPath.equals("")) {
            UnderFileSystem ufs = UnderFileSystem.get(checkpointPath);
            try {
              if (!ufs.delete(checkpointPath, true)) {
                succeed = false;
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

          if (((InodeFile) delInode).isPin()) {
            synchronized (mFileIdPinList) {
              mFileIdPinList.remove(delInode.getId());
            }
          }
        }

        if (mRawTables.exist(delInode.getId())) {
          succeed = succeed && mRawTables.delete(delInode.getId());
        }
      }

      for (Inode delInode : delInodes) {
        mInodes.remove(delInode.getId());
        delInode.reverseId();
      }

      return succeed;
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
        tFile.setCheckpointPath(checkpointPath);
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
   * Adds the given inode to mFileIdPinList, traversing folders recursively. Used while loading an
   * image.
   * 
   * @param inode
   *          The inode to add
   * @throws IOException
   */
  private void addToFileIdPinList(Inode inode) throws IOException {
    if (inode.isFile() && ((InodeFile) inode).isPin()) {
      synchronized (mFileIdPinList) {
        mFileIdPinList.add(inode.getId());
      }
    } else if (inode.isDirectory()) {
      for (Inode child : ((InodeFolder) inode).getChildren()) {
        addToFileIdPinList(child);
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
      tFile.addLocation(blockIndex, workerId,
          new NetAddress(address.getHostName(), address.getPort()));

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
   * @param recursive
   * @param path
   * @param directory
   * @param columns
   * @param metadata
   * @param blockSizeByte
   * @param creationTimeMs
   * @return
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
   * Same as {@link #getInode(String[] pathNames)} except that it takes a path string.
   */
  private Inode getInode(String path) throws InvalidPathException {
    return getInode(CommonUtils.getPathComponents(path));
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
      List<Integer> ret = new ArrayList<Integer>();
      for (int id : mFileIdPinList) {
        ret.add(id);
      }
      return ret;
    }
  }

  /**
   * Get the pin list.
   * 
   * @return the pin list
   */
  public List<String> getPinList() {
    return mPinList.getList();
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
          } else {
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
   * Load the image from <code>is</code>. Assume this blocks the whole MasterInfo.
   * 
   * @param is
   *          the inputstream to load the image.
   * @throws IOException
   */
  public void loadImage(DataInputStream is) throws IOException {
    while (true) {
      byte type = -1;
      try {
        type = is.readByte();
      } catch (EOFException e) {
        return;
      }

      if (type == Image.T_CHECKPOINT) {
        mInodeCounter.set(is.readInt());
        mCheckpointInfo.updateEditTransactionCounter(is.readLong());
        mCheckpointInfo.updateDependencyCounter(is.readInt());
      } else if (type == Image.T_DEPENDENCY) {
        Dependency dep = Dependency.loadImage(is);

        mDependencies.put(dep.ID, dep);
        if (!dep.hasCheckpointed()) {
          mUncheckpointedDependencies.add(dep.ID);
        }
        for (int parentDependencyId : dep.PARENT_DEPENDENCIES) {
          mDependencies.get(parentDependencyId).addChildrenDependency(dep.ID);
        }
      } else if (Image.T_INODE_FILE == type || Image.T_INODE_FOLDER == type) {
        Inode inode = null;

        if (type == Image.T_INODE_FILE) {
          inode = InodeFile.loadImage(is);
        } else {
          inode = InodeFolder.loadImage(is);
        }

        if (inode.getId() > mInodeCounter.get()) {
          mInodeCounter.set(inode.getId());
        }

        addToInodeMap(inode, mInodes);
        addToFileIdPinList(inode);

        if (inode.getId() == 1) {
          mRoot = (InodeFolder) inode;
        }
      } else if (Image.T_RAW_TABLE == type) {
        mRawTables.loadImage(is);
      } else {
        throw new IOException("Corrupted image with unknown element type: " + type);
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
   * Rename an inode to the given path.
   * 
   * @param srcInode
   *          The inode to rename
   * @param dstPath
   *          The new path of the inode
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  private void rename(Inode srcInode, String dstPath) throws FileAlreadyExistException,
      InvalidPathException, FileDoesNotExistException {
    if (getInode(dstPath) != null) {
      throw new FileAlreadyExistException("Failed to rename: " + dstPath + " already exist");
    }

    String dstName = CommonUtils.getName(dstPath);
    String dstFolderPath = dstPath.substring(0, dstPath.length() - dstName.length() - 1);

    // If we are renaming into the root folder
    if (dstFolderPath.isEmpty()) {
      dstFolderPath = Constants.PATH_SEPARATOR;
    }

    Inode dstFolderInode = getInode(dstFolderPath);
    if (dstFolderInode == null || dstFolderInode.isFile()) {
      throw new FileDoesNotExistException("Failed to rename: " + dstFolderPath
          + " does not exist.");
    }

    srcInode.setName(dstName);
    InodeFolder parent = (InodeFolder) mInodes.get(srcInode.getParentId());
    parent.removeChild(srcInode);
    srcInode.setParentId(dstFolderInode.getId());
    ((InodeFolder) dstFolderInode).addChild(srcInode);

    mJournal.getEditLog().rename(srcInode.getId(), dstPath);
    mJournal.getEditLog().flush();
  }

  /**
   * Rename a file to the given path.
   * 
   * @param fileId
   *          The id of the file to rename
   * @param dstPath
   *          The new path of the file
   * @throws FileDoesNotExistException
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   */
  public void rename(int fileId, String dstPath) throws FileDoesNotExistException,
      FileAlreadyExistException, InvalidPathException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + fileId + " does not exist");
      }

      rename(inode, dstPath);
    }
  }

  /**
   * Rename a file to the given path.
   * 
   * @param srcPath
   *          The path of the file to rename
   * @param dstPath
   *          The new path of the file
   */
  public void rename(String srcPath, String dstPath) throws FileAlreadyExistException,
      FileDoesNotExistException, InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(srcPath);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + srcPath + " does not exist");
      }

      rename(inode, dstPath);
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

  /**
   * Same as {@link #traverseToInode(String[] pathNames)} except that it takes a path
   * string.
   */
  private Pair<Inode, Integer> traverseToInode(String path) throws InvalidPathException {
    return traverseToInode(CommonUtils.getPathComponents(path));
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
   * Unpin the file with the given id.
   * 
   * @param fileId
   *          The id of the file to unpin
   * @throws FileDoesNotExistException
   */
  public void unpinFile(int fileId) throws FileDoesNotExistException {
    // TODO Change meta data only. Data will be evicted from worker based on data replacement
    // policy. TODO May change it to be active from V0.2
    LOG.info("unpinFile(" + fileId + ")");
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("Failed to unpin " + fileId);
      }

      ((InodeFile) inode).setPin(false);
      synchronized (mFileIdPinList) {
        mFileIdPinList.remove(fileId);
      }

      mJournal.getEditLog().unpinFile(fileId);
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
  public void writeImage(DataOutputStream os) throws IOException {
    synchronized (mRoot) {
      synchronized (mDependencies) {
        for (Dependency dep : mDependencies.values()) {
          dep.writeImage(os);
        }
      }
      mRoot.writeImage(os);
      mRawTables.writeImage(os);

      os.writeByte(Image.T_CHECKPOINT);
      os.writeInt(mInodeCounter.get());
      os.writeLong(mCheckpointInfo.getEditTransactionCounter());
      os.writeInt(mCheckpointInfo.getDependencyCounter());
    }
  }
}
