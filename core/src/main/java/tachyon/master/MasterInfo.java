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

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.UnderFileSystem.SpaceType;
import tachyon.conf.TachyonConf;
import tachyon.master.Inode.InodeType;
import tachyon.master.permission.Acl;
import tachyon.master.permission.AclEntry.AclPermission;
import tachyon.master.permission.AclUtil;
import tachyon.master.permission.FsPermissionChecker;
import tachyon.security.UserGroup;
import tachyon.security.authentication.TSetUserProcessor;
import tachyon.thrift.AccessControlException;
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
          int masterWorkerTimeoutMs =
              mTachyonConf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS, 10 * Constants.SECOND_MS);
          if (CommonUtils.getCurrentMs()
              - worker.getValue().getLastUpdatedTimeMs() > masterWorkerTimeoutMs) {
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
        synchronized (mRootLock) {
          synchronized (mFileIdToDependency) {
            try {
              for (long blockId : worker.getBlocks()) {
                int fileId = BlockInfo.computeInodeId(blockId);
                InodeFile tFile = (InodeFile) mFileIdToInodes.get(fileId);
                if (tFile != null) {
                  int blockIndex = BlockInfo.computeBlockIndex(blockId);
                  tFile.removeLocation(blockIndex, worker.getId());
                  if (!tFile.hasCheckpointed()
                      && tFile.getBlockLocations(blockIndex, mTachyonConf).size() == 0) {
                    LOG.info("Block " + blockId + " got lost from worker " + worker.getId() + " .");
                    int depId = tFile.getDependencyId();
                    if (depId == -1) {
                      LOG.error("Permanent Data loss: " + tFile);
                    } else {
                      mLostFiles.add(tFile.getId());
                      Dependency dep = mFileIdToDependency.get(depId);
                      dep.addLostFile(tFile.getId());
                      LOG.info("File " + tFile.getId() + " got lost from worker " + worker.getId()
                          + " . Trying to recompute it using dependency " + dep.mId);
                      String tmp = mTachyonConf.get(Constants.MASTER_TEMPORARY_FOLDER, "/tmp");
                      if (!getPath(tFile).toString().startsWith(tmp)) {
                        mMustRecomputedDpendencies.add(depId);
                      }
                    }
                  } else {
                    LOG.info("Block " + blockId + " only lost an in memory copy from worker "
                        + worker.getId());
                  }
                }
              }
            } catch (BlockInfoException e) {
              LOG.error(e.getMessage(), e);
            }
          }
        }
      }

      if (hadFailedWorker) {
        LOG.warn("Restarting failed workers.");
        try {
          String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
          java.lang.Runtime.getRuntime()
              .exec(tachyonHome + "/bin/tachyon-start.sh restart_workers");
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  public class RecomputationScheduler implements Runnable {
    @Override
    public void run() {
      Thread.currentThread().setName("recompute-scheduler");
      while (!Thread.currentThread().isInterrupted()) {
        boolean hasLostFiles = false;
        boolean launched = false;
        List<String> cmds = new ArrayList<String>();
        synchronized (mRootLock) {
          synchronized (mFileIdToDependency) {
            if (!mMustRecomputedDpendencies.isEmpty()) {
              List<Integer> recomputeList = new ArrayList<Integer>();
              Queue<Integer> checkQueue = new LinkedList<Integer>();

              checkQueue.addAll(mMustRecomputedDpendencies);
              while (!checkQueue.isEmpty()) {
                int depId = checkQueue.poll();
                Dependency dep = mFileIdToDependency.get(depId);
                boolean canLaunch = true;
                for (int k = 0; k < dep.mParentFiles.size(); k ++) {
                  int fildId = dep.mParentFiles.get(k);
                  if (mLostFiles.contains(fildId)) {
                    canLaunch = false;
                    InodeFile iFile = (InodeFile) mFileIdToInodes.get(fildId);
                    if (!mBeingRecomputedFiles.contains(fildId)) {
                      int tDepId = iFile.getDependencyId();
                      if (tDepId != -1 && !mMustRecomputedDpendencies.contains(tDepId)) {
                        mMustRecomputedDpendencies.add(tDepId);
                        checkQueue.add(tDepId);
                      }
                    }
                  }
                }
                if (canLaunch) {
                  recomputeList.add(depId);
                }
              }
              hasLostFiles = !mMustRecomputedDpendencies.isEmpty();
              launched = (recomputeList.size() > 0);

              for (int k = 0; k < recomputeList.size(); k ++) {
                mMustRecomputedDpendencies.remove(recomputeList.get(k));
                Dependency dep = mFileIdToDependency.get(recomputeList.get(k));
                mBeingRecomputedFiles.addAll(dep.getLostFiles());
                cmds.add(dep.getCommand());
              }
            }
          }
        }

        for (String cmd : cmds) {
          String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
          String filePath = tachyonHome + "/logs/rerun-" + mRerunCounter.incrementAndGet();
          // TODO use bounded threads (ExecutorService)
          Thread thread = new Thread(new RecomputeCommand(cmd, filePath));
          thread.setName("recompute-command-" + cmd);
          thread.start();
        }

        if (!launched) {
          if (hasLostFiles) {
            LOG.info("HasLostFiles, but no job can be launched.");
          }
          CommonUtils.sleepMs(LOG, Constants.SECOND_MS, true);
        }
      }
    }
  }

  public static final String COL = "COL_";

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final InetSocketAddress mMasterAddress;
  private final long mStartTimeNSPrefix;
  private final long mStartTimeMs;
  private final Counters mCheckpointInfo = new Counters(0, 0, 0);

  private final AtomicInteger mInodeCounter = new AtomicInteger(0);
  private final AtomicInteger mDependencyCounter = new AtomicInteger(0);
  private final AtomicInteger mRerunCounter = new AtomicInteger(0);

  private final AtomicInteger mUserCounter = new AtomicInteger(0);
  private final AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // Root Inode's id must be 1.
  private InodeFolder mRoot;
  private final Object mRootLock = new Object();

  // A map from file ID's to Inodes. All operations on it are currently synchronized on mRootLock.
  private final Map<Integer, Inode> mFileIdToInodes = new HashMap<Integer, Inode>();
  private final Map<Integer, Dependency> mFileIdToDependency = new HashMap<Integer, Dependency>();
  private final RawTables mRawTables;

  // TODO add initialization part for master failover or restart. All operations on these members
  // are synchronized on mFileIdToDependency.
  private final Set<Integer> mUncheckpointedDependencies = new HashSet<Integer>();
  private final Set<Integer> mPriorityDependencies = new HashSet<Integer>();
  private final Set<Integer> mLostFiles = new HashSet<Integer>();

  private final Set<Integer> mBeingRecomputedFiles = new HashSet<Integer>();
  private final Set<Integer> mMustRecomputedDpendencies = new HashSet<Integer>();
  private final Map<Long, MasterWorkerInfo> mWorkers = new HashMap<Long, MasterWorkerInfo>();

  private final Map<NetAddress, Long> mWorkerAddressToId = new HashMap<NetAddress, Long>();

  private final BlockingQueue<MasterWorkerInfo> mLostWorkers =
      new ArrayBlockingQueue<MasterWorkerInfo>(32);

  // TODO Check the logic related to this two lists.
  private final PrefixList mWhitelist;
  // Synchronized set containing all InodeFile ids that are currently pinned.
  private final Set<Integer> mPinnedInodeFileIds;

  private final Journal mJournal;

  private final ExecutorService mExecutorService;
  private Future<?> mHeartbeat;
  private Future<?> mRecompute;

  private final TachyonConf mTachyonConf;
  private final String mUFSDataFolder;
  private final UserGroup mFsOwner;
  private final String mSupergroup;
  private boolean mPermissionEnabled;

  public MasterInfo(InetSocketAddress address, Journal journal, ExecutorService executorService,
      TachyonConf tachyonConf) throws IOException {
    mExecutorService = executorService;
    mTachyonConf = tachyonConf;
    mUFSDataFolder = mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);

    mRawTables = new RawTables(mTachyonConf);

    mFsOwner = UserGroup.getTachyonLoginUser();
    mSupergroup = tachyonConf.get(Constants.FS_PERMISSIONS_SUPERGROUP,
        Constants.FS_PERMISSIONS_SUPERGROUP_DEFAULT);
    mPermissionEnabled = tachyonConf.getBoolean(Constants.FS_PERMISSIONS_ENABLED_KEY,
        Constants.FS_PERMISSIONS_ENABLED_DEFAULT);
    mRoot = new InodeFolder("", mInodeCounter.incrementAndGet(), -1,System.currentTimeMillis(),
        AclUtil.getAcl(mFsOwner.getShortUserName(), mSupergroup, mTachyonConf, InodeType.FOLDER));
    mFileIdToInodes.put(mRoot.getId(), mRoot);

    mMasterAddress = address;
    mStartTimeMs = System.currentTimeMillis();
    // TODO This name need to be changed.
    mStartTimeNSPrefix = mStartTimeMs - (mStartTimeMs % 1000000);
    mJournal = journal;

    mWhitelist =
        new PrefixList(mTachyonConf.getList(Constants.MASTER_WHITELIST, ",",
            new LinkedList<String>()));
    mPinnedInodeFileIds = Collections.synchronizedSet(new HashSet<Integer>());

    mJournal.loadImage(this);
  }

  private UserGroup getRemoteUser() {
    return TSetUserProcessor.getRemoteUser();
  }

  private FsPermissionChecker getPermissionChecker() throws AccessControlException {
    return new FsPermissionChecker(mFsOwner.getShortUserName(), mSupergroup,
        getRemoteUser());
  }

  /**
   * Add a checkpoint to a file, inner method.
   *
   * @param workerId The worker which submitted the request. -1 if the request is not from a worker.
   * @param fileId The file to add the checkpoint.
   * @param length The length of the checkpoint.
   * @param checkpointPath The path of the checkpoint.
   * @param opTimeMs The time of the operation, in milliseconds
   * @return the Pair of success and needLog
   * @throws FileNotFoundException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  Pair<Boolean, Boolean> _addCheckpoint(long workerId, int fileId, long length,
      TachyonURI checkpointPath, long opTimeMs) throws FileNotFoundException,
      SuspectedFileSizeException, BlockInfoException {
    LOG.info(CommonUtils.parametersToString(workerId, fileId, length, checkpointPath));

    if (workerId != -1) {
      MasterWorkerInfo tWorkerInfo = getWorkerInfo(workerId);
      tWorkerInfo.updateLastUpdatedTimeMs();
    }

    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

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
        tFile.setUfsPath(checkpointPath.toString());
        needLog = true;

        synchronized (mFileIdToDependency) {
          int depId = tFile.getDependencyId();
          if (depId != -1) {
            Dependency dep = mFileIdToDependency.get(depId);
            dep.childCheckpointed(tFile.getId());
            if (dep.hasCheckpointed()) {
              mUncheckpointedDependencies.remove(dep.mId);
              mPriorityDependencies.remove(dep.mId);
            }
          }
        }
      }
      addFile(fileId, tFile.getDependencyId());
      tFile.setComplete();

      if (needLog) {
        tFile.setLastModificationTimeMs(opTimeMs);
      }
      return new Pair<Boolean, Boolean>(true, needLog);
    }
  }

  /**
   * Completes the checkpointing of a file, inner method.
   *
   * @param fileId The id of the file
   * @param opTimeMs The time of the complete file operation, in milliseconds
   * @throws FileDoesNotExistException
   */
  void _completeFile(int fileId, long opTimeMs) throws FileDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exit.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File " + fileId + " is not a file.");
      }

      addFile(fileId, ((InodeFile) inode).getDependencyId());

      ((InodeFile) inode).setComplete();
      inode.setLastModificationTimeMs(opTimeMs);
    }
  }

  int _createDependency(List<Integer> parentsIds, List<Integer> childrenIds, String commandPrefix,
      List<ByteBuffer> data, String comment, String framework, String frameworkVersion,
      DependencyType dependencyType, int dependencyId, long creationTimeMs)
      throws InvalidPathException, FileDoesNotExistException {
    Dependency dep = null;
    synchronized (mRootLock) {
      Set<Integer> parentDependencyIds = new HashSet<Integer>();
      for (int k = 0; k < parentsIds.size(); k ++) {
        int parentId = parentsIds.get(k);
        Inode inode = mFileIdToInodes.get(parentId);
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
              framework, frameworkVersion, dependencyType, parentDependencyIds, creationTimeMs,
              mTachyonConf);

      List<Inode> childrenInodes = new ArrayList<Inode>();
      for (int k = 0; k < childrenIds.size(); k ++) {
        InodeFile inode = (InodeFile) mFileIdToInodes.get(childrenIds.get(k));
        inode.setDependencyId(dep.mId);
        inode.setLastModificationTimeMs(creationTimeMs);
        childrenInodes.add(inode);
        if (inode.hasCheckpointed()) {
          dep.childCheckpointed(inode.getId());
        }
      }
    }

    synchronized (mFileIdToDependency) {
      mFileIdToDependency.put(dep.mId, dep);
      if (!dep.hasCheckpointed()) {
        mUncheckpointedDependencies.add(dep.mId);
      }
      for (int parentDependencyId : dep.mParentDependencies) {
        mFileIdToDependency.get(parentDependencyId).addChildrenDependency(dep.mId);
      }
    }

    mJournal.getEditLog().createDependency(parentsIds, childrenIds, commandPrefix, data, comment,
        framework, frameworkVersion, dependencyType, dependencyId, creationTimeMs);
    mJournal.getEditLog().flush();

    LOG.info("Dependency created: " + dep);

    return dep.mId;
  }

  // TODO Make this API better.
  /**
   * Internal API.
   *
   * @param recursive If recursive is true and the filesystem tree is not filled in all the way to
   *        path yet, it fills in the missing components.
   * @param path The path to create
   * @param directory If true, creates an InodeFolder instead of an Inode
   * @param blockSizeByte If it's a file, the block size for the Inode
   * @param creationTimeMs The time the file was created
   * @param acl the acl of the inode
   * @return the id of the inode created at the given path
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws BlockInfoException
   * @throws TachyonException
   */
  int _createFile(boolean recursive, TachyonURI path, boolean directory, long blockSizeByte,
      long creationTimeMs, Acl acl) throws FileAlreadyExistException, InvalidPathException,
      BlockInfoException, AccessControlException, TachyonException {
    if (path.isRoot()) {
      LOG.info("FileAlreadyExistException: " + path);
      throw new FileAlreadyExistException(path.toString());
    }

    if (!directory && blockSizeByte < 1) {
      throw new BlockInfoException("Invalid block size " + blockSizeByte);
    }

    LOG.debug("createFile {}", CommonUtils.parametersToString(path));

    String[] pathNames = CommonUtils.getPathComponents(path.toString());
    String name = path.getName();

    String[] parentPath = new String[pathNames.length - 1];
    System.arraycopy(pathNames, 0, parentPath, 0, parentPath.length);

    synchronized (mRootLock) {
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

      /**
       * permission checker
       */
      checkAncestorAccess(path, AclPermission.WRITE);

      InodeFolder currentInodeFolder = (InodeFolder) inodeTraversal.getFirst();
      // Fill in the directories that were missing.
      for (int k = pathIndex; k < parentPath.length; k ++) {
        Inode dir =
            new InodeFolder(pathNames[k], mInodeCounter.incrementAndGet(),
                currentInodeFolder.getId(), creationTimeMs,
                AclUtil.getAcl(acl.getUserName(), acl.getGroupName(), mTachyonConf,
                InodeType.FOLDER));
        dir.setPinned(currentInodeFolder.isPinned());
        currentInodeFolder.addChild(dir);
        currentInodeFolder.setLastModificationTimeMs(creationTimeMs);
        mFileIdToInodes.put(dir.getId(), dir);
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
        throw new FileAlreadyExistException(path.toString());
      }

      if (directory) {
        ret =
            new InodeFolder(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                creationTimeMs, acl);
        ret.setPinned(currentInodeFolder.isPinned());
      } else {
        ret =
            new InodeFile(name, mInodeCounter.incrementAndGet(), currentInodeFolder.getId(),
                blockSizeByte, creationTimeMs, acl);
        ret.setPinned(currentInodeFolder.isPinned());
        if (ret.isPinned()) {
          mPinnedInodeFileIds.add(ret.getId());
        }
        if (mWhitelist.inList(path.toString())) {
          ((InodeFile) ret).setCache(true);
        }
      }

      mFileIdToInodes.put(ret.getId(), ret);
      currentInodeFolder.addChild(ret);
      currentInodeFolder.setLastModificationTimeMs(creationTimeMs);

      LOG.debug("createFile: File Created: {} parent: ", ret, currentInodeFolder);
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
   * @param fileId The inode to delete
   * @param recursive True if the file and it's subdirectories should be deleted
   * @param opTimeMs The time of the delete operation, in milliseconds
   * @return true if the deletion succeeded and false otherwise.
   * @throws TachyonException
   */
  boolean _delete(int fileId, boolean recursive, long opTimeMs)
      throws AccessControlException, TachyonException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
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

      checkPathAccess(getPath(inode), AclPermission.WRITE);

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

          mPinnedInodeFileIds.remove(delInode.getId());
        }

        InodeFolder parent = (InodeFolder) mFileIdToInodes.get(delInode.getParentId());
        parent.removeChild(delInode);
        parent.setLastModificationTimeMs(opTimeMs);

        if (mRawTables.exist(delInode.getId()) && !mRawTables.delete(delInode.getId())) {
          return false;
        }

        mFileIdToInodes.remove(delInode.getId());
        delInode.reverseId();
      }

      return true;
    }
  }

  /**
   * Get the raw table info associated with the given id.
   *
   * @param path The path of the table
   * @param inode The inode at the path
   * @return the table info
   * @throws TableDoesNotExistException
   */
  public ClientRawTableInfo _getClientRawTableInfo(TachyonURI path, Inode inode)
      throws TableDoesNotExistException {
    LOG.info("getClientRawTableInfo(" + path + ")");
    if (!mRawTables.exist(inode.getId())) {
      throw new TableDoesNotExistException("Table " + inode.getId() + " does not exist.");
    }
    ClientRawTableInfo ret = new ClientRawTableInfo();
    ret.id = inode.getId();
    ret.name = inode.getName();
    ret.path = path.toString();
    ret.columns = mRawTables.getColumns(ret.id);
    ret.metadata = mRawTables.getMetadata(ret.id);
    return ret;
  }

  /**
   * Get the names of the sub-directories at the given path.
   *
   * @param inode The inode to list
   * @param path The path of the given inode
   * @param recursive If true, recursively add the paths of the sub-directories
   * @return the list of paths
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  private List<TachyonURI> _ls(Inode inode, TachyonURI path, boolean recursive)
      throws InvalidPathException, FileDoesNotExistException {
    synchronized (mRootLock) {
      List<TachyonURI> ret = new ArrayList<TachyonURI>();
      ret.add(path);
      if (inode.isDirectory()) {
        for (Inode child : ((InodeFolder) inode).getChildren()) {
          TachyonURI childUri = path.join(child.getName());
          if (recursive) {
            ret.addAll(_ls(child, childUri, recursive));
          } else {
            ret.add(childUri);
          }
        }
      }
      return ret;
    }
  }

  /**
   * Inner method of recomputePinnedFiles. Also directly called by EditLog.
   *
   * @param inode The inode to start traversal from
   * @param setPinState An optional parameter indicating whether we should also set the "pinned"
   *        flag on each inode we traverse. If absent, the "isPinned" flag is unchanged.
   * @param opTimeMs The time of set pinned, in milliseconds
   */
  void _recomputePinnedFiles(Inode inode, Optional<Boolean> setPinState, long opTimeMs) {
    if (setPinState.isPresent()) {
      inode.setPinned(setPinState.get());
      inode.setLastModificationTimeMs(opTimeMs);
    }

    if (inode.isFile()) {
      if (inode.isPinned()) {
        mPinnedInodeFileIds.add(inode.getId());
      } else {
        mPinnedInodeFileIds.remove(inode.getId());
      }
    } else if (inode.isDirectory()) {
      for (Inode child : ((InodeFolder) inode).getChildren()) {
        _recomputePinnedFiles(child, setPinState, opTimeMs);
      }
    }
  }

  /**
   * Rename a file to the given path, inner method.
   *
   * @param fileId The id of the file to rename
   * @param dstPath The new path of the file
   * @param opTimeMs The time of the rename operation, in milliseconds
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException If the id doesn't point to an inode
   * @throws InvalidPathException if the source path is a prefix of the destination
   */
  public boolean _rename(int fileId, TachyonURI dstPath, long opTimeMs)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    synchronized (mRootLock) {
      TachyonURI srcPath = getPath(fileId);
      if (srcPath.equals(dstPath)) {
        return true;
      }
      if (srcPath.isRoot() || dstPath.isRoot()) {
        return false;
      }
      /*
       * To check 'w'permission or the existing src
       */
      checkPathAccess(srcPath, AclPermission.WRITE);
      /*
       * To check 'w' permission of parent for src and ancesstor dst directory
       */
      checkParentAccess(srcPath, AclPermission.WRITE);
      checkAncestorAccess(dstPath, AclPermission.WRITE);

      String[] srcComponents = CommonUtils.getPathComponents(srcPath.toString());
      String[] dstComponents = CommonUtils.getPathComponents(dstPath.toString());
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

      TachyonURI srcParent = srcPath.getParent();
      TachyonURI dstParent = dstPath.getParent();

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
      if (((InodeFolder) dstParentInode)
          .getChild(dstComponents[dstComponents.length - 1]) != null) {
        return false;
      }

      // Now we remove srcInode from it's parent and insert it into dstPath's parent
      ((InodeFolder) srcParentInode).removeChild(srcInode);
      srcParentInode.setLastModificationTimeMs(opTimeMs);
      srcInode.setParentId(dstParentInode.getId());
      srcInode.setName(dstComponents[dstComponents.length - 1]);
      ((InodeFolder) dstParentInode).addChild(srcInode);
      dstParentInode.setLastModificationTimeMs(opTimeMs);
      return true;
    }
  }

  void _setPinned(int fileId, boolean pinned, long opTimeMs) throws FileDoesNotExistException,
      AccessControlException {
    LOG.info("setPinned(" + fileId + ", " + pinned + ")");
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("Failed to find inode" + fileId);
      }

      checkOwner(getPath(inode));

      _recomputePinnedFiles(inode, Optional.of(pinned), opTimeMs);
    }
  }

  private void addBlock(InodeFile tFile, BlockInfo blockInfo, long opTimeMs)
      throws BlockInfoException {
    tFile.addBlock(blockInfo);
    tFile.setLastModificationTimeMs(opTimeMs);
    mJournal.getEditLog().addBlock(tFile.getId(), blockInfo.mBlockIndex, blockInfo.mLength,
        opTimeMs);
    mJournal.getEditLog().flush();
  }

  /**
   * Add a checkpoint to a file.
   *
   * @param workerId The worker which submitted the request. -1 if the request is not from a worker.
   * @param fileId The file to add the checkpoint.
   * @param length The length of the checkpoint.
   * @param checkpointPath The path of the checkpoint.
   * @return true if the checkpoint is added successfully, false if not.
   * @throws FileNotFoundException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public boolean addCheckpoint(long workerId, int fileId, long length, TachyonURI checkpointPath)
      throws FileNotFoundException, SuspectedFileSizeException, BlockInfoException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      Pair<Boolean, Boolean> ret =
          _addCheckpoint(workerId, fileId, length, checkpointPath, opTimeMs);
      if (ret.getSecond()) {
        mJournal.getEditLog().addCheckpoint(fileId, length, checkpointPath, opTimeMs);
        mJournal.getEditLog().flush();
      }
      return ret.getFirst();
    }
  }

  /**
   * Removes a checkpointed file from the set of lost or being-recomputed files if it's there
   *
   * @param fileId The file to examine
   */
  private void addFile(int fileId, int dependencyId) {
    synchronized (mFileIdToDependency) {
      if (mLostFiles.contains(fileId)) {
        mLostFiles.remove(fileId);
      }
      if (mBeingRecomputedFiles.contains(fileId)) {
        mBeingRecomputedFiles.remove(fileId);
      }
    }
  }

  /**
   * While loading an image, addToInodeMap will map the various ids to their inodes.
   *
   * @param inode The inode to add
   * @param map The map to add the inodes to
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
   * @param usedBytesOnTier
   * @param blockId
   * @param length
   * @return the dependency id of the file if it has not been checkpointed. -1 means the file either
   *         does not have dependency or has already been checkpointed.
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  public int cacheBlock(long workerId, long usedBytesOnTier, long storageDirId, long blockId,
      long length) throws FileDoesNotExistException, BlockInfoException {
    LOG.debug("Cache block: {}",
        CommonUtils.parametersToString(workerId, usedBytesOnTier, blockId, length));

    MasterWorkerInfo tWorkerInfo = getWorkerInfo(workerId);
    int storageLevelAliasValue = StorageDirId.getStorageLevelAliasValue(storageDirId);
    tWorkerInfo.updateBlock(true, blockId);
    tWorkerInfo.updateUsedBytes(storageLevelAliasValue, usedBytesOnTier);
    tWorkerInfo.updateLastUpdatedTimeMs();

    int fileId = BlockInfo.computeInodeId(blockId);
    int blockIndex = BlockInfo.computeBlockIndex(blockId);
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      InodeFile tFile = (InodeFile) inode;
      if (tFile.getNumberOfBlocks() <= blockIndex) {
        addBlock(tFile, new BlockInfo(tFile, blockIndex, length), System.currentTimeMillis());
      }

      tFile.addLocation(blockIndex, workerId, tWorkerInfo.mWorkerAddress, storageDirId);

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
   * @param fileId The id of the file
   * @throws FileDoesNotExistException
   */
  public void completeFile(int fileId) throws FileDoesNotExistException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      _completeFile(fileId, opTimeMs);
      mJournal.getEditLog().completeFile(fileId, opTimeMs);
      mJournal.getEditLog().flush();
    }
  }

  public int createDependency(List<TachyonURI> parents, List<TachyonURI> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, DependencyType dependencyType) throws InvalidPathException,
      FileDoesNotExistException {
    synchronized (mRootLock) {
      LOG.info("ParentList: " + CommonUtils.listToString(parents));
      List<Integer> parentsIdList = getFilesIds(parents);
      List<Integer> childrenIdList = getFilesIds(children);

      int depId = mDependencyCounter.incrementAndGet();
      long creationTimeMs = System.currentTimeMillis();
      int ret =
          _createDependency(parentsIdList, childrenIdList, commandPrefix, data, comment, framework,
              frameworkVersion, dependencyType, depId, creationTimeMs);

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
  public int createFile(boolean recursive, TachyonURI path, boolean directory, long blockSizeByte)
      throws FileAlreadyExistException, InvalidPathException, BlockInfoException,
      AccessControlException, TachyonException {
    long creationTimeMs = System.currentTimeMillis();
    Acl acl = AclUtil.getAcl(getRemoteUser().getShortUserName(),
        mSupergroup, mTachyonConf,
        directory ? InodeType.FOLDER : InodeType.FILE);
    synchronized (mRootLock) {
      int ret = _createFile(recursive, path, directory, blockSizeByte, creationTimeMs, acl);
      mJournal.getEditLog().createFile(recursive, path, directory, blockSizeByte,
          creationTimeMs, acl);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  public int createFile(TachyonURI path, long blockSizeByte) throws FileAlreadyExistException,
      InvalidPathException, BlockInfoException, AccessControlException, TachyonException {
    return createFile(true, path, false, blockSizeByte);
  }

  public int createFile(TachyonURI path, long blockSizeByte, boolean recursive)
      throws FileAlreadyExistException, InvalidPathException, BlockInfoException,
      AccessControlException, TachyonException {
    return createFile(recursive, path, false, blockSizeByte);
  }

  /**
   * Creates a new block for the given file.
   *
   * @param fileId The id of the file
   * @return the block id.
   * @throws FileDoesNotExistException
   */
  public long createNewBlock(int fileId) throws FileDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

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
   * @param path The path to place the table at
   * @param columns The number of columns in the table
   * @param metadata Additional metadata about the table
   * @return the file id of the table
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws TableColumnException
   * @throws TachyonException
   */
  public int createRawTable(TachyonURI path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      AccessControlException, TachyonException {
    LOG.info("createRawTable" + CommonUtils.parametersToString(path, columns));

    int maxColumns = mTachyonConf.getInt(Constants.MAX_COLUMNS, 1000);
    if (columns <= 0 || columns >= maxColumns) {
      throw new TableColumnException("Column " + columns + " should between 0 to " + maxColumns);
    }

    int id;
    try {
      id = createFile(true, path, true, 0);
      _createRawTable(id, columns, metadata);
    } catch (BlockInfoException e) {
      throw new FileAlreadyExistException(e.getMessage());
    }

    for (int k = 0; k < columns; k ++) {
      mkdirs(path.join(COL + k), true);
    }

    return id;
  }

  /**
   * Delete a file based on the file's ID.
   *
   * @param fileId the file to be deleted.
   * @param recursive whether delete the file recursively or not.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean delete(int fileId, boolean recursive)
      throws TachyonException, AccessControlException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      boolean ret = _delete(fileId, recursive, opTimeMs);
      mJournal.getEditLog().delete(fileId, recursive, opTimeMs);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  /**
   * Delete files based on the path.
   *
   * @param path The file to be deleted.
   * @param recursive whether delete the file recursively or not.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean delete(TachyonURI path, boolean recursive)
      throws TachyonException, AccessControlException {
    LOG.info("delete(" + path + ")");
    synchronized (mRootLock) {
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
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
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
   * @param path The file.
   * @return The list of the blocks of the file.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<BlockInfo> getBlockList(TachyonURI path) throws InvalidPathException,
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
   * @param blockId The id of the block return
   * @return the block info
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  public ClientBlockInfo getClientBlockInfo(long blockId) throws FileDoesNotExistException,
      BlockInfoException {
    int fileId = BlockInfo.computeInodeId(blockId);
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      ClientBlockInfo ret =
          ((InodeFile) inode)
              .getClientBlockInfo(BlockInfo.computeBlockIndex(blockId), mTachyonConf);
      LOG.debug("getClientBlockInfo: {} : {}", blockId, ret);
      return ret;
    }
  }

  /**
   * Get the dependency info associated with the given id.
   *
   * @param dependencyId The id of the dependency
   * @return the dependency info
   * @throws DependencyDoesNotExistException
   */
  public ClientDependencyInfo getClientDependencyInfo(int dependencyId)
      throws DependencyDoesNotExistException {
    Dependency dep = null;
    synchronized (mFileIdToDependency) {
      dep = mFileIdToDependency.get(dependencyId);
      if (dep == null) {
        throw new DependencyDoesNotExistException("No dependency with id " + dependencyId);
      }
    }
    return dep.generateClientDependencyInfo();
  }

  /**
   * Get the file info associated with the given id.
   *
   * @param fid The id of the file
   * @return the file info
   */
  public ClientFileInfo getClientFileInfo(int fid) throws AccessControlException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fid);
      if (inode == null) {
        ClientFileInfo info = new ClientFileInfo();
        info.id = -1;
        return info;
      }
      checkPathAccess(getPath(inode), AclPermission.READ);
      return inode.generateClientFileInfo(getPath(inode).toString());
    }
  }

  /**
   * Get the file info for the file at the given path
   *
   * @param path The path of the file
   * @return the file info
   * @throws InvalidPathException
   */
  public ClientFileInfo getClientFileInfo(TachyonURI path)
      throws AccessControlException, InvalidPathException {
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        ClientFileInfo info = new ClientFileInfo();
        info.id = -1;
        return info;
      }
      checkPathAccess(path, AclPermission.READ);
      return inode.generateClientFileInfo(path.toString());
    }
  }

  /**
   * Get the raw table info associated with the given id.
   *
   * @param id The id of the table
   * @return the table info
   * @throws TableDoesNotExistException
   */
  public ClientRawTableInfo getClientRawTableInfo(int id) throws TableDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(id);
      if (inode == null || !inode.isDirectory()) {
        throw new TableDoesNotExistException("Table " + id + " does not exist.");
      }
      return _getClientRawTableInfo(getPath(inode), inode);
    }
  }

  /**
   * Get the raw table info for the table at the given path
   *
   * @param path The path of the table
   * @return the table info
   * @throws TableDoesNotExistException
   * @throws InvalidPathException
   */
  public ClientRawTableInfo getClientRawTableInfo(TachyonURI path)
      throws TableDoesNotExistException, InvalidPathException {
    synchronized (mRootLock) {
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
   * @param path The path of the file
   * @return The file id of the file. -1 if the file does not exist.
   * @throws InvalidPathException
   */
  public int getFileId(TachyonURI path) throws InvalidPathException {
    Inode inode = getInode(path);
    int ret = -1;
    if (inode != null) {
      ret = inode.getId();
    }
    LOG.debug("getFileId({}): {}", path, ret);
    return ret;
  }

  /**
   * Get the block infos of a file with the given id. Throws an exception if the id names a
   * directory.
   *
   * @param fileId The id of the file to look up
   * @return the block infos of the file
   * @throws FileDoesNotExistException
   */
  public List<ClientBlockInfo> getFileBlocks(int fileId) throws FileDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      List<ClientBlockInfo> ret = ((InodeFile) inode).getClientBlockInfos(mTachyonConf);
      LOG.debug("getFileLocations: {} {}", fileId, ret);
      return ret;
    }
  }

  /**
   * Get the block infos of a file with the given path. Throws an exception if the path names a
   * directory.
   *
   * @param path The path of the file to look up
   * @return the block infos of the file
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public List<ClientBlockInfo> getFileBlocks(TachyonURI path) throws FileDoesNotExistException,
      InvalidPathException {
    LOG.info("getFileLocations: " + path);
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path.toString());
      }
      return getFileBlocks(inode.getId());
    }
  }

  /**
   * Get the file id's of the given paths. It recursively scans directories for the file id's inside
   * of them.
   *
   * @param pathList The list of paths to look at
   * @return the file id's of the files.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  private List<Integer> getFilesIds(List<TachyonURI> pathList) throws InvalidPathException,
      FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>(pathList.size());
    for (int k = 0; k < pathList.size(); k ++) {
      ret.addAll(listFiles(pathList.get(k), true));
    }
    return ret;
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its ClientFileInfo.
   *
   * @param path the target directory/file path
   * @return A list of ClientFileInfo
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public List<ClientFileInfo> getFilesInfo(TachyonURI path) throws FileDoesNotExistException,
      InvalidPathException, AccessControlException {
    List<ClientFileInfo> ret = new ArrayList<ClientFileInfo>();
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path.toString());
      }

      checkPathAccess(path, AclPermission.READ);

      if (inode.isDirectory()) {
        for (Inode child : ((InodeFolder) inode).getChildren()) {
          ret.add(child.generateClientFileInfo(CommonUtils.concat(path, child.getName())));
        }
      } else {
        ret.add(inode.generateClientFileInfo(path.toString()));
      }
    }
    return ret;
  }

  /**
   * @return the total bytes on each storage tier.
   */
  public List<Long> getTotalBytesOnTiers() {
    List<Long> ret = new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        for (int i = 0; i < worker.getTotalBytesOnTiers().size(); i ++) {
          ret.set(i, ret.get(i) + worker.getTotalBytesOnTiers().get(i));
        }
      }
    }
    return ret;
  }

  /**
   * @return the used bytes on each storage tier.
   */
  public List<Long> getUsedBytesOnTiers() {
    List<Long> ret = new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        for (int i = 0; i < worker.getUsedBytesOnTiers().size(); i ++) {
          ret.set(i, ret.get(i) + worker.getUsedBytesOnTiers().get(i));
        }
      }
    }
    return ret;
  }

  /**
   * Get absolute paths of all in memory files.
   *
   * @return absolute paths of all in memory files.
   */
  public List<TachyonURI> getInMemoryFiles() {
    List<TachyonURI> ret = new ArrayList<TachyonURI>();
    LOG.info("getInMemoryFiles()");
    Queue<Pair<InodeFolder, TachyonURI>> nodesQueue =
        new LinkedList<Pair<InodeFolder, TachyonURI>>();
    synchronized (mRootLock) {
      // TODO: Verify we want to use absolute path.
      nodesQueue
          .add(new Pair<InodeFolder, TachyonURI>(mRoot, new TachyonURI(TachyonURI.SEPARATOR)));
      while (!nodesQueue.isEmpty()) {
        Pair<InodeFolder, TachyonURI> tPair = nodesQueue.poll();
        InodeFolder tFolder = tPair.getFirst();
        TachyonURI curUri = tPair.getSecond();

        Set<Inode> children = tFolder.getChildren();
        for (Inode tInode : children) {
          TachyonURI newUri = curUri.join(tInode.getName());
          if (tInode.isDirectory()) {
            nodesQueue.add(new Pair<InodeFolder, TachyonURI>((InodeFolder) tInode, newUri));
          } else if (((InodeFile) tInode).isFullyInMemory()) {
            ret.add(newUri);
          }
        }
      }
    }
    return ret;
  }

  /**
   * Same as {@link #getInode(String[] pathNames)} except that it takes a path string.
   */
  private Inode getInode(TachyonURI path) throws InvalidPathException {
    return getInode(CommonUtils.getPathComponents(path.toString()));
  }

  /**
   * Get the inode of the file at the given path.
   *
   * @param pathNames The path components of the path to search for
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
   * @param inodeFolder The folder to start looking at
   * @return a list of the children inodes.
   */
  private List<Inode> getInodeChildrenRecursive(InodeFolder inodeFolder) {
    synchronized (mRootLock) {
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
    return mMasterAddress;
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
   * @param path The path to look at
   * @return The number of files at the path. Returns 1 if the path specifies a file. If it's a
   *         directory, returns the number of items in the directory.
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public int getNumberOfFiles(TachyonURI path) throws InvalidPathException,
      FileDoesNotExistException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new FileDoesNotExistException(path.toString());
    }
    if (inode.isFile()) {
      return 1;
    }
    return ((InodeFolder) inode).getNumberOfChildren();
  }

  /**
   * Get the path specified by a given inode.
   *
   * @param inode The inode
   * @return the path of the inode
   */
  private TachyonURI getPath(Inode inode) {
    synchronized (mRootLock) {
      if (inode.getId() == 1) {
        return new TachyonURI(TachyonURI.SEPARATOR);
      }
      if (inode.getParentId() == 1) {
        return new TachyonURI(TachyonURI.SEPARATOR + inode.getName());
      }
      return getPath(mFileIdToInodes.get(inode.getParentId())).join(inode.getName());
    }
  }

  /**
   * Get the path of a file with the given id
   *
   * @param fileId The id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist.
   */
  public TachyonURI getPath(int fileId) throws FileDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
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
    synchronized (mPinnedInodeFileIds) {
      return Lists.newArrayList(mPinnedInodeFileIds);
    }
  }

  /**
   * Creates a list of high priority dependencies, which don't yet have checkpoints.
   *
   * @return the list of dependency ids
   */
  public List<Integer> getPriorityDependencyList() {
    synchronized (mFileIdToDependency) {
      int earliestDepId = -1;
      if (mPriorityDependencies.isEmpty()) {
        long earliest = Long.MAX_VALUE;
        for (int depId : mUncheckpointedDependencies) {
          Dependency dep = mFileIdToDependency.get(depId);
          if (!dep.hasChildrenDependency()) {
            mPriorityDependencies.add(dep.mId);
          }

          if (dep.mCreationTimeMs < earliest) {
            earliest = dep.mCreationTimeMs;
            earliestDepId = dep.mId;
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
   * @param path The path of the table
   * @return the id of the table
   * @throws InvalidPathException
   * @throws TableDoesNotExistException
   */
  public int getRawTableId(TachyonURI path) throws InvalidPathException,
      TableDoesNotExistException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new TableDoesNotExistException(path.toString());
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
    return mStartTimeMs;
  }

  /**
   * Get the capacity of the under file system.
   *
   * @return the capacity in bytes
   * @throws IOException
   */
  public long getUnderFsCapacityBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_TOTAL);
  }

  /**
   * Get the amount of free space in the under file system.
   *
   * @return the free space in bytes
   * @throws IOException
   */
  public long getUnderFsFreeBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_FREE);
  }

  /**
   * Get the amount of space used in the under file system.
   *
   * @return the space used in bytes
   * @throws IOException
   */
  public long getUnderFsUsedBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(mUFSDataFolder, mTachyonConf);
    return ufs.getSpace(mUFSDataFolder, SpaceType.SPACE_USED);
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
    return mWhitelist.getList();
  }

  /**
   * Get the address of a worker.
   *
   * @param random If true, select a random worker
   * @param host If <code>random</code> is false, select a worker on this host
   * @return the address of the selected worker, or null if no address could be found
   */
  public NetAddress getWorker(boolean random, String host) throws UnknownHostException {
    synchronized (mWorkers) {
      if (mWorkerAddressToId.isEmpty()) {
        return null;
      }
      if (random) {
        int index = new Random(mWorkerAddressToId.size()).nextInt(mWorkerAddressToId.size());
        for (NetAddress address : mWorkerAddressToId.keySet()) {
          if (index == 0) {
            LOG.debug("getRandomWorker: {}", address);
            return address;
          }
          index --;
        }
        for (NetAddress address : mWorkerAddressToId.keySet()) {
          LOG.debug("getRandomWorker: {}", address);
          return address;
        }
      } else {
        for (NetAddress address : mWorkerAddressToId.keySet()) {
          InetAddress inetAddress = InetAddress.getByName(address.getMHost());
          if (inetAddress.getHostName().equals(host) || inetAddress.getHostAddress().equals(host)
              || inetAddress.getCanonicalHostName().equals(host)) {
            LOG.debug("getLocalWorker: {}" + address);
            return address;
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
   * @param workerId The id of the worker to look at
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

  /**
   * Get info about the lost workers
   *
   * @return a list of worker info
   */
  public List<ClientWorkerInfo> getLostWorkersInfo() {
    List<ClientWorkerInfo> ret = new ArrayList<ClientWorkerInfo>();

    for (MasterWorkerInfo worker : mLostWorkers) {
      ret.add(worker.generateClientWorkerInfo());
    }

    return ret;
  }

  public void init() throws IOException {
    synchronized (mRoot) {
      boolean originPermissionEnable = mPermissionEnabled;
      mPermissionEnabled = false;//turn off permission check for editlog loading
      mCheckpointInfo.updateEditTransactionCounter(mJournal.loadEditLog(this));
      mJournal.createImage(this);
      mJournal.createEditLog(mCheckpointInfo.getEditTransactionCounter());
      mPermissionEnabled = originPermissionEnable;
    }
    mHeartbeat =
        mExecutorService.submit(new HeartbeatThread("Master Heartbeat",
            new MasterInfoHeartbeatExecutor(), mTachyonConf.getInt(
                Constants.MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS)));

    mRecompute = mExecutorService.submit(new RecomputationScheduler());
  }

  /**
   * Get the id of the file at the given path. If recursive, it scans the subdirectories as well.
   *
   * @param path The path to start looking at
   * @param recursive If true, recursively scan the subdirectories at the given path as well
   * @return the list of the inode id's at the path
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<Integer> listFiles(TachyonURI path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>();
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path.toString());
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
      } else {
        for (Inode child : ((InodeFolder) inode).getChildren()) {
          ret.add(child.getId());
        }
      }
    }

    return ret;
  }

  /**
   * Load the image from <code>parser</code>, which is created based on the <code>path</code>.
   * Assume this blocks the whole MasterInfo.
   *
   * @param parser the JsonParser to load the image
   * @param path the file to load the image
   * @throws IOException
   */
  public void loadImage(JsonParser parser, TachyonURI path) throws IOException {
    while (true) {
      ImageElement ele;
      try {
        ele = parser.readValueAs(ImageElement.class);
        LOG.debug("Read Element: {}", ele);
      } catch (IOException e) {
        // Unfortunately brittle, but Jackson rethrows EOF with this message.
        if (e.getMessage().contains("end-of-input")) {
          break;
        } else {
          throw e;
        }
      }

      switch (ele.mType) {
        case Version: {
          if (ele.getInt("version") != Constants.JOURNAL_VERSION) {
            throw new IOException("Image " + path + " has journal version " + ele.getInt("version")
                + ". The system has version " + Constants.JOURNAL_VERSION);
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
          Dependency dep = Dependency.loadImage(ele, mTachyonConf);

          mFileIdToDependency.put(dep.mId, dep);
          if (!dep.hasCheckpointed()) {
            mUncheckpointedDependencies.add(dep.mId);
          }
          for (int parentDependencyId : dep.mParentDependencies) {
            mFileIdToDependency.get(parentDependencyId).addChildrenDependency(dep.mId);
          }
          break;
        }
        case InodeFile: {
          // This element should not be loaded here. It should be loaded by InodeFolder.
          throw new IOException("Invalid element type " + ele);
        }
        case InodeFolder: {
          Inode inode = InodeFolder.loadImage(parser, ele);
          addToInodeMap(inode, mFileIdToInodes);
          recomputePinnedFiles(inode, Optional.<Boolean>absent());

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
   * @param path The path to look at
   * @param recursive If true, recursively add the paths of the sub-directories
   * @return the list of paths
   * @throws InvalidPathException
   * @throws FileDoesNotExistException
   */
  public List<TachyonURI> ls(TachyonURI path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path.toString());
      }
      return _ls(inode, path, recursive);
    }
  }

  /**
   * Create a directory at the given path.
   *
   * @param path The path to create a directory at
   * @return true if and only if the directory was created; false otherwise
   * @throws FileAlreadyExistException
   * @throws InvalidPathException
   * @throws TachyonException
   */
  public boolean mkdirs(TachyonURI path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException, AccessControlException, TachyonException {
    try {
      return createFile(recursive, path, true, 0) > 0;
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
   * @param opTimeMs
   * @throws FileDoesNotExistException
   * @throws BlockInfoException
   */
  void opAddBlock(int fileId, int blockIndex, long blockLength, long opTimeMs)
      throws FileDoesNotExistException, BlockInfoException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      addBlock((InodeFile) inode, new BlockInfo((InodeFile) inode, blockIndex, blockLength),
          opTimeMs);
    }
  }

  /**
   * Recomputes mFileIdPinList at the given Inode, recursively recomputing for children. Optionally
   * will set the "pinned" flag as we go.
   *
   * @param inode The inode to start traversal from
   * @param setPinState An optional parameter indicating whether we should also set the "pinned"
   *        flag on each inode we traverse. If absent, the "isPinned" flag is unchanged.
   */
  private void recomputePinnedFiles(Inode inode, Optional<Boolean> setPinState) {
    long opTimeMs = System.currentTimeMillis();
    _recomputePinnedFiles(inode, setPinState, opTimeMs);
  }

  /**
   * Register a worker at the given address, setting it up and associating it with a given list of
   * blocks.
   *
   * @param workerNetAddress The address of the worker to register
   * @param totalBytesOnTiers Total bytes on each storage tier
   * @param usedBytesOnTiers Used Bytes on each storage tier
   * @param currentBlockIds Mapping from id of the StorageDir to id list of the blocks
   * @return the new id of the registered worker
   * @throws BlockInfoException
   */
  public long registerWorker(NetAddress workerNetAddress, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlockIds)
      throws BlockInfoException {
    long id = 0;
    long capacityBytes = 0;
    NetAddress workerAddress = new NetAddress(workerNetAddress);
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
      id = mStartTimeNSPrefix + mWorkerCounter.incrementAndGet();
      for (long b : totalBytesOnTiers) {
        capacityBytes += b;
      }
      MasterWorkerInfo tWorkerInfo =
          new MasterWorkerInfo(id, workerAddress, totalBytesOnTiers, capacityBytes);
      tWorkerInfo.updateUsedBytes(usedBytesOnTiers);
      for (List<Long> blockIds : currentBlockIds.values()) {
        tWorkerInfo.updateBlocks(true, blockIds);
      }
      tWorkerInfo.updateLastUpdatedTimeMs();
      mWorkers.put(id, tWorkerInfo);
      mWorkerAddressToId.put(workerAddress, id);
      LOG.info("registerWorker(): " + tWorkerInfo);
    }

    synchronized (mRootLock) {
      for (Entry<Long, List<Long>> blockIds : currentBlockIds.entrySet()) {
        long storageDirId = blockIds.getKey();
        for (long blockId : blockIds.getValue()) {
          int fileId = BlockInfo.computeInodeId(blockId);
          int blockIndex = BlockInfo.computeBlockIndex(blockId);
          Inode inode = mFileIdToInodes.get(fileId);
          if (inode != null && inode.isFile()) {
            ((InodeFile) inode).addLocation(blockIndex, id, workerAddress, storageDirId);
          } else {
            LOG.warn("registerWorker failed to add fileId " + fileId + " blockIndex " + blockIndex);
          }
        }
      }
    }

    return id;
  }

  /**
   * Rename a file to the given path.
   *
   * @param fileId The id of the file to rename
   * @param dstPath The new path of the file
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public boolean rename(int fileId, TachyonURI dstPath) throws FileDoesNotExistException,
      FileAlreadyExistException, AccessControlException, InvalidPathException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      boolean ret = _rename(fileId, dstPath, opTimeMs);
      mJournal.getEditLog().rename(fileId, dstPath, opTimeMs);
      mJournal.getEditLog().flush();
      return ret;
    }
  }

  /**
   * Rename a file to the given path.
   *
   * @param srcPath The path of the file to rename
   * @param dstPath The new path of the file
   * @return true if the rename succeeded, false otherwise
   * @throws FileDoesNotExistException
   * @throws InvalidPathException
   */
  public boolean rename(TachyonURI srcPath, TachyonURI dstPath) throws FileDoesNotExistException,
      FileAlreadyExistException, AccessControlException, InvalidPathException {
    synchronized (mRootLock) {
      Inode inode = getInode(srcPath);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + srcPath + " does not exist");
      }
      return rename(inode.getId(), dstPath);
    }
  }

  /**
   * Set owner of a path based on the file's ID.
   * @param fileId The id of the file to set
   * @param username
   * @param groupname
   * @param recursive If fileId or path represents a folder, change the folder owner recursively
   * @return true if the set succeeded, false otherwise
   * @throws TachyonException
   */
  public boolean setOwner(int fileId,String username, String groupname,
      boolean recursive) throws FileDoesNotExistException, AccessControlException,
      TachyonException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      if (recursive) {
        if (!inode.isDirectory()) {
          throw new AccessControlException("Failed to recursive setOwner on a file: "
                                           + getPath(inode));
        }
        Queue<Inode> queue = new LinkedList<Inode>();
        queue.addAll(((InodeFolder) inode).getChildren());

        while (!queue.isEmpty()) {
          Inode qinode = queue.poll();
          if (qinode.isDirectory()) {
            queue.addAll(((InodeFolder) qinode).getChildren());
          }
          _setOwner(qinode, username, groupname);
        }
      }
      _setOwner(inode, username, groupname);
    }
    mJournal.getEditLog().chown(fileId, username, groupname, recursive, opTimeMs);
    mJournal.getEditLog().flush();
    return true;
  }

  /**
   * Set owner of a path based on the path
   */
  public boolean setOwner(TachyonURI path, String username, String groupname,
      boolean recursive) throws FileDoesNotExistException, InvalidPathException,
      AccessControlException, TachyonException {
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to setOwner: " + path + " does not exist");
      }
      return setOwner(inode.getId(), username, groupname, recursive);
    }
  }

  public void _setOwner(Inode inode, String username, String groupname)
      throws AccessControlException {
    checkOwner(getPath(inode));
    Acl newAcl = new Acl(inode.getAcl());
    if (username != null) {
      newAcl.setUserOwner(username);
    }
    if (groupname != null) {
      newAcl.setGroupOwner(groupname);
    }
    inode.setAcl(newAcl);
  }

  /**
   * Set permission based on fileId
   * @param fileId The id of the file / folder.
   * @param short permission, e.g. 777
   * @param recursive If fileId represents a folder, change the folder permission recursively
   * @return true if setPermission successfully, false otherwise.
   * @throws IOException
   */
  public boolean setPermission(int fileId, short permission,boolean recursive)
      throws FileDoesNotExistException, AccessControlException, TachyonException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      if (recursive) {
        if (!inode.isDirectory()) {
          throw new AccessControlException("Failed to recursive setPermission on a file: "
                                           + getPath(inode));
        }
        Queue<Inode> queue = new LinkedList<Inode>();
        queue.addAll(((InodeFolder) inode).getChildren());

        while (!queue.isEmpty()) {
          Inode qinode = queue.poll();
          if (qinode.isDirectory()) {
            queue.addAll(((InodeFolder) qinode).getChildren());
          }
          _setPermission(qinode, permission);
        }
      }
      _setPermission(inode, permission);
    }
    mJournal.getEditLog().chmod(fileId, permission, recursive, opTimeMs);
    mJournal.getEditLog().flush();
    return true;
  }

  /**
   * Set permission based on path
   * @param path The path of the file
   * @param short permission, e.g. 777
   * @param recursive If path represents a folder, change the folder permission recursively
   * @return true if setPermission successfully, false otherwise.
   * @throws IOException
   */
  public boolean setPermission(TachyonURI path, short permission,boolean recursive)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      TachyonException {
    synchronized (mRootLock) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to setPermission: " + path + " does not exist");
      }
      return setPermission(inode.getId(), permission, recursive);
    }
  }

  public void _setPermission(Inode inode, short permission) throws AccessControlException {
    checkOwner(getPath(inode));
    Acl newAcl = new Acl(inode.getAcl());
    newAcl.setPermission(permission);
    inode.setAcl(newAcl);
  }
  /**
   * Logs a lost file and sets it to be recovered.
   *
   * @param fileId The id of the file to be recovered
   */
  public void reportLostFile(int fileId) {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null) {
        LOG.warn("Tachyon does not have file " + fileId);
      } else if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory " + inode);
      } else {
        InodeFile iFile = (InodeFile) inode;
        int depId = iFile.getDependencyId();
        synchronized (mFileIdToDependency) {
          mLostFiles.add(fileId);
          if (depId == -1) {
            LOG.error("There is no dependency info for " + iFile + " . No recovery on that");
          } else {
            LOG.info("Reported file loss. Tachyon will recompute it: " + iFile.toString());

            Dependency dep = mFileIdToDependency.get(depId);
            dep.addLostFile(fileId);
            mMustRecomputedDpendencies.add(depId);
          }
        }
      }
    }
  }

  /**
   * Request that the files for the given dependency be recomputed.
   *
   * @param depId The dependency whose files are to be recomputed
   */
  public void requestFilesInDependency(int depId) {
    synchronized (mFileIdToDependency) {
      if (mFileIdToDependency.containsKey(depId)) {
        Dependency dep = mFileIdToDependency.get(depId);
        LOG.info("Request files in dependency " + dep);
        if (dep.hasLostFile()) {
          mMustRecomputedDpendencies.add(depId);
        }
      } else {
        LOG.error("There is no dependency with id " + depId);
      }
    }
  }

  /** Sets the isPinned flag on the given inode and all of its children. */
  public void setPinned(int fileId, boolean pinned) throws AccessControlException,
      FileDoesNotExistException {
    long opTimeMs = System.currentTimeMillis();
    synchronized (mRootLock) {
      _setPinned(fileId, pinned, opTimeMs);
      mJournal.getEditLog().setPinned(fileId, pinned, opTimeMs);
      mJournal.getEditLog().flush();
    }
  }

  /**
   * Free the file/folder based on the files' ID
   *
   * @param fileId the file/folder to be freed.
   * @param recursive whether free the folder recursively or not
   * @return succeed or not
   * @throws TachyonException
   */
  boolean freepath(int fileId, boolean recursive) throws TachyonException {
    LOG.info("free(" + fileId + ")");
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(fileId);
      if (inode == null) {
        LOG.error("File " + fileId + " does not exist");
        return true;
      }

      if (inode.isDirectory() && !recursive && ((InodeFolder) inode).getNumberOfChildren() > 0) {
        // inode is nonempty, and we don't want to free a nonempty directory unless recursive is
        // true
        return false;
      }

      if (inode.getId() == mRoot.getId()) {
        // The root cannot be freed.
        return false;
      }

      List<Inode> freeInodes = new ArrayList<Inode>();
      freeInodes.add(inode);
      if (inode.isDirectory()) {
        freeInodes.addAll(getInodeChildrenRecursive((InodeFolder) inode));
      }

      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i --) {
        Inode freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          List<Pair<Long, Long>> blockIdWorkerIdList =
              ((InodeFile) freeInode).getBlockIdWorkerIdPairs();
          synchronized (mWorkers) {
            for (Pair<Long, Long> blockIdWorkerId : blockIdWorkerIdList) {
              MasterWorkerInfo workerInfo = mWorkers.get(blockIdWorkerId.getSecond());
              if (workerInfo != null) {
                workerInfo.updateToRemovedBlock(true, blockIdWorkerId.getFirst());
              }
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * Frees files based on the path
   *
   * @param path The file to be freed.
   * @param recursive whether delete the file recursively or not.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean freepath(TachyonURI path, boolean recursive) throws TachyonException {
    LOG.info("free(" + path + ")");
    synchronized (mRootLock) {
      Inode inode = null;
      try {
        inode = getInode(path);
      } catch (InvalidPathException e) {
        return false;
      }
      if (inode == null) {
        return true;
      }
      return freepath(inode.getId(), recursive);
    }
  }

  /**
   * Stops the heartbeat thread.
   */
  public void stop() {
    if (mHeartbeat != null) {
      mHeartbeat.cancel(true);
    }
    if (mRecompute != null) {
      mRecompute.cancel(true);
    }
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
   * @param pathNames The path to search for, broken into components
   * @return the inode of the file at the given path. If it was not able to traverse down the entire
   *         path, it will set the second field to the first path component it didn't find. It never
   *         returns null.
   * @throws InvalidPathException
   */
  private Pair<Inode, Integer> traverseToInode(String[] pathNames) throws InvalidPathException {
    synchronized (mRootLock) {
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
                "Traversal failed. Component " + k + "(" + ret.getFirst().getName() + ") is a file";
            LOG.info("InvalidPathException: " + msg);
            throw new InvalidPathException(msg);
          }
        }
      }
      return ret;
    }
  }

  /**
   * Update the metadata of a table.
   *
   * @param tableId The id of the table to update
   * @param metadata The new metadata to update the table with
   * @throws TableDoesNotExistException
   * @throws TachyonException
   */
  public void updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException {
    synchronized (mRootLock) {
      Inode inode = mFileIdToInodes.get(tableId);

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
   * @param workerId The id of the worker to deal with
   * @param usedBytesOnTiers Used bytes on each storage tier
   * @param removedBlockIds The list of removed block ids
   * @param addedBlockIds Mapping from id of the StorageDir and id list of blocks evicted in
   * @return a command specifying an action to take
   * @throws BlockInfoException
   */
  public Command workerHeartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlockIds) throws BlockInfoException {
    LOG.debug("WorkerId: {}", workerId);
    synchronized (mRootLock) {
      synchronized (mWorkers) {
        MasterWorkerInfo tWorkerInfo = mWorkers.get(workerId);

        if (tWorkerInfo == null) {
          LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId
              + " . Send command to let it re-register.");
          return new Command(CommandType.Register, new ArrayList<Long>());
        }

        tWorkerInfo.updateUsedBytes(usedBytesOnTiers);
        tWorkerInfo.updateBlocks(false, removedBlockIds);
        tWorkerInfo.updateToRemovedBlocks(false, removedBlockIds);
        tWorkerInfo.updateLastUpdatedTimeMs();

        for (long blockId : removedBlockIds) {
          int fileId = BlockInfo.computeInodeId(blockId);
          int blockIndex = BlockInfo.computeBlockIndex(blockId);
          Inode inode = mFileIdToInodes.get(fileId);
          if (inode == null) {
            LOG.error("File " + fileId + " does not exist");
          } else if (inode.isFile()) {
            ((InodeFile) inode).removeLocation(blockIndex, workerId);
            LOG.debug("File {} with block {} was evicted from worker {} ", fileId, blockIndex,
                workerId);
          }
        }

        for (Entry<Long, List<Long>> addedBlocks : addedBlockIds.entrySet()) {
          long storageDirId = addedBlocks.getKey();
          for (long blockId : addedBlocks.getValue()) {
            int fileId = BlockInfo.computeInodeId(blockId);
            int blockIndex = BlockInfo.computeBlockIndex(blockId);
            Inode inode = mFileIdToInodes.get(fileId);
            if (inode == null) {
              LOG.error("File " + fileId + " does not exist");
            } else if (inode.isFile()) {
              List<BlockInfo> blockInfoList = ((InodeFile) inode).getBlockList();
              NetAddress workerAddress = mWorkers.get(workerId).getAddress();
              if (blockInfoList.size() <= blockIndex) {
                throw new BlockInfoException("BlockInfo not found! blockIndex:" + blockIndex);
              } else {
                BlockInfo blockInfo = blockInfoList.get(blockIndex);
                blockInfo.addLocation(workerId, workerAddress, storageDirId);
              }
            }
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
   * @param objWriter The used object writer
   * @param dos The target data output stream
   * @throws IOException
   */
  @Override
  public void writeImage(ObjectWriter objWriter, DataOutputStream dos) throws IOException {
    ImageElement ele =
        new ImageElement(ImageElementType.Version).withParameter("version",
            Constants.JOURNAL_VERSION);

    writeElement(objWriter, dos, ele);

    synchronized (mRootLock) {
      synchronized (mFileIdToDependency) {
        for (Dependency dep : mFileIdToDependency.values()) {
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

  /**
   * Used by internal classes when trying to create new instance based on this MasterInfo
   *
   * @return TachyonConf used by this MasterInfo.
   */
  TachyonConf getTachyonConf() {
    return mTachyonConf;
  }

  private void checkOwner(TachyonURI path)
      throws AccessControlException {
    if (mPermissionEnabled) {
      getPermissionChecker().check(resolve(mRoot, path), true, null, null, null);
    }
  }

  private void checkPathAccess(TachyonURI path, AclPermission access)
      throws AccessControlException {
    if (mPermissionEnabled) {
      getPermissionChecker().check(resolve(mRoot, path), false, null, null, access);
    }
  }

  private void checkAncestorAccess(TachyonURI path, AclPermission access)
      throws AccessControlException {
    if (mPermissionEnabled) {
      getPermissionChecker().check(resolve(mRoot, path), false, access, null, null);
    }
  }

  private void checkParentAccess(TachyonURI path, AclPermission access)
      throws AccessControlException {
    if (mPermissionEnabled) {
      getPermissionChecker().check(resolve(mRoot, path), false, null, access, null);
    }
  }

  /**
   * Retrieve InodesInPath from startingFolder and path.
   * the number of INodes is equal to the number of path array.
   *
   * Example:
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following parameter path "/c1/c2/c3" and "/" as starting
   * directory

   * fill the array with InodesInpath [rootINode,c1,c2,null]
   *
   * @param startingFolder the starting directory
   * @param path
   * @return the specified number of existing INodes in the path
   */
  public static InodesInPath resolve(final InodeFolder startingFolder, TachyonURI path) {
    try {
      String[] pathByNameArr = CommonUtils.getPathComponents(path.toString());
      Preconditions.checkArgument(startingFolder.getName().equals(pathByNameArr[0]));
      Inode[] inodes = new Inode[pathByNameArr.length];
      int inodeNum = 0;
      Inode curNode = startingFolder;

      while (inodeNum < pathByNameArr.length && curNode != null) {
        final boolean lastComp = (inodeNum == pathByNameArr.length - 1);
        final boolean isDir = curNode.isDirectory();
        inodes[inodeNum++] = curNode;
        if (lastComp || !isDir) {
          break;
        }
        InodeFolder tmpNode = (InodeFolder)curNode;
        curNode = tmpNode.getChild(pathByNameArr[inodeNum]);
      }
      return new InodesInPath(inodes, pathByNameArr);
    } catch (InvalidPathException e) {
      throw new RuntimeException("resolve path :" + path.toString() + " error");
    }
  }
}
