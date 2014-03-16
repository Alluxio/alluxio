/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
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
import tachyon.io.Utils;
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
public class MasterInfo {
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

        // TODO these two locks are not efficient. Since node failure is rare,
        // this is fine for now
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
          new Thread(new RecomputeCmd(cmd, filePath)).start();
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

  public class RecomputeCmd implements Runnable {
    private final String CMD;
    private final String FILE_PATH;

    public RecomputeCmd(String cmd, String filePath) {
      CMD = cmd;
      FILE_PATH = filePath;
    }

    @Override
    public void run() {
      try {
        LOG.info("Exec " + CMD + " output to " + FILE_PATH);
        Process p = java.lang.Runtime.getRuntime().exec(CMD);
        String line;
        BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        File file = new File(FILE_PATH);
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        while ((line = bri.readLine()) != null) {
          bw.write(line + "\n");
        }
        bri.close();
        while ((line = bre.readLine()) != null) {
          bw.write(line + "\n");
        }
        bre.close();
        bw.flush();
        bw.close();
        p.waitFor();
        LOG.info("Exec " + CMD + " output to " + FILE_PATH + " done.");
      } catch (IOException e) {
        LOG.error(e.getMessage());
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final InetSocketAddress MASTER_ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;
  private final MasterConf MASTER_CONF;
  private CheckpointInfo mCheckpointInfo = new CheckpointInfo(0, 0, 0);

  private AtomicInteger mInodeCounter = new AtomicInteger(0);

  private AtomicInteger mDependencyCounter = new AtomicInteger(0);
  private AtomicInteger mRerunCounter = new AtomicInteger(0);

  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);
  // Root Inode's id must be 1.
  private InodeFolder mRoot;
  // A map from file ID's to Inodes.
  private Map<Integer, Inode> mInodes = new ConcurrentHashMap<Integer, Inode>();
  private Map<Integer, Dependency> mDependencies = new HashMap<Integer, Dependency>();
  private InodeRawTables mRawTables = new InodeRawTables();

  // TODO add initialization part for master failover or restart.
  // All operations on these members are synchronized on mDependencies.
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

  int
      _createDependency(List<Integer> parentsIdList, List<Integer> childrenIdList,
          String commandPrefix, List<ByteBuffer> data, String comment, String framework,
          String frameworkVersion, DependencyType dependencyType, int dependencyId,
          long creationTimeMs) throws InvalidPathException, FileDoesNotExistException {
    Dependency dep = null;
    synchronized (mRoot) {
      Set<Integer> parentDependencyIds = new HashSet<Integer>();
      for (int k = 0; k < parentsIdList.size(); k ++) {
        int parentId = parentsIdList.get(k);
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
          new Dependency(dependencyId, parentsIdList, childrenIdList, commandPrefix, data,
              comment, framework, frameworkVersion, dependencyType, parentDependencyIds,
              creationTimeMs);

      List<Inode> childrenInodeList = new ArrayList<Inode>();
      for (int k = 0; k < childrenIdList.size(); k ++) {
        InodeFile inode = (InodeFile) mInodes.get(childrenIdList.get(k));
        inode.setDependencyId(dep.ID);
        childrenInodeList.add(inode);
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

    mJournal.getEditLog().createDependency(parentsIdList, childrenIdList, commandPrefix, data,
        comment, framework, frameworkVersion, dependencyType, dependencyId, creationTimeMs);
    mJournal.getEditLog().flush();

    LOG.info("Dependency created: " + dep);

    return dep.ID;
  }

  // TODO Make this API better.
  /**
   * Internal API.
   *
   * @param recursive
   * @param path
   * @param directory
   * @param blockSizeByte
   * @param creationTimeMs
   * @return
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
    }

    LOG.debug("createFile" + CommonUtils.parametersToString(path));

    String[] pathNames = CommonUtils.getPathComponents(path);
    String name = pathNames[pathNames.length - 1];

    String[] folderPath = new String[pathNames.length - 1];
    System.arraycopy(pathNames, 0, folderPath, 0, folderPath.length);
    InodeLocks inodeLocks = getInode(folderPath, true);
    try {
      // pathInd is the index into pathNames where we start filling in the path from the inode.
      int pathInd = folderPath.length;
      if (inodeLocks.errorInd >= 0) {
        // Then the path component at errorInd k doesn't exist. If
        // it's not recursive, we throw an exception here. Otherwise
        // we add the remaining path components to the list of
        // components to create.
        if (!recursive) {
          final String msg =
              "File " + path + " creation failed. Component " + inodeLocks.errorInd + "("
                  + folderPath[inodeLocks.errorInd] + ") does not exist";
          LOG.info("InvalidPathException: " + msg);
          throw new InvalidPathException(msg);
        } else {
          // The lock at errorInd-1 was the last lock taken, but it was only readLocked. Since we'll
          // be modifying inodeLocks.inode, we need to upgrade the lock to a write lock.
          inodeLocks.locks[inodeLocks.errorInd - 1].upgrade();
          inodeLocks.isWrite = true;
          // We will start filling in the path from inodeLocks.errorInd
          pathInd = inodeLocks.errorInd;
        }
      }

      InodeFolder cur = (InodeFolder) inodeLocks.inode;
      // Fill in the directories that were missing. We don't need to take any more locks, since the
      // starting inodeLocks.inode should be write-locked.
      for (int k = pathInd; k < folderPath.length; k ++) {
        InodeFolder dir =
            new InodeFolder(pathNames[k], mInodeCounter.incrementAndGet(), cur.getId(),
                creationTimeMs);
        cur.addChild(dir);
        mInodes.put(dir.getId(), dir);
        cur = dir;
      }

      // Create the final path component. First we need to make sure that there isn't already a file
      // here with that name.
      Inode ret = cur.getChild(name);
      if (ret != null) {
        final String msg = "File " + path + " already exist.";
        LOG.info("FileAlreadyExistException: " + msg);
        throw new FileAlreadyExistException(msg);
      }
      if (directory) {
        ret = new InodeFolder(name, mInodeCounter.incrementAndGet(), cur.getId(), creationTimeMs);
      } else {
        ret =
            new InodeFile(name, mInodeCounter.incrementAndGet(), cur.getId(), blockSizeByte,
                creationTimeMs);
        String curPath = StringUtils.join(pathNames, Constants.PATH_SEPARATOR);
        if (mPinList.inList(curPath)) {
          synchronized (mFileIdPinList) {
            mFileIdPinList.add(ret.getId());
            ((InodeFile) ret).setPin(true);
          }
        }
        if (mWhiteList.inList(curPath)) {
          ((InodeFile) ret).setCache(true);
        }
      }

      mInodes.put(ret.getId(), ret);
      ((InodeFolder) cur).addChild(ret);

      LOG.debug("createFile: File Created: " + ret + " parent: " + cur);
      return ret.getId();
    } finally {
      inodeLocks.destroy();
    }
  }

  void _createRawTable(int inodeId, int columns, ByteBuffer metadata) throws TachyonException {
    if (!mRawTables.addRawTable(inodeId, columns, metadata)) {
      throw new TachyonException("Failed to create raw table.");
    }
    mJournal.getEditLog().createRawTable(inodeId, columns, metadata);
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
    int ret = _createFile(recursive, path, directory, blockSizeByte, creationTimeMs);
    mJournal.getEditLog().createFile(recursive, path, directory, blockSizeByte, creationTimeMs);
    mJournal.getEditLog().flush();
    return ret;
  }

  public int createFile(String path, long blockSizeByte) throws FileAlreadyExistException,
      InvalidPathException, BlockInfoException, TachyonException {
    return createFile(true, path, false, blockSizeByte);
  }

  /**
   * Create an image of the dependencies and filesystem tree.
   *
   * @param os
   *          The output stream to write the image to
   */
  public void createImage(DataOutputStream os) throws IOException {
    // We have to add the nodes in bottom up order, so that when we
    // load an image, we add children nodes to mInodes before the
    // parent has to search mInodes to get its children.
    for (Dependency dep : mDependencies.values()) {
      createImageDependencyWriter(dep, os);
    }
    createImageHelper(os, mRoot);

    mRawTables.createImageWriter(os);

    os.writeByte(Image.T_CHECKPOINT);
    os.writeInt(mInodeCounter.get());
    os.writeLong(mCheckpointInfo.getEditTransactionCounter());
    os.writeInt(mCheckpointInfo.getDependencyCounter());
  }

  /**
   * Walks the tree in a depth-first search and adds the inodes rooted
   * at tFolder, include tFolder itself.
   */
  private void createImageHelper(DataOutputStream os, InodeFolder tFolder) throws IOException {
    tFolder.rwl.readLock();
    try {
      for (Inode tInode : tFolder.getChildren(false)) {
        if (tInode.isDirectory()) {
          createImageHelper(os, (InodeFolder) tInode);
        } else {
          createImageInodeWriter(tInode, os);
        }
        if (tInode.isFile() && ((InodeFile) tInode).isPin()) {
          synchronized (mFileIdPinList) {
            mFileIdPinList.add(tInode.getId());
          }
        }
      }
      createImageInodeWriter(tFolder, os);
    } finally {
      tFolder.rwl.readUnlock();
    }
  }

  /**
   * Writes a dependency to the image.
   *
   * @param dep
   *          The dependency to write
   * @param os
   *          The output stream to write the dependency to
   */
  private void createImageDependencyWriter(Dependency dep, DataOutputStream os) throws IOException {
    os.writeByte(Image.T_DEPENDENCY);
    os.writeInt(dep.ID);
    Utils.writeIntegerList(dep.PARENT_FILES, os);
    Utils.writeIntegerList(dep.CHILDREN_FILES, os);
    Utils.writeString(dep.COMMAND_PREFIX, os);
    Utils.writeByteBufferList(dep.DATA, os);
    Utils.writeString(dep.COMMENT, os);
    Utils.writeString(dep.FRAMEWORK, os);
    Utils.writeString(dep.FRAMEWORK_VERSION, os);
    os.writeInt(dep.TYPE.getValue());
    Utils.writeIntegerList(dep.PARENT_DEPENDENCIES, os);
    os.writeLong(dep.CREATION_TIME_MS);
    Utils.writeIntegerList(dep.getUncheckpointedChildrenFiles(), os);
  }

  /**
   * Writes an inode to the image.
   *
   * @param inode
   *          The inode to write
   * @param os
   *          The output stream to write the inode to
   */
  private void createImageInodeWriter(Inode inode, DataOutputStream os) throws IOException {
    if (inode.isFile()) {
      InodeFile file = (InodeFile) inode;
      os.writeByte(Image.T_INODE_FILE);
      os.writeLong(file.getCreationTimeMs());
      os.writeInt(file.getId());
      Utils.writeString(file.getName(), os);
      os.writeInt(file.getParentId());

      os.writeLong(file.getBlockSizeByte());
      os.writeLong(file.getLength());
      os.writeBoolean(file.isComplete());
      os.writeBoolean(file.isPin());
      os.writeBoolean(file.isCache());
      Utils.writeString(file.getCheckpointPath(), os);
      os.writeInt(file.getDependencyId());
    } else {
      if (inode.isRawTable()) {
        os.writeByte(Image.T_INODE_RAW_TABLE);
      } else {
        os.writeByte(Image.T_INODE_FOLDER);
      }

      InodeFolder folder = (InodeFolder) inode;
      os.writeLong(folder.getCreationTimeMs());
      os.writeInt(folder.getId());
      Utils.writeString(folder.getName(), os);
      os.writeInt(folder.getParentId());

      List<Integer> children = folder.getChildrenIds();
      os.writeInt(children.size());
      for (int k = 0; k < children.size(); k ++) {
        os.writeInt(children.get(k));
      }
    }
  }

  /**
   * Creates a new block for the given file.
   *
   * @param fileId
   *          The id of the file
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
   */
  public int createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException,
      TachyonException {
    LOG.info("createRawTable" + CommonUtils.parametersToString(path, columns));

    if (columns <= 0 || columns >= Constants.MAX_COLUMNS) {
      throw new TableColumnException("Column " + columns + " should between 0 to "
          + Constants.MAX_COLUMNS);
    }

    int id;
    try {
      id = createFile(true, path, true, 0);
      _createRawTable(id, columns, metadata);
    } catch (BlockInfoException e) {
      throw new FileAlreadyExistException(e.getMessage());
    }

    for (int k = 0; k < columns; k ++) {
      mkdir(path + Constants.PATH_SEPARATOR + Constants.COL_PREFIX + k);
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
  public boolean delete(int fileId, boolean recursive) throws TachyonException,
      InvalidPathException, FileDoesNotExistException {
    String path = getPath(fileId);
    if (path == null) {
      return false;
    }
    return delete(path, recursive);
  }

  /**
   * Delete a file at a given path.
   *
   * @param path
   *          The file to be deleted.
   * @param recursive
   *          If the path points to a directory, whether to delete the entire directory or
   *          do nothing.
   * @return succeed or not
   * @throws TachyonException
   */
  public boolean delete(String path, boolean recursive) throws TachyonException,
      InvalidPathException {
    LOG.info("delete(" + path + ")");
    // If retid == -1, there was an error for which we want to return
    // false. If retid == 0, there is an error for which we want to
    // return true. Otherwise we return true.
    int retid = _delete(path, recursive);
    if (retid > 0) {
      mJournal.getEditLog().delete(retid, recursive);
      mJournal.getEditLog().flush();
      return true;
    }
    return (retid != -1);
  }

  /**
   * Inner delete function. Returns the id of the deleted inode so it
   * can be logged.
   */
  int _delete(String path, boolean recursive) throws TachyonException, InvalidPathException {
    boolean succeed = true;

    // Grab a write lock on the parent directory
    String pathParent = CommonUtils.getParent(path);
    String pathName = CommonUtils.getName(path);
    InodeLocks inodeLocks = getInode(pathParent, true);
    try {
      if (inodeLocks.errorInd >= 0 || !inodeLocks.inode.isDirectory()) {
        // We couldn't traverse to the parent, but we still return
        // true.
        return 0;
      }
      // Now we get the inode from the parent that we want to delete.
      // If path == Constants.PATH_SEPARATOR, we get mRoot.
      Inode delNode = null;
      if (path == Constants.PATH_SEPARATOR) {
        delNode = mRoot;
      } else {
        delNode = ((InodeFolder) inodeLocks.inode).getChild(pathName);
      }
      if (delNode == null) {
        // We couldn't find the inode we want to delete in the
        // parent directory, but we still return true.
        return 0;
      }

      // Since the parent inode has a write lock on it, we can
      // safely traverse and manipulate delNode.
      Set<Inode> inodes = new HashSet<Inode>();
      if (delNode.isDirectory()) {
        inodes.addAll(((InodeFolder) delNode).getChildren(true));
      }
      inodes.add(delNode);

      if (delNode.isDirectory() && !recursive && inodes.size() > 1) {
        // delNode is nonempty, and we don't want to delete a
        // nonempty directory unless recursive is true
        return -1;
      }

      // We go through each inode, removing it from it's parent set
      // and from mInodes. If it's a file, we deal with the
      // checkpoints and blocks as well.
      for (Inode i : inodes) {
        if (i.equals(mRoot)) {
          continue;
        }
        InodeFolder parent = (InodeFolder) mInodes.get(i.getParentId());
        parent.removeChild(i);

        if (i.isFile()) {
          String checkpointPath = ((InodeFile) i).getCheckpointPath();
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

          List<Pair<Long, Long>> blockIdWorkerIdList = ((InodeFile) i).getBlockIdWorkerIdPairs();
          synchronized (mWorkers) {
            for (Pair<Long, Long> blockIdWorkerId : blockIdWorkerIdList) {
              MasterWorkerInfo workerInfo = mWorkers.get(blockIdWorkerId.getSecond());
              if (workerInfo != null) {
                workerInfo.updateToRemovedBlock(true, blockIdWorkerId.getFirst());
              }
            }
          }

          if (((InodeFile) i).isPin()) {
            synchronized (mFileIdPinList) {
              mFileIdPinList.remove(i.getId());
            }
          }
        }

        if (mRawTables.exist(i.getId())) {
          succeed = succeed && mRawTables.delete(i.getId());
        }
      }

      int retid = delNode.getId();
      if (!succeed) {
        retid = -1;
      }

      for (Inode i : inodes) {
        mInodes.remove(i.getId());
        i.reverseId();
      }

      return retid;
    } finally {
      inodeLocks.destroy();
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
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path + " does not exist.");
      }
      if (!inodeLocks.inode.isFile()) {
        throw new FileDoesNotExistException(path + " is not a file.");
      }
      return ((InodeFile) inodeLocks.inode).getBlockList();
    } finally {
      inodeLocks.destroy();
    }
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
   */
  public ClientFileInfo getClientFileInfo(int fid) throws FileDoesNotExistException,
      InvalidPathException {
    String path = getPath(fid);
    if (path == null) {
      throw new FileDoesNotExistException("Failed to getClientFileInfo: " + fid
          + " does not exist");
    }
    return getClientFileInfo(path);
  }

  /**
   * Get the file info for the file at the given path
   *
   * @param path
   *          The path of the file
   * @return the file info
   */
  public ClientFileInfo getClientFileInfo(String path) throws FileDoesNotExistException,
      InvalidPathException {
    LOG.info("getClientFileInfo(" + path + ")");
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path);
      }
      LOG.info("finished creating client file info");
      return inodeLocks.inode.generateClientFileInfo(path);
    } finally {
      inodeLocks.destroy();
    }
  }

  /**
   * Get the raw table info associated with the given id.
   *
   * @param id
   *          The id of the table
   * @return the table info
   */
  public ClientRawTableInfo getClientRawTableInfo(int id) throws TableDoesNotExistException,
      InvalidPathException, FileDoesNotExistException {
    String path = getPath(id);
    if (path == null) {
      throw new TableDoesNotExistException("Table " + id + " does not exist.");
    }
    return getClientRawTableInfo(path);
  }

  /**
   * Get the raw table info for the table at the given path
   *
   * @param path
   *          The path of the table
   * @return the table info
   */
  public ClientRawTableInfo getClientRawTableInfo(String path) throws TableDoesNotExistException,
      InvalidPathException {
    LOG.info("getClientRawTableInfo(" + path + ")");
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new TableDoesNotExistException(path);
      }
      ClientRawTableInfo ret = new ClientRawTableInfo();
      ret.id = inodeLocks.inode.getId();
      ret.name = inodeLocks.inode.getName();
      ret.path = path;
      ret.columns = mRawTables.getColumns(ret.id);
      ret.metadata = mRawTables.getMetadata(ret.id);
      return ret;
    } finally {
      inodeLocks.destroy();
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
    LOG.info("getFileId(" + path + ")");
    InodeLocks inodeLocks = getInode(path, false);
    try {
      int ret = -1;
      if (inodeLocks.errorInd == -1) {
        ret = inodeLocks.inode.getId();
      }
      LOG.info("getFileId(" + path + "): " + ret);
      return ret;
    } finally {
      inodeLocks.destroy();
    }
  }

  /**
   * Get the block infos of a file with the given id. Throws an exception if the id names a
   * directory.
   *
   * @param fileId
   *          The id of the file to look up
   * @return the block infos of the file
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
   */
  public List<ClientBlockInfo> getFileLocations(String path) throws FileDoesNotExistException,
      InvalidPathException, IOException {
    LOG.info("getFileLocations: " + path);
    synchronized (mRoot) {
      InodeLocks inodeLocks = getInode(path, false);
      try {
        if (inodeLocks.errorInd >= 0) {
          throw new FileDoesNotExistException(path);
        }
        return getFileLocations(inodeLocks.inode.getId());
      } finally {
        inodeLocks.destroy();
      }
    }
  }

  /**
   * Get the file id's of the given paths. It recursively scans directories for the file id's inside
   * of them.
   *
   * @param pathList
   *          The list of paths to look at
   * @return the file id's of the files.
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

    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path);
      }

      if (inodeLocks.inode.isDirectory()) {
        for (Inode i : ((InodeFolder) inodeLocks.inode).getChildren(false)) {
          ret.add(i.generateClientFileInfo(path + Constants.PATH_SEPARATOR + i.getName()));
        }
      } else {
        ret.add(inodeLocks.inode.generateClientFileInfo(path));
      }
      return ret;
    } finally {
      inodeLocks.destroy();
    }
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

        List<Integer> childrenIds = tFolder.getChildrenIds();
        for (int id : childrenIds) {
          Inode tInode = mInodes.get(id);
          String newPath = curPath + Constants.PATH_SEPARATOR + tInode.getName();
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
   * A container for an inode found in the tree and the locks taken to get there. It also contains
   * certain flags to provide more information about the inode and the state of the traversal to the
   * inode.
   */
  private class InodeLocks {
    public Inode inode;
    public ReadWriteLock[] locks;
    // If true, the last lock is assumed to be a write lock
    public boolean isWrite;
    // If set to >= 0, there was an error while processing the path
    // component at this index
    public int errorInd;

    InodeLocks(Inode i, ReadWriteLock[] l, boolean iw) {
      inode = i;
      locks = l;
      isWrite = iw;
      errorInd = -1;
    }

    // Unlocks the taken locks. If isWrite is true, the last lock taken should
    // be a write lock. This can safely be called multiple times, since locks is
    // set to null after it completes the first time.
    void destroy() {
      if (locks != null) {
        int i;
        // It should never be the case that locks.length > 1 and
        // locks[0] == null and locks[1] != null.
        for (i = 0; i < locks.length - 1 && locks[i + 1] != null; i ++) {
          locks[i].readUnlock();
        }
        if (locks[i] != null) {
          if (isWrite) {
            locks[i].writeUnlock();
          } else {
            locks[i].readUnlock();
          }
        }
        locks = null;
      }
    }
  }

  /**
   * Get the inode of the file at the given path.
   *
   * @param path
   *          The path to search for
   * @return the inode of the file at the given path, or null if the file does not exist
   */
  private InodeLocks getInode(String path, boolean isWrite) throws InvalidPathException {
    return getInode(CommonUtils.getPathComponents(path), isWrite);
  }

  /**
   * Get the inode at the given path.
   *
   * @param pathNames
   *          The path to search for, broken into components
   * @param isWrite
   *          If true, the last component in the path is write-locked, provided
   *          it is a directory.
   * @return the inode of the file at the given path as well as the locks taken
   *         to get there. If there is an error while traversing the tree, it will
   *         indicate the position of the error and return. Thus it will never return
   *         null.
   */
  private InodeLocks getInode(String[] pathNames, boolean isWrite) throws InvalidPathException {
    if (pathNames == null || pathNames.length == 0) {
      return null;
    }
    if (pathNames.length == 1) {
      if (pathNames[0].equals("")) {
        ReadWriteLock[] locks = new ReadWriteLock[] { mRoot.rwl };
        if (isWrite) {
          locks[0].writeLock();
        } else {
          locks[0].readLock();
        }
        return new InodeLocks(mRoot, locks, isWrite);
      } else {
        final String msg = "File name starts with " + pathNames[0];
        LOG.info("InvalidPathException: " + msg);
        throw new InvalidPathException(msg);
      }
    }

    InodeLocks ret = new InodeLocks(mRoot, new ReadWriteLock[pathNames.length], isWrite);
    ret.locks[0] = mRoot.rwl;
    ret.locks[0].readLock();

    for (int k = 1; k < pathNames.length; k ++) {
      Inode next = ((InodeFolder) ret.inode).getChild(pathNames[k]);
      if (next == null) {
        // The user might want to create the nonexistent directories, so we leave the locks intact.
        // We leave ret.inode as the inode above the nonexistent directory, so the caller can start
        // filling in the path from there. We set errorInd to k, to indicate that the kth path
        // component was the first one that couldn't be found. We set isWrite to false, since no
        // write locks were taken.
        ret.isWrite = false;
        ret.errorInd = k;
        break;
      }
      ret.inode = next;
      if (!ret.inode.isDirectory()) {
        // The inode can't have any children. If this is the last path component, we're good.
        // Otherwise, we can't traverse further, so we clean up and throw an exception. We set
        // isWrite to false, since we haven't yet taken any write locks.
        ret.isWrite = false;
        if (k == pathNames.length - 1) {
          break;
        } else {
          ret.destroy();
          final String msg =
              "Traversal to " + StringUtils.join(pathNames, Constants.PATH_SEPARATOR)
                  + " failed. Component " + k + "(" + ret.inode.getName() + ") is a file";
          LOG.info("InvalidPathException: " + msg);
          throw new InvalidPathException(msg);
        }
      }
      // It's a directory, so we can take the lock and continue.
      ret.locks[k] = ((InodeFolder) ret.inode).rwl;
      if (isWrite && k == pathNames.length - 1) {
        // We want to write-lock the final path component
        ret.locks[k].writeLock();
      } else {
        ret.locks[k].readLock();
      }
    }

    return ret;
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
   */
  public int getNumberOfFiles(String path) throws InvalidPathException, FileDoesNotExistException {
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path);
      }
      if (inodeLocks.inode.isFile()) {
        return 1;
      }
      return ((InodeFolder) inodeLocks.inode).getNumberOfChildren();
    } finally {
      inodeLocks.destroy();
    }
  }

  /**
   * Get the file path specified by a given inode.
   *
   * @param inode
   *          The inode
   * @return the path of the inode
   */
  public String getPath(Inode inode) {
    // We can't let any destructive operations occur while traversing up the tree, so the whole tree
    // needs to be locked.
    if (inode.getId() == 1) {
      return Constants.PATH_SEPARATOR;
    }
    mRoot.rwl.writeLock();
    try {
      String path = "";
      while (inode != null && inode.getId() != 1) {
        path = Constants.PATH_SEPARATOR + inode.getName() + path;
        inode = mInodes.get(inode.getParentId());
      }
      if (inode == null) {
        return null;
      } else {
        return path;
      }
    } finally {
      mRoot.rwl.writeUnlock();
    }
  }

  /**
   * Get the file path specified by a given id.
   *
   * @param id
   *          The id of the inode
   * @return the path of the inode
   */
  public String getPath(int id) throws FileDoesNotExistException {
    Inode inode = mInodes.get(id);
    if (inode == null) {
      throw new FileDoesNotExistException("FileId " + id + " does not exist");
    } else {
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
   */
  public int getRawTableId(String path) throws InvalidPathException {
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0 || !inodeLocks.inode.isRawTable()) {
        return -1;
      }
      return inodeLocks.inode.getId();
    } finally {
      inodeLocks.destroy();
    }
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
   */
  public long getUnderFsCapacityBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(CommonConf.get().UNDERFS_DATA_FOLDER);
    return ufs.getSpace(CommonConf.get().UNDERFS_DATA_FOLDER, SpaceType.SPACE_TOTAL);
  }

  /**
   * Get the amount of free space in the under file system.
   *
   * @return the free space in bytes
   */
  public long getUnderFsFreeBytes() throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(CommonConf.get().UNDERFS_DATA_FOLDER);
    return ufs.getSpace(CommonConf.get().UNDERFS_DATA_FOLDER, SpaceType.SPACE_FREE);
  }

  /**
   * Get the amount of space used in the under file system.
   *
   * @return the space used in bytes
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
   * @return the address of the selected worker
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
   */
  public List<Integer> listFiles(String path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>();
    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path);
      }

      if (inodeLocks.inode.isFile()) {
        ret.add(inodeLocks.inode.getId());
      } else if (recursive) {
        for (Inode i : ((InodeFolder) inodeLocks.inode).getChildren(true)) {
          if (!i.isDirectory()) {
            ret.add(i.getId());
          }
        }
      }
    } finally {
      inodeLocks.destroy();
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
        Dependency dep =
            new Dependency(is.readInt(), Utils.readIntegerList(is), Utils.readIntegerList(is),
                Utils.readString(is), Utils.readByteBufferList(is), Utils.readString(is),
                Utils.readString(is), Utils.readString(is), DependencyType.getDependencyType(is
                    .readInt()), Utils.readIntegerList(is), is.readLong());
        dep.resetUncheckpointedChildrenFiles(Utils.readIntegerList(is));

        mDependencies.put(dep.ID, dep);
        if (!dep.hasCheckpointed()) {
          mUncheckpointedDependencies.add(dep.ID);
        }
        for (int parentDependencyId : dep.PARENT_DEPENDENCIES) {
          mDependencies.get(parentDependencyId).addChildrenDependency(dep.ID);
        }
      } else if (Image.T_INODE_FILE == type || Image.T_INODE_FOLDER == type) {
        if (type > Image.T_INODE_RAW_TABLE) {
          throw new IOException("Corrupted image with unknown element type: " + type);
        }

        long creationTimeMs = is.readLong();
        int fileId = is.readInt();
        String fileName = Utils.readString(is);
        int parentId = is.readInt();

        Inode inode = null;

        if (type == Image.T_INODE_FILE) {
          long blockSizeByte = is.readLong();
          long length = is.readLong();
          boolean isComplete = is.readBoolean();
          boolean isPin = is.readBoolean();
          boolean isCache = is.readBoolean();
          String checkpointPath = Utils.readString(is);

          InodeFile tInode =
              new InodeFile(fileName, fileId, parentId, blockSizeByte, creationTimeMs);

          try {
            tInode.setLength(length);
          } catch (Exception e) {
            throw new IOException(e);
          }
          tInode.setComplete(isComplete);
          tInode.setPin(isPin);
          tInode.setCache(isCache);
          tInode.setCheckpointPath(checkpointPath);
          tInode.setDependencyId(is.readInt());
          inode = tInode;
        } else if (type == Image.T_INODE_FOLDER) {
          int numberOfChildren = is.readInt();
          int[] children = new int[numberOfChildren];
          for (int k = 0; k < numberOfChildren; k ++) {
            children[k] = is.readInt();
          }

          InodeFolder folder = new InodeFolder(fileName, fileId, parentId, creationTimeMs);
          folder.addChildren(children, mInodes);
          inode = folder;
        }

        LOG.info("Putting " + inode);
        if (inode.getId() > mInodeCounter.get()) {
          mInodeCounter.set(inode.getId());
        }

        if (inode.getId() == 1) {
          mRoot = (InodeFolder) inode;
        }
        mInodes.put(inode.getId(), inode);
      } else if (Image.T_INODE_RAW_TABLE == type) {
        int inodeId = is.readInt();
        int columns = is.readInt();
        ByteBuffer metadata = Utils.readByteBuffer(is);
        try {
          if (!mRawTables.addRawTable(inodeId, columns, metadata)) {
            throw new IOException("Failed to create raw table");
          }
        } catch (TachyonException e) {
          throw new IOException(e);
        }
      } else {
        throw new IOException("Corrupted image with unknown element type: " + type);
      }
    }
  }

  /**
   * Get the names of the subdirectories at the given path.
   *
   * @param path
   *          The path to look at
   * @param recursive
   *          If true, recursively add the paths of the subdirectories
   * @return the list of paths
   */
  public List<String> ls(String path, boolean recursive) throws InvalidPathException,
      FileDoesNotExistException {
    List<String> ret = new ArrayList<String>();

    InodeLocks inodeLocks = getInode(path, false);
    try {
      if (inodeLocks.errorInd >= 0) {
        throw new FileDoesNotExistException(path);
      }
      ret.add(path);
      if (inodeLocks.inode.isDirectory()) {
        ret.addAll(((InodeFolder) inodeLocks.inode).getChildrenPaths(path, recursive));
      }
      return ret;
    } finally {
      inodeLocks.destroy();
    }
  }

  /**
   * Create a directory at the given path.
   *
   * @param path
   *          The path to create a directory at
   * @return true if the creation was successful and false if it wasn't
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
   */
  /**
   * There's a couple things wrong with this implementation. It should fail if
   * dstPath is a subpath of srcInode's path. Also fetching the dstFolder must
   * be done after srcInode has been removed from the tree, so that its inode
   * locks can be removed.
   */
  private void rename(Inode srcInode, String dstPath) throws FileAlreadyExistException,
      InvalidPathException, FileDoesNotExistException {
    String srcPath = getPath(srcInode);
    if (srcPath == null) {
      throw new FileDoesNotExistException("Failed to rename: " + srcInode.getId()
          + " does not exist");
    }
    rename(srcPath, dstPath);
  }

  /**
   * Rename a file to the given path.
   *
   * @param fileId
   *          The id of the file to rename
   * @param dstPath
   *          The new path of the file
   */
  public void rename(int fileId, String dstPath) throws FileDoesNotExistException,
      FileAlreadyExistException, InvalidPathException {
    String srcPath = getPath(fileId);
    if (srcPath == null) {
      throw new FileDoesNotExistException("Failed to rename: " + fileId + " does not exist");
    }
    rename(srcPath, dstPath);
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
    if (srcPath.equals(dstPath)) {
      return;
    }
    // We make sure srcPath isn't a prefix of dstPath, since that is an invalid rename. If srcPath
    // is Constants.PATH_SEPARATOR, then this test should always fail, so if it passes, we know
    // srcPath must have a parent.
    if (CommonUtils.startsWith(dstPath.split(Constants.PATH_SEPARATOR),
        srcPath.split(Constants.PATH_SEPARATOR))) {
      throw new InvalidPathException("Failed to rename: " + srcPath + " is a prefix of " + dstPath);
    }
    /*
     * Before we do any destructive operations, we have to make sure that srcPath and dstPath are
     * valid locations. Since dstPath could share path components with srcPath, we traverse to
     * srcPath's parent and dstPath's parent without any write locks and make sure everything is
     * good. Then we upgrade the srcPath parent's lock to write, remove the intended inode, and
     * place it into dstPath's parent.
     */
    String srcFolder = CommonUtils.getParent(srcPath);
    String srcName = CommonUtils.getName(srcPath);
    String dstFolder = CommonUtils.getParent(dstPath);
    String dstName = CommonUtils.getName(dstPath);
    InodeLocks sil = getInode(srcFolder, false);
    InodeLocks dil = getInode(dstFolder, false);
    try {
      if (sil.errorInd >= 0) {
        throw new InvalidPathException("Failed to rename: source subpath " + srcFolder
            + " does not exist.");
      }
      if (dil.errorInd >= 0) {
        throw new InvalidPathException("Failed to rename: destination subpath " + dstFolder
            + " does not exist.");
      }
      if (!dil.inode.isDirectory()) {
        throw new InvalidPathException("Failed to rename: destination subpath " + dstFolder
            + " is not a directory.");
      }
      // We make sure that srcName exists inside srcFolder and that dstName doesn't exist inside
      // dstFolder
      Inode srcInode = ((InodeFolder) sil.inode).getChild(srcName);
      if (srcInode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + srcPath + " does not exist");
      }
      if (((InodeFolder) dil.inode).getChild(dstName) != null) {
        throw new FileAlreadyExistException("Failed to rename: " + dstPath + " already exists");
      }
      // Currently we have a read lock on srcFolder and dstFolder. We first need to upgrade
      // srcFolder to a write lock before we remove srcInode. It's possible that srcFolder and
      // dstFolder are the same directory, but in that case, we can simply rename srcInode and we're
      // done.
      if (sil.inode.equals(dil.inode)) {
        srcInode.setName(dstName);
      } else {
        sil.locks[sil.locks.length - 1].upgrade();
        sil.isWrite = true;
        ((InodeFolder) sil.inode).removeChild(srcInode);
        srcInode.setParentId(dil.inode.getId());
        srcInode.setName(dstName);
        // Now we can release the locks on sil, upgrade dil to a write lock, and add srcInode with
        // the
        // name dstName.
        sil.destroy();
        dil.locks[dil.locks.length - 1].upgrade();
        dil.isWrite = true;
        ((InodeFolder) dil.inode).addChild(srcInode);
      }

      mJournal.getEditLog().rename(srcInode.getId(), dstPath);
      mJournal.getEditLog().flush();
    } finally {
      sil.destroy();
      dil.destroy();
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
   * Unpin the file with the given id.
   *
   * @param fileId
   *          The id of the file to unpin
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
   */
  public void updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException, TachyonException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(tableId);

      if (inode == null || inode.isFile() || !mRawTables.exist(tableId)) {
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
}
