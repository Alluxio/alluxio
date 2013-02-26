package tachyon;

import java.io.File;
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
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * A global view of filesystem in master.
 * @author Haoyuan Li haoyuan@cs.berkeley.edu
 */
public class MasterInfo {
  public static final String COL = "COL_";

  private final Logger LOG = LoggerFactory.getLogger(MasterInfo.class);

  private final InetSocketAddress MASTER_ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;

  private AtomicInteger mInodeCounter = new AtomicInteger(0);
  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // Root Inode's id must be 1.
  private InodeFolder mRoot;

  private Map<Integer, Inode> mInodes = new HashMap<Integer, Inode>();

  private Map<Long, WorkerInfo> mWorkers = new HashMap<Long, WorkerInfo>();
  private Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();
  private BlockingQueue<WorkerInfo> mLostWorkers = new ArrayBlockingQueue<WorkerInfo>(32);

  // TODO Check the logic related to this two lists.
  private PrefixList mWhiteList;
  private PrefixList mPinList;
  private List<Integer> mIdPinList;

  private MasterLogWriter mMasterLogWriter;

  private Thread mHeartbeatThread;

  /**
   * System periodical status check.
   * 
   * @author Haoyuan
   */
  public class MasterHeartbeatExecutor implements HeartbeatExecutor {
    public MasterHeartbeatExecutor() {
    }

    @Override
    public void heartbeat() {
      LOG.info("Periodical system status checking...");

      Set<Long> lostWorkers = new HashSet<Long>();

      synchronized (mWorkers) {
        for (Entry<Long, WorkerInfo> worker: mWorkers.entrySet()) {
          if (CommonUtils.getCurrentMs() - worker.getValue().getLastUpdatedTimeMs() 
              > Config.WORKER_TIMEOUT_MS) {
            LOG.error("The worker " + worker.getValue() + " got timed out!");
            mLostWorkers.add(worker.getValue());
            lostWorkers.add(worker.getKey());
          }
        }
        for (long workerId: lostWorkers) {
          mWorkers.remove(workerId);
        }
      }

      boolean hadFailedWorker = false;

      while (mLostWorkers.size() != 0) {
        hadFailedWorker = true;
        WorkerInfo worker = mLostWorkers.poll();

        for (int id: worker.getFiles()) {
          synchronized (mRoot) {
            Inode tFile = mInodes.get(id);
            if (tFile != null) {
              tFile.removeLocation(worker.getId());
            }
          }
        }
      }

      if (hadFailedWorker) {
        LOG.warn("Restarting failed workers");
        try {
          java.lang.Runtime.getRuntime().exec(Config.TACHYON_HOME + 
              "/bin/restart-failed-tachyon-workers.sh");
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  public MasterInfo(InetSocketAddress address) {
    mRoot = new InodeFolder("", mInodeCounter.incrementAndGet(), -1);
    mInodes.put(mRoot.getId(), mRoot);

    MASTER_ADDRESS = address;
    START_TIME_MS = System.currentTimeMillis();
    // TODO This name need to be changed.
    START_TIME_NS_PREFIX = START_TIME_MS - (START_TIME_MS % 1000000);

    mWhiteList = new PrefixList(Config.WHITELIST);
    mPinList = new PrefixList(Config.PINLIST);
    mIdPinList = Collections.synchronizedList(new ArrayList<Integer>());

    // TODO Fault recovery: need user counter info;
    recoveryFromLog();
    writeCheckpoint();

    mMasterLogWriter = new MasterLogWriter(Config.MASTER_LOG_FILE);

    mHeartbeatThread = new Thread(new HeartbeatThread(
        new MasterHeartbeatExecutor(), Config.MASTER_HEARTBEAT_INTERVAL_MS));
    mHeartbeatThread.start();
  }

  public int createFile(String path, boolean directory)
      throws FileAlreadyExistException, InvalidPathException {
    return createFile(path, directory, -1, null);
  }

  public int createFile(String path, boolean directory, int columns, List<Byte> metadata)
      throws FileAlreadyExistException, InvalidPathException {
    String parameters = CommonUtils.parametersToString(path);
    LOG.info("createFile" + parameters);

    String[] pathNames = getPathNames(path);

    synchronized (mRoot) {
      Inode inode = getInode(pathNames);
      if (inode != null) {
        Log.info("FileAlreadyExistException: File " + path + " already exist.");
        throw new FileAlreadyExistException("File " + path + " already exist.");
      }

      String name = pathNames[pathNames.length - 1];
      String folderPath = null;
      if (path.length() - name.length() == 1) {
        folderPath = path.substring(0, path.length() - name.length()); 
      } else {
        folderPath = path.substring(0, path.length() - name.length() - 1);
      }
      inode = getInode(folderPath);
      if (inode == null || inode.isFile()) {
        Log.info("InvalidPathException: File " + path + " creation failed. Folder "
            + folderPath + " does not exist.");
        throw new InvalidPathException("File " + path + " creation failed. Folder "
            + folderPath + " does not exist.");
      }

      Inode ret = null;

      if (directory) {
        if (columns != -1) {
          ret = new InodeRawTable(name, mInodeCounter.incrementAndGet(), inode.getId(),
              columns, metadata);
        } else {
          ret = new InodeFolder(name, mInodeCounter.incrementAndGet(), inode.getId());
        }
      } else {
        ret = new InodeFile(name, mInodeCounter.incrementAndGet(), inode.getId());
      }

      mInodes.put(ret.getId(), ret);
      ((InodeFolder) inode).addChild(ret.getId());

      // TODO this two appends should be atomic;
      mMasterLogWriter.appendAndFlush(inode);
      mMasterLogWriter.appendAndFlush(ret);

      LOG.info("createFile: File Created: " + ret + " parent: " + inode);
      return ret.getId();
    }
  }

  public int createRawTable(String path, int columns, List<Byte> metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException {
    String parameters = CommonUtils.parametersToString(path, columns);
    LOG.info("user_createRawTable" + parameters);

    if (columns <= 0 || columns >= Config.MAX_COLUMNS) {
      throw new TableColumnException("Column " + columns + " should between 0 to " + 
          Config.MAX_COLUMNS);
    }

    int id = createFile(path, true, columns, metadata);

    for (int k = 0; k < columns; k ++) {
      createFile(path + Config.SEPARATOR + COL + k, true);
    }

    return id;
  }

  public List<String> ls(String path) throws InvalidPathException, FileDoesNotExistException {
    List<String> ret = new ArrayList<String>();

    Inode inode = getInode(path);

    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }

    if (inode.isFile()) {
      ret.add(path);
    } else {
      List<Integer> childernIds = ((InodeFolder) inode).getChildrenIds();

      if (!path.endsWith("/")) {
        path += "/";
      }
      synchronized (mRoot) {
        for (int k : childernIds) {
          inode = mInodes.get(k);
          if (inode != null) {
            ret.add(path + inode.getName());
          }
        }
      }
    }

    return ret;
  }

  public List<ClientFileInfo> getFilesInfo(String path) 
      throws FileDoesNotExistException, InvalidPathException {
    List<ClientFileInfo> ret = new ArrayList<ClientFileInfo>();

    Inode inode = getInode(path);

    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }

    if (inode.isDirectory()) {
      List<Integer> childernIds = ((InodeFolder) inode).getChildrenIds();

      if (!path.endsWith("/")) {
        path += "/";
      }
      synchronized (mRoot) {
        for (int k : childernIds) {
          ret.add(getClientFileInfo(k));
        }
      }
    } else {
      ret.add(getClientFileInfo(inode.getId()));
    }

    return ret;
  }

  public ClientFileInfo getFileInfo(String path)
      throws FileDoesNotExistException, InvalidPathException {
    Inode inode = getInode(path);
    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }
    return getClientFileInfo(inode.getId());
  }

  public void delete(int id) {
    LOG.info("delete(" + id + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mRoot) {
      Inode inode = mInodes.get(id);

      if (inode == null) {
        return;
      }

      if (inode.isDirectory()) {
        List<Integer> childrenIds = ((InodeFolder) inode).getChildrenIds();

        for (int childId : childrenIds) {
          delete(childId);
        }
      }

      InodeFolder parent = (InodeFolder) mInodes.get(inode.getParentId());
      parent.removeChild(inode.getId());
      mInodes.remove(inode.getId());
      if (inode.isPin()) {
        synchronized (mIdPinList) {
          mIdPinList.remove(inode.getId());
        }
      }
      inode.reverseId();

      // TODO this following append should be atomic
      mMasterLogWriter.appendAndFlush(inode);
      mMasterLogWriter.appendAndFlush(parent);
    }
  }

  public void delete(String path) throws InvalidPathException, FileDoesNotExistException {
    LOG.info("delete(" + path + ")");
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      delete(inode.getId());
    }
  }

  private Inode getInode(String path) throws InvalidPathException {
    return getInode(getPathNames(path));
  }

  private Inode getInode(String[] pathNames) throws InvalidPathException {
    if (pathNames == null || pathNames.length == 0) {
      return null;
    }
    if (pathNames.length == 1) {
      if (pathNames[0].equals("")) {
        return mRoot;
      } else {
        LOG.info("InvalidPathException: File name starts with " + pathNames[0]);
        throw new InvalidPathException("File name starts with " + pathNames[0]);
      }
    }

    Inode cur = mRoot;

    synchronized (mRoot) {
      for (int k = 1; k < pathNames.length && cur != null; k ++) {
        String name = pathNames[k];
        if (cur.isFile()) {
          return null;
        }
        cur = ((InodeFolder) cur).getChild(name, mInodes);
      }

      return cur;
    }
  }

  private static String[] getPathNames(String path) throws InvalidPathException {
    CommonUtils.validatePath(path);
    if (path.length() == 1 && path.equals(Config.SEPARATOR)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(Config.SEPARATOR);
  }

  private void writeCheckpoint() {
    LOG.info("Files recoveried from logs: ");
    MasterLogWriter checkpointWriter =
        new MasterLogWriter(Config.MASTER_CHECKPOINT_FILE + ".tmp");
    Queue<Inode> nodesQueue = new LinkedList<Inode>();
    synchronized (mRoot) {
      checkpointWriter.appendAndFlush(mRoot);
      nodesQueue.add(mRoot);
      while (!nodesQueue.isEmpty()) {
        InodeFolder tFolder = (InodeFolder) nodesQueue.poll();

        List<Integer> childrenIds = tFolder.getChildrenIds();
        for (int id : childrenIds) {
          Inode tInode = mInodes.get(id);
          checkpointWriter.appendAndFlush(tInode);
          if (tInode.isDirectory()) {
            nodesQueue.add(tInode);
          }

          if (tInode.isPin()) {
            synchronized (mIdPinList) {
              mIdPinList.add(tInode.getId());
            }
          }
        }
      }

      checkpointWriter.appendAndFlush(new CheckpointInfo(mInodeCounter.get()));
      checkpointWriter.close();

      CommonUtils.renameFile(Config.MASTER_CHECKPOINT_FILE + ".tmp", Config.MASTER_CHECKPOINT_FILE);

      CommonUtils.deleteFile(Config.MASTER_LOG_FILE);
    }
    LOG.info("Files recovery done. Current mInodeCounter: " + mInodeCounter.get());
  }

  private void recoveryFromFile(String fileName, String msg) {
    MasterLogReader reader;

    File file = new File(fileName);
    if (!file.exists()) {
      LOG.info(msg + fileName + " does not exist.");
    } else {
      LOG.info("Reading " + msg + fileName);
      reader = new MasterLogReader(fileName);
      while (reader.hasNext()) {
        Pair<LogType, Object> pair = reader.getNextPair();
        switch (pair.getFirst()) {
          case CheckpointInfo: {
            CheckpointInfo checkpointInfo = (CheckpointInfo) pair.getSecond();
            mInodeCounter.set(checkpointInfo.COUNTER_INODE);
            break;
          }
          case InodeFile:
          case InodeFolder:
          case InodeRawTable: {
            Inode inode = (Inode) pair.getSecond();
            LOG.info("Putting " + inode);
            if (Math.abs(inode.getId()) > mInodeCounter.get()) {
              mInodeCounter.set(Math.abs(inode.getId()));
            }
            if (inode.getId() > 0) {
              mInodes.put(inode.getId(), inode);
              if (inode.getId() == 1) {
                mRoot = (InodeFolder) inode;
              }
            } else {
              mInodes.remove(- inode.getId());
            }
            break;
          }
          case Undefined:
            CommonUtils.runtimeException("Corruptted data from " + fileName + 
                ". It has undefined data type.");
            break;
          default:
            CommonUtils.runtimeException("Corruptted data from " + fileName);
        }
      }
    }
  }

  private void recoveryFromLog() {
    recoveryFromFile(Config.MASTER_CHECKPOINT_FILE, "Master Checkpoint file ");
    recoveryFromFile(Config.MASTER_LOG_FILE, "Master Log file ");
  }

  public NetAddress getLocalWorker(String host) throws NoLocalWorkerException {
    LOG.info("getLocalWorker(" + host + ")");
    synchronized (mWorkers) {
      for (InetSocketAddress address: mWorkerAddressToId.keySet()) {
        if (address.getHostName().equals(host)) {
          LOG.info("getLocalWorker: " + address);
          return new NetAddress(address.getHostName(), address.getPort());
        }
      }
    }
    LOG.info("getLocalWorker: no local worker on " + host);
    throw new NoLocalWorkerException("getLocalWorker: no local worker on " + host);
  }

  public long getCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (WorkerInfo worker : mWorkers.values()) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  public long getUsedCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (WorkerInfo worker : mWorkers.values()) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  public ClientFileInfo getClientFileInfo(int id) throws FileDoesNotExistException {
    LOG.info("getClientFileInfo(" + id + ")");
    synchronized (mRoot) {
      Inode inode = mInodes.get(id);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + id + " does not exist.");
      }
      ClientFileInfo ret = new ClientFileInfo();
      ret.id = inode.getId();
      ret.name = inode.getName();
      ret.path = getPath(inode);
      ret.checkpointPath = inode.getCheckpointPath();
      ret.sizeBytes = 0;
      ret.inMemory = false;
      if (inode.isFile()) {
        ret.sizeBytes = ((InodeFile) inode).getLength();
        ret.inMemory = ((InodeFile) inode).isInMemory();
      }
      ret.isFolder = inode.isDirectory();
      ret.needPin = inode.isPin();
      ret.needCache = inode.isCache();
      LOG.info("getClientFileInfo(" + id + "): "  + ret);
      return ret;
    }
  }

  public ClientFileInfo getClientFileInfo(String path)
      throws FileDoesNotExistException, InvalidPathException {
    LOG.info("getClientFileInfo(" + path + ")");
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return getClientFileInfo(inode.getId());
    }
  }

  public ClientRawTableInfo getClientRawTableInfo(int id) throws TableDoesNotExistException {
    LOG.info("getClientRawTableInfo(" + id + ")");
    synchronized (mRoot) {
      Inode inode = mInodes.get(id);
      if (inode == null || inode.isFile() || !((InodeFolder) inode).isRawTable()) {
        throw new TableDoesNotExistException("Table " + id + " does not exist.");
      }
      ClientRawTableInfo ret = new ClientRawTableInfo();
      ret.id = inode.getId();
      ret.name = inode.getName();
      ret.path = getPath(inode);
      ret.columns = ((InodeRawTable) inode).getColumns();
      ret.metadata = ((InodeRawTable) inode).getMetadata();
      return ret;
    }
  }

  public ClientRawTableInfo getClientRawTableInfo(String path)
      throws TableDoesNotExistException, InvalidPathException {
    LOG.info("getClientRawTableInfo(" + path + ")");
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new TableDoesNotExistException(path);
      }
      return getClientRawTableInfo(inode.getId());
    }
  }

  public List<NetAddress> getFileLocations(int fileId) throws FileDoesNotExistException {
    LOG.info("getFileLocations: " + fileId);
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null || inode.isDirectory()) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist.");
      }
      LOG.info("getFileLocations: " + fileId + " good return");
      return inode.getLocations();
    }
  }

  public List<NetAddress> getFileLocations(String path) 
      throws FileDoesNotExistException, InvalidPathException {
    LOG.info("getFileLocations: " + path);
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return getFileLocations(inode.getId());
    }
  }

  public int getFileId(String filePath) throws InvalidPathException {
    LOG.info("getFileId(" + filePath + ")");
    Inode inode = getInode(filePath);
    int ret = -1;
    if (inode != null) {
      ret = inode.getId();
    }
    LOG.info("getFileId(" + filePath + "): " + ret);
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

        List<Integer> childrenIds = tFolder.getChildrenIds();
        for (int id : childrenIds) {
          Inode tInode = mInodes.get(id);
          String newPath = curPath + Config.SEPARATOR + tInode.getName();
          if (tInode.isDirectory()) {
            nodesQueue.add(new Pair<InodeFolder, String>((InodeFolder) tInode, newPath));
          } else if (tInode.isInMemory()) {
            ret.add(newPath);
          }
        }
      }
    }
    return ret;
  }

  public int getRawTableId(String path) throws InvalidPathException {
    Inode inode = getInode(path);
    if (inode == null || inode.isFile() || !((InodeFolder) inode).isRawTable()) {
      return -1;
    }
    return inode.getId();
  }

  public long getNewUserId() {
    return mUserCounter.incrementAndGet();
  }

  public int getWorkerCount() {
    synchronized (mWorkers) {
      return mWorkers.size();
    }
  }

  public List<ClientWorkerInfo> getWorkersInfo() {
    List<ClientWorkerInfo> ret = new ArrayList<ClientWorkerInfo>();

    synchronized (mWorkers) {
      for (WorkerInfo worker : mWorkers.values()) {
        ret.add(worker.generateClientWorkerInfo());
      }
    }

    return ret;
  }

  public List<String> getWhiteList() {
    return mWhiteList.getList();
  }

  public List<String> getPinList() {
    return mPinList.getList();
  }

  public List<Integer> getPinIdList() {
    synchronized (mIdPinList) {
      List<Integer> ret = new ArrayList<Integer>();
      for (int id : mIdPinList) {
        ret.add(id);
      }
      return ret;
    }
  }

  private String getPath(Inode inode) {
    synchronized (mRoot) {
      if (inode.getId() == 1) {
        return "/";
      }
      if (inode.getParentId() == 1) {
        return Config.SEPARATOR + inode.getName();
      }
      return getPath(mInodes.get(inode.getParentId())) + Config.SEPARATOR + inode.getName();
    }
  }

  private static String getName(String path) throws InvalidPathException {
    String[] pathNames = getPathNames(path);
    return pathNames[pathNames.length - 1];
  }

  public InetSocketAddress getMasterAddress() {
    return MASTER_ADDRESS;
  }

  public void renameFile(String srcFilePath, String dstFilePath) 
      throws FileDoesNotExistException, InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(srcFilePath);
      if (inode == null) {
        throw new FileDoesNotExistException("File " + srcFilePath + " does not exist");
      }

      String dstName = getName(dstFilePath);
      inode.setName(dstName);

      InodeFolder parent = (InodeFolder) mInodes.get(inode.getParentId());
      parent.removeChild(inode.getId());

      String dstFileFolder = dstFilePath.substring(0, dstFilePath.length() - dstName.length() - 1);
      Inode dstFolder = getInode(dstFileFolder);
      if (dstFolder == null || dstFolder.isFile()) {
        throw new FileDoesNotExistException("Folder " + dstFileFolder + " does not exist.");
      }
      ((InodeFolder) dstFolder).addChild(inode.getId());

      // TODO The following should be done atomically.
      mMasterLogWriter.appendAndFlush(parent);
      mMasterLogWriter.appendAndFlush(dstFolder);
      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  public void unpinFile(int fileId) throws FileDoesNotExistException {
    // TODO Change meta data only. Data will be evicted from worker based on data replacement 
    // policy. TODO May change it to be active from V0.2
    LOG.info("unpinFile(" + fileId + ")");
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("Failed to unpin " + fileId);
      }

      inode.setPin(false);
      synchronized (mIdPinList) {
        mIdPinList.remove(fileId);
      }

      mMasterLogWriter.appendAndFlush(inode);
    }
  }

  public void cachedFile(long workerId, long workerUsedBytes, int fileId,
      int fileSizeBytes, boolean hasCheckpointed, String checkpointPath)
          throws FileDoesNotExistException, SuspectedFileSizeException {
    String parameters = CommonUtils.parametersToString(workerId, workerUsedBytes, fileId, 
        fileSizeBytes);
    LOG.info("cachedFile" + parameters);

    WorkerInfo tWorkerInfo = null;
    synchronized (mWorkers) {
      tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.error("No worker: " + workerId);
        return;
      }
    }

    tWorkerInfo.updateFile(true, fileId);
    tWorkerInfo.updateUsedBytes(workerUsedBytes);
    tWorkerInfo.updateLastUpdatedTimeMs();

    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      InodeFile tFile = (InodeFile) inode;
      boolean needLog = false;

      if (tFile.isReady()) {
        if (tFile.getLength() != fileSizeBytes) {
          throw new SuspectedFileSizeException(fileId + ". Original Size: " +
              tFile.getLength() + ". New Size: " + fileSizeBytes);
        }
      } else {
        tFile.setLength(fileSizeBytes);
        needLog = true;
      }
      if (hasCheckpointed && !tFile.hasCheckpointed()) {
        tFile.setHasCheckpointed(true);
        tFile.setCheckpointPath(checkpointPath);
        needLog = true;
      }
      if (needLog) {
        mMasterLogWriter.appendAndFlush(tFile);
      }
      InetSocketAddress address = tWorkerInfo.ADDRESS;
      tFile.addLocation(workerId, new NetAddress(address.getHostName(), address.getPort()));
    }
  }

  public Command workerHeartbeat(long workerId, long usedBytes, List<Integer> removedFileIds) {
    LOG.info("workerHeartbeat(): WorkerId: " + workerId);
    synchronized (mWorkers) {
      WorkerInfo tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId +
            " . Send command to let it re-register.");
        return new Command(CommandType.Register, ByteBuffer.allocate(0));
      }

      tWorkerInfo.updateUsedBytes(usedBytes);
      tWorkerInfo.updateFiles(false, removedFileIds);
      tWorkerInfo.updateLastUpdatedTimeMs();

      synchronized (mRoot) {
        for (int id : removedFileIds) {
          Inode inode = mInodes.get(id);
          if (inode == null) {
            LOG.error("Data " + id + " does not exist");
          } else {
            inode.removeLocation(id);
          }
        }
      }
    }

    return new Command(CommandType.Nothing, ByteBuffer.allocate(0));
  }

  public long registerWorker(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Integer> currentFileIds) {
    long id = 0;
    InetSocketAddress workerAddress =
        new InetSocketAddress(workerNetAddress.mHost, workerNetAddress.mPort);
    LOG.info("registerWorker(): WorkerNetAddress: " + workerAddress);

    synchronized (mWorkers) {
      if (mWorkerAddressToId.containsKey(workerAddress)) {
        id = mWorkerAddressToId.get(workerAddress);
        mWorkerAddressToId.remove(id);
        LOG.warn("The worker " + workerAddress + " already exists as id " + id + ".");
      }
      if (id != 0 && mWorkers.containsKey(id)) {
        WorkerInfo tWorkerInfo = mWorkers.get(id);
        mWorkers.remove(id);
        mLostWorkers.add(tWorkerInfo);
        LOG.warn("The worker with id " + id + " has been removed.");
      }
      id = START_TIME_NS_PREFIX + mWorkerCounter.incrementAndGet();
      WorkerInfo tWorkerInfo = new WorkerInfo(id, workerAddress, totalBytes);
      tWorkerInfo.updateUsedBytes(usedBytes);
      tWorkerInfo.updateFiles(true, currentFileIds);
      tWorkerInfo.updateLastUpdatedTimeMs();
      mWorkers.put(id, tWorkerInfo);
      mWorkerAddressToId.put(workerAddress, id);
      LOG.info("registerWorker(): " + tWorkerInfo);
    }

    synchronized (mRoot) {
      for (long fileId: currentFileIds) {
        Inode inode = mInodes.get(fileId);
        if (inode != null) {
          inode.addLocation(id, workerNetAddress);
        } else {
          LOG.warn("registerWorker failed to add fileId " + fileId);
        }
      }
    }

    return id;
  }

  public long getStarttimeMs() {
    return START_TIME_MS;
  }

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
}