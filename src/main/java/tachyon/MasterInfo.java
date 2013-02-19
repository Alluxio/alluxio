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
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.LogEventType;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * A global view of filesystem in master.
 * @author Haoyuan Li haoyuan@cs.berkeley.edu
 */
public class MasterInfo {
  private static final String SEPARATOR = "/";

  private final Logger LOG = LoggerFactory.getLogger(MasterInfo.class);

  private final InetSocketAddress MASTER_ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;

  private AtomicInteger mINodeCounter = new AtomicInteger(1);
  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // Root INode's id must be 1.
  private INodeFolder mRoot;

  private Map<Integer, INode> mINodes = new HashMap<Integer, INode>();

  private Map<Long, WorkerInfo> mWorkers = new HashMap<Long, WorkerInfo>();
  private Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();
  private BlockingQueue<WorkerInfo> mLostWorkers = new ArrayBlockingQueue<WorkerInfo>(32);

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
            INode tFile = mINodes.get(id);
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
    mRoot = new INodeFolder("", mINodeCounter.getAndIncrement(), -1);

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

  public int createFile(String path) throws FileAlreadyExistException, InvalidPathException {
    String parameters = CommonUtils.parametersToString(path);
    LOG.info("createFile" + parameters);

    String[] pathNames = getPathNames(path);

    synchronized (mRoot) {
      INode inode = getINode(path);
      if (inode != null && path.equals(SEPARATOR)) {
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
      inode = getINode(folderPath);
      if (inode == null || inode.isFile()) {
        Log.info("InvalidPathException: File " + path + " creation failed. Folder "
            + folderPath + " does not exist.");
        throw new InvalidPathException("File " + path + " creation failed. Folder "
            + folderPath + " does not exist.");
      }

      INode newFile = new INodeFile(name, mINodeCounter.getAndIncrement(), inode.getId());

      mINodes.put(newFile.getId(), newFile);
      ((INodeFolder) inode).addChild(newFile.getId());

      // TODO this two appends should be atomic;
      mMasterLogWriter.appendAndFlush(inode);
      mMasterLogWriter.appendAndFlush(newFile);

      LOG.info("createFile: File Created: " + newFile);
      return newFile.getId();
    }
  }

  public List<String> ls(String path) throws InvalidPathException, FileDoesNotExistException {
    List<String> ret = new ArrayList<String>();

    INode inode = getINode(path);

    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }

    if (inode.isFile()) {
      ret.add(path);
    } else {
      List<Integer> childernIds = ((INodeFolder) inode).getChildrenIds();

      synchronized (mRoot) {
        for (int k : childernIds) {
          inode = mINodes.get(childernIds.get(k));
          if (inode != null) {
            ret.add(path + "/" + inode.getName());
          }
        }
      }
    }

    return ret;
  }

  public void delete(int id) {
    LOG.info("delete(" + id + ")");
    // Only remove meta data from master. The data in workers will be evicted since no further
    // application can read them. (Based on LRU) TODO May change it to be active from V0.2. 
    synchronized (mRoot) {
      INode inode = mINodes.get(id);

      if (inode == null) {
        return;
      }

      if (inode.isDirectory()) {
        List<Integer> childrenIds = ((INodeFolder) inode).getChildrenIds();

        for (int childId : childrenIds) {
          delete(childId);
        }
      }

      INodeFolder parent = (INodeFolder) mINodes.get(inode.getParentId());
      parent.removeChild(inode.getId());
      mINodes.remove(inode.getId());
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
      INode inode = getINode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      delete(inode.getId());
    }
  }

  public boolean mkdir(String path) {
    // TODO
    return false;
  }

  private INode getINode(String path) throws InvalidPathException {
    String[] s = getPathNames(path);
    System.out.println("S :" + path + ":");
    for (int k = 0; k < s.length; k ++) {
      System.out.println(k + " +" + s[k] + "+");
    }
    return getINode(getPathNames(path));
  }

  private INode getINode(String[] pathNames) throws InvalidPathException {
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

    INode cur = mRoot;

    synchronized (mRoot) {
      for (int k = 1; k < pathNames.length; k ++) {
        String name = pathNames[k];
        if (cur.isFile()) {
          return null;
        }
        cur = ((INodeFolder) cur).getChild(name, mINodes);
      }

      return cur;
    }
  }

  private static void validatePath(String path) throws InvalidPathException {
    if (path == null || !path.startsWith(SEPARATOR) || 
        (path.length() > 1 && path.endsWith(SEPARATOR))) {
      throw new InvalidPathException("Path " + path + " is invalid.");
    }
  }

  private static String[] getPathNames(String path) throws InvalidPathException {
    validatePath(path);
    if (path.length() == 1 && path.equals(SEPARATOR)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(SEPARATOR);
  }

  private void writeCheckpoint() {
    LOG.info("Files recoveried from logs: ");
    MasterLogWriter checkpointWriter =
        new MasterLogWriter(Config.MASTER_CHECKPOINT_FILE + ".tmp");
    Queue<INode> nodesQueue = new LinkedList<INode>();
    synchronized (mRoot) {
      checkpointWriter.appendAndFlush(mRoot);
      nodesQueue.add(mRoot);
      while (!nodesQueue.isEmpty()) {
        INodeFolder tFolder = (INodeFolder) nodesQueue.poll();

        List<Integer> childrenIds = tFolder.getChildrenIds();
        for (int id : childrenIds) {
          INode tINode = mINodes.get(id);
          checkpointWriter.appendAndFlush(tINode);
          if (tINode.isDirectory()) {
            nodesQueue.add(tINode);
          }

          if (tINode.isPin()) {
            synchronized (mIdPinList) {
              mIdPinList.add(tINode.getId());
            }
          }
        }
      }

      //      for (RawColumnDatasetInfo dataset : mRawColumnDatasets.values()) {
      //        checkpointWriter.appendAndFlush(dataset);
      //        LOG.info(dataset.toString());
      //        maxDatasetId = Math.max(maxDatasetId, dataset.mId);
      //      }

      checkpointWriter.appendAndFlush(new CheckpointInfo(mINodeCounter.get()));
      checkpointWriter.close();

      CommonUtils.renameFile(Config.MASTER_CHECKPOINT_FILE + ".tmp", Config.MASTER_CHECKPOINT_FILE);

      CommonUtils.deleteFile(Config.MASTER_LOG_FILE);
    }
    LOG.info("Files recovery done. Current mDatasetCounter: " + mINodeCounter.get());
  }

  private void recoveryFromFile(String fileName, String msg) {
    MasterLogReader reader;

    File file = new File(fileName);
    if (!file.exists()) {
      LOG.info(msg + fileName + " does not exist.");
    } else {
      reader = new MasterLogReader(fileName);
      while (reader.hasNext()) {
        Pair<LogEventType, Object> pair = reader.getNextDatasetInfo();
        switch (pair.getFirst()) {
          case CheckpointInfo: {
            CheckpointInfo checkpointInfo = (CheckpointInfo) pair.getSecond();
            mINodeCounter.set(checkpointInfo.COUNTER_INODE);
            break;
          }
          case INode: {
            INode inode = (INode) pair.getSecond();
            System.out.println("Putting " + inode);
            if (Math.abs(inode.getId()) > mINodeCounter.get()) {
              mINodeCounter.set(Math.abs(inode.getId()));
            }
            if (inode.getId() > 0) {
              mINodes.put(inode.getId(), inode);
              if (inode.getId() == 1) {
                mRoot = (INodeFolder) inode;
              }
            } else {
              mINodes.remove(- inode.getId());
            }
            break;
          }
          //          case RawColumnDatasetInfo: {
          //            RawColumnDatasetInfo dataset = (RawColumnDatasetInfo) pair.getSecond();
          //            if (Math.abs(dataset.mId) > mDatasetCounter.get()) {
          //              mDatasetCounter.set(Math.abs(dataset.mId));
          //            }
          //
          //            System.out.println("Putting " + dataset);
          //            if (dataset.mId > 0) {
          //              mRawColumnDatasets.put(dataset.mId, dataset);
          //              mRawColumnDatasetPathToId.put(dataset.mPath, dataset.mId);
          //            } else {
          //              mRawColumnDatasets.remove(- dataset.mId);
          //              mRawColumnDatasetPathToId.remove(dataset.mPath);
          //            }
          //            break;
          //          }
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

  public String toHtml() {
    long timeMs = System.currentTimeMillis() - START_TIME_MS;
    StringBuilder sb = new StringBuilder("<h1> Tachyon has been running @ " + MASTER_ADDRESS + 
        " for " + CommonUtils.convertMillis(timeMs) + " </h1> \n");

    sb.append(mWhiteList.toHtml("WhiteList"));

    sb.append(mPinList.toHtml("PinList"));

    synchronized (mWorkers) {
      synchronized (mRoot) {
        sb.append("<h2>" + mWorkers.size() + " worker(s) are running: </h2>");
        List<Long> workerList = new ArrayList<Long>(mWorkers.keySet());
        Collections.sort(workerList);
        for (int k = 0; k < workerList.size(); k ++) {
          sb.append("<strong>Worker " + (k + 1) + " </strong>: " + 
              mWorkers.get(workerList.get(k)).toHtml() + "<br \\>");
        }

        sb.append("<h2>" + mINodes.size() + " File(s): </h2>");
        List<Integer> fileIdList = new ArrayList<Integer>(mINodes.keySet());
        Collections.sort(fileIdList);
        for (int k = 0; k < fileIdList.size(); k ++) {
          INode tINode = mINodes.get(fileIdList.get(k));
          sb.append("<strong>File " + (k + 1) + " </strong>: ");
          sb.append("ID: ").append(tINode.getId()).append("; ");
          sb.append("Path: ").append(tINode.getName()).append("; ");
          if (Config.DEBUG) {
            sb.append(tINode.toString());
          }
          sb.append("<br \\>");
        }
      }
    }

    return sb.toString();
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

  public ClientFileInfo getClientFileInfo(int id) throws FileDoesNotExistException {
    LOG.info("getClientFileInfo(" + id + ")");
    synchronized (mRoot) {
      INode inode = mINodes.get(id);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + id + " does not exist.");
      }
      ClientFileInfo ret = new ClientFileInfo();
      ret.id = inode.getId();
      ret.fileName = inode.getName();
      ret.checkpointPath = inode.getCheckpointPath();
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
      INode inode = getINode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return getClientFileInfo(inode.getId());
    }
  }

  public List<NetAddress> getFileLocations(int fileId) throws FileDoesNotExistException {
    LOG.info("getFileLocations: " + fileId);
    synchronized (mRoot) {
      INode inode = mINodes.get(fileId);
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
      INode inode = getINode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }
      return getFileLocations(inode.getId());
    }
  }

  public int getFileId(String filePath) throws InvalidPathException {
    LOG.info("getFileId(" + filePath + ")");
    INode inode = getINode(filePath);
    int ret = -1;
    if (inode != null) {
      ret = inode.getId();
    }
    LOG.info("getFileId(" + filePath + "): " + ret);
    return ret;
  }

  public long getNewUserId() {
    return mUserCounter.incrementAndGet();
  }

  private static String getName(String path) throws InvalidPathException {
    String[] pathNames = getPathNames(path);
    return pathNames[pathNames.length - 1];
  }

  public void renameFile(String srcFilePath, String dstFilePath) 
      throws FileDoesNotExistException, InvalidPathException {
    synchronized (mRoot) {
      INode inode = getINode(srcFilePath);
      if (inode == null) {
        throw new FileDoesNotExistException("File " + srcFilePath + " does not exist");
      }

      String dstName = getName(dstFilePath);
      inode.setName(dstName);

      INodeFolder parent = (INodeFolder) mINodes.get(inode.getParentId());
      parent.removeChild(inode.getId());

      String dstFileFolder = dstFilePath.substring(0, dstFilePath.length() - dstName.length() - 1);
      INode dstFolder = getINode(dstFileFolder);
      if (dstFolder == null || dstFolder.isFile()) {
        throw new FileDoesNotExistException("Folder " + dstFileFolder + " does not exist.");
      }
      ((INodeFolder) dstFolder).addChild(inode.getId());

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
      INode inode = mINodes.get(fileId);

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
      INode inode = mINodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exist.");
      }
      if (inode.isDirectory()) {
        throw new FileDoesNotExistException("File " + fileId + " is a folder.");
      }

      INodeFile tFile = (INodeFile) inode;

      if (tFile.isReady()) {
        if (tFile.getLength() != fileSizeBytes) {
          throw new SuspectedFileSizeException(fileId + ". Original Size: " +
              tFile.getLength() + ". New Size: " + fileSizeBytes);
        }
      } else {
        tFile.setLength(fileSizeBytes);
      }
      if (hasCheckpointed && !tFile.hasCheckpointed()) {
        tFile.setHasCheckpointed(true);
        tFile.setCheckpointPath(checkpointPath);
      }
      InetSocketAddress address = tWorkerInfo.ADDRESS;
      tFile.addLocation(workerId, new NetAddress(address.getHostName(), address.getPort()));
    }
  }

  public List<Integer> getPinList() {
    synchronized (mIdPinList) {
      List<Integer> ret = new ArrayList<Integer>();
      for (int id : mIdPinList) {
        ret.add(id);
      }
      return ret;
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
          INode inode = mINodes.get(id);
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
        INode inode = mINodes.get(fileId);
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
}