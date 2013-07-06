package tachyon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.conf.MasterConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
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
import tachyon.thrift.TachyonException;

/**
 * A global view of filesystem in master.
 */
public class MasterInfo {
  public static final String COL = "COL_";

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final InetSocketAddress MASTER_ADDRESS;
  private final long START_TIME_NS_PREFIX;
  private final long START_TIME_MS;

  private final MasterConf MASTER_CONF;

  private AtomicInteger mInodeCounter = new AtomicInteger(0);
  private AtomicInteger mUserCounter = new AtomicInteger(0);
  private AtomicInteger mWorkerCounter = new AtomicInteger(0);

  // Root Inode's id must be 1.
  private InodeFolder mRoot;

  private Map<Integer, Inode> mInodes = new HashMap<Integer, Inode>();

  private Map<Long, MasterWorkerInfo> mWorkers = new HashMap<Long, MasterWorkerInfo>();
  private Map<InetSocketAddress, Long> mWorkerAddressToId = new HashMap<InetSocketAddress, Long>();
  private BlockingQueue<MasterWorkerInfo> mLostWorkers = new ArrayBlockingQueue<MasterWorkerInfo>(32);

  // TODO Check the logic related to this two lists.
  private PrefixList mWhiteList;
  private PrefixList mPinList;
  private Set<Integer> mFileIdPinList;

  private MasterLogWriter mMasterLogWriter;

  private HeartbeatThread mHeartbeatThread;

  /**
   * Master info periodical status check.
   */
  public class MasterInfoHeartbeatExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      LOG.debug("System status checking.");

      Set<Long> lostWorkers = new HashSet<Long>();

      synchronized (mWorkers) {
        for (Entry<Long, MasterWorkerInfo> worker: mWorkers.entrySet()) {
          if (CommonUtils.getCurrentMs() - worker.getValue().getLastUpdatedTimeMs() 
              > MASTER_CONF.WORKER_TIMEOUT_MS) {
            LOG.error("The worker " + worker.getValue() + " got timed out!");
            mLostWorkers.add(worker.getValue());
            lostWorkers.add(worker.getKey());
          }
        }
        for (long workerId: lostWorkers) {
          MasterWorkerInfo workerInfo = mWorkers.get(workerId);
          mWorkerAddressToId.remove(workerInfo.getAddress());
          mWorkers.remove(workerId);
        }
      }

      boolean hadFailedWorker = false;

      while (mLostWorkers.size() != 0) {
        hadFailedWorker = true;
        MasterWorkerInfo worker = mLostWorkers.poll();

        // TODO these a lock is not efficient. Since node failure is rare, this is fine for now.
        synchronized (mRoot) {
          try {
            for (long blockId: worker.getBlocks()) {
              int fileId = BlockInfo.computeInodeId(blockId);
              InodeFile tFile = (InodeFile) mInodes.get(fileId);
              if (tFile != null) {
                int blockIndex = BlockInfo.computeBlockIndex(blockId);
                tFile.removeLocation(blockIndex, worker.getId());
                if (!tFile.hasCheckpointed() && tFile.getBlockLocations(blockIndex).size() == 0) {
                  LOG.info("Block " + blockId + " got lost from worker " + worker.getId() + " .");
                } else {
                  LOG.info("Block " + blockId + " only lost an in memory copy from worker " +
                      worker.getId());
                }
              } 
            }
          } catch (BlockInfoException e) {
            LOG.error(e);
          }
        }
      }

      if (hadFailedWorker) {
        LOG.warn("Restarting failed workers.");
        try {
          java.lang.Runtime.getRuntime().exec(CommonConf.get().TACHYON_HOME + 
              "/bin/restart-failed-workers.sh");
        } catch (IOException e) {
          LOG.error(e.getMessage());
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

  public MasterInfo(InetSocketAddress address) throws IOException {
    MASTER_CONF = MasterConf.get();

    mRoot = new InodeFolder("", mInodeCounter.incrementAndGet(), -1);
    mInodes.put(mRoot.getId(), mRoot);

    MASTER_ADDRESS = address;
    START_TIME_MS = System.currentTimeMillis();
    // TODO This name need to be changed.
    START_TIME_NS_PREFIX = START_TIME_MS - (START_TIME_MS % 1000000);

    mWhiteList = new PrefixList(MASTER_CONF.WHITELIST);
    mPinList = new PrefixList(MASTER_CONF.PINLIST);
    mFileIdPinList = Collections.synchronizedSet(new HashSet<Integer>());

    // TODO Fault recovery: need user counter info;
    recoveryFromLog();
    writeCheckpoint();

    mMasterLogWriter = new MasterLogWriter(MASTER_CONF.LOG_FILE);

    mHeartbeatThread = new HeartbeatThread("Master Heartbeat", 
        new MasterInfoHeartbeatExecutor(), MASTER_CONF.HEARTBEAT_INTERVAL_MS);
    mHeartbeatThread.start();
  }

  public boolean addCheckpoint(long workerId, int fileId, long length, String checkpointPath)
      throws FileDoesNotExistException, SuspectedFileSizeException, BlockInfoException {
    LOG.info(CommonUtils.parametersToString(workerId, fileId, length, checkpointPath));

    if (workerId != -1) {
      MasterWorkerInfo tWorkerInfo = getWorkerInfo(workerId);
      tWorkerInfo.updateLastUpdatedTimeMs();
    }

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

      if (tFile.isComplete()) {
        if (tFile.getLength() != length) {
          throw new SuspectedFileSizeException(fileId + ". Original Size: " +
              tFile.getLength() + ". New Size: " + length);
        }
      } else {
        tFile.setLength(length);
        needLog = true;
      }

      if (!tFile.hasCheckpointed()) {
        tFile.setCheckpointPath(checkpointPath);
        needLog = true;
      }

      tFile.setComplete();

      if (needLog) {
        mMasterLogWriter.append(tFile, true);
      }
      return true;
    }
  }

  /**
   * A worker cache a block in its memory.
   * 
   * @param workerId
   * @param workerUsedBytes
   * @param blockId
   * @param length
   * @throws FileDoesNotExistException
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException 
   */
  public void cacheBlock(long workerId, long workerUsedBytes, long blockId, long length)
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
        tFile.addBlock(new BlockInfo(tFile, blockIndex, length));
        mMasterLogWriter.append(tFile, true);
      }

      InetSocketAddress address = tWorkerInfo.ADDRESS;
      tFile.addLocation(blockIndex, workerId,
          new NetAddress(address.getHostName(), address.getPort()));
    }
  }

  public int createFile(String path, long blockSizeByte)
      throws FileAlreadyExistException, InvalidPathException {
    return createFile(true, path, false, -1, null, blockSizeByte);
  }

  // TODO Make this API better.
  public int createFile(boolean recursive, String path, boolean directory, int columns,
      ByteBuffer metadata, long blockSizeByte)
          throws FileAlreadyExistException, InvalidPathException {
    LOG.debug("createFile" + CommonUtils.parametersToString(path));

    String[] pathNames = getPathNames(path);

    synchronized (mRoot) {
      Inode inode = getInode(pathNames);
      if (inode != null) {
        LOG.info("FileAlreadyExistException: File " + path + " already exist.");
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
      if (inode == null) {
        int succeed = 0;
        if (recursive) {
          succeed = createFile(true, folderPath, true, -1, null, blockSizeByte);
        }
        if (!recursive || succeed <= 0) {
          LOG.info("InvalidPathException: File " + path + " creation failed. Folder "
              + folderPath + " does not exist.");
          throw new InvalidPathException("InvalidPathException: File " + path + " creation " +
              "failed. Folder " + folderPath + " does not exist.");
        } else {
          inode = mInodes.get(succeed);
        }
      } else if (inode.isFile()) {
        LOG.info("InvalidPathException: File " + path + " creation failed. "
            + folderPath + " is a file.");
        throw new InvalidPathException("File " + path + " creation failed. "
            + folderPath + " is a file");
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
        ret = new InodeFile(name, mInodeCounter.incrementAndGet(), inode.getId(), blockSizeByte);
        String curPath = getPath(ret);
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
      ((InodeFolder) inode).addChild(ret.getId());

      // TODO this two appends should be atomic;
      mMasterLogWriter.append(inode, false);
      mMasterLogWriter.append(ret, false);
      mMasterLogWriter.flush();

      LOG.debug("createFile: File Created: " + ret + " parent: " + inode);
      return ret.getId();
    }
  }

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

  public void completeFile(int fileId) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        throw new FileDoesNotExistException("File " + fileId + " does not exit.");
      }
      if (!inode.isFile()) {
        throw new FileDoesNotExistException("File " + fileId + " is not a file.");
      }

      ((InodeFile) inode).setComplete();
    }
  }

  public int createRawTable(String path, int columns, ByteBuffer metadata)
      throws FileAlreadyExistException, InvalidPathException, TableColumnException {
    LOG.info("createRawTable" + CommonUtils.parametersToString(path, columns));

    if (columns <= 0 || columns >= Constants.MAX_COLUMNS) {
      throw new TableColumnException("Column " + columns + " should between 0 to " + 
          Constants.MAX_COLUMNS);
    }

    int id = createFile(true, path, true, columns, metadata, 0);

    for (int k = 0; k < columns; k ++) {
      mkdir(path + Constants.PATH_SEPARATOR + COL + k);
    }

    return id;
  }

  /**
   * Delete a file.
   * @param fileId The file to be deleted. 
   * @param recursive
   * @return
   * @throws TachyonException
   */
  public boolean delete(int fileId, boolean recursive) throws TachyonException {
    LOG.info("delete(" + fileId + ")");
    boolean succeed = true;
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);

      if (inode == null) {
        return true;
      }

      if (inode.isDirectory()) {
        List<Integer> childrenIds = ((InodeFolder) inode).getChildrenIds();

        if (!recursive && childrenIds.size() != 0) {
          return false;
        }
        for (int childId : childrenIds) {
          succeed = succeed && delete(childId, recursive);
        }
      }

      InodeFolder parent = (InodeFolder) mInodes.get(inode.getParentId());
      parent.removeChild(inode.getId());
      mInodes.remove(inode.getId());
      if (inode.isFile()) {
        String checkpointPath = ((InodeFile) inode).getCheckpointPath();
        if (!checkpointPath.equals("")) {
          UnderFileSystem ufs = UnderFileSystem.get(checkpointPath);
          
          try {
            if (!ufs.delete(checkpointPath, true)) {
              return false;
            }
          } catch (IOException e) {
            throw new TachyonException(e.getMessage());
          }
        }

        List<Pair<Long, Long>> blockIdWorkerIdList = ((InodeFile) inode).getBlockIdWorkerIdPairs();
        synchronized (mWorkers) {
          for (Pair<Long, Long> blockIdWorkerId: blockIdWorkerIdList) {
            MasterWorkerInfo workerInfo = mWorkers.get(blockIdWorkerId.getSecond());
            if (workerInfo != null) {
              workerInfo.updateToRemovedBlock(true, blockIdWorkerId.getFirst());
            }
          }
        }

        if (((InodeFile) inode).isPin()) {
          synchronized (mFileIdPinList) {
            mFileIdPinList.remove(inode.getId());
          }
        }
      }
      inode.reverseId();

      // TODO this following append should be atomic
      mMasterLogWriter.append(inode, false);
      mMasterLogWriter.append(parent, false);
      mMasterLogWriter.flush();

      return succeed;
    }
  }

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

  public long getCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  public ClientFileInfo getClientFileInfo(int id) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(id);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + id + " does not exist.");
      }

      ClientFileInfo ret = inode.generateClientFileInfo(getPath(inode));
      LOG.debug("getClientFileInfo(" + id + "): "  + ret);
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

  public String getFileNameById(int fileId) throws FileDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("FileId " + fileId + " does not exist");
      }
      return getPath(inode);
    }
  }

  public ClientBlockInfo getClientBlockInfo(long blockId)
      throws FileDoesNotExistException, IOException, BlockInfoException {
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

  public List<ClientBlockInfo> getFileLocations(int fileId)
      throws FileDoesNotExistException, IOException {
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

  public List<ClientBlockInfo> getFileLocations(String path) 
      throws FileDoesNotExistException, InvalidPathException, IOException {
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
    LOG.debug("getFileId(" + filePath + ")");
    Inode inode = getInode(filePath);
    int ret = -1;
    if (inode != null) {
      ret = inode.getId();
    }
    LOG.debug("getFileId(" + filePath + "): " + ret);
    return ret;
  }

  public List<Integer> getFilesIds(List<String> pathList)
      throws InvalidPathException, FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>(pathList.size());
    for (int k = 0; k < pathList.size(); k ++) {
      ret.addAll(listFiles(pathList.get(k), true));
    }
    return ret;
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

  public InetSocketAddress getMasterAddress() {
    return MASTER_ADDRESS;
  }

  private static String getName(String path) throws InvalidPathException {
    String[] pathNames = getPathNames(path);
    return pathNames[pathNames.length - 1];
  }

  public long getNewUserId() {
    return mUserCounter.incrementAndGet();
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

  private String getPath(Inode inode) {
    synchronized (mRoot) {
      if (inode.getId() == 1) {
        return "/";
      }
      if (inode.getParentId() == 1) {
        return Constants.PATH_SEPARATOR + inode.getName();
      }
      return getPath(mInodes.get(inode.getParentId())) + Constants.PATH_SEPARATOR + inode.getName();
    }
  }

  private static String[] getPathNames(String path) throws InvalidPathException {
    CommonUtils.validatePath(path);
    if (path.length() == 1 && path.equals(Constants.PATH_SEPARATOR)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(Constants.PATH_SEPARATOR);
  }

  public List<String> getPinList() {
    return mPinList.getList();
  }

  public List<Integer> getPinIdList() {
    synchronized (mFileIdPinList) {
      List<Integer> ret = new ArrayList<Integer>();
      for (int id : mFileIdPinList) {
        ret.add(id);
      }
      return ret;
    }
  }

  public int getRawTableId(String path) throws InvalidPathException {
    Inode inode = getInode(path);
    if (inode == null || inode.isFile() || !((InodeFolder) inode).isRawTable()) {
      return -1;
    }
    return inode.getId();
  }

  public long getStarttimeMs() {
    return START_TIME_MS;
  }

  public long getUsedBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  public NetAddress getWorker(boolean random, String host) throws NoLocalWorkerException {
    synchronized (mWorkers) {
      if (random) {
        int index = new Random(mWorkerAddressToId.size()).nextInt(mWorkerAddressToId.size());
        for (InetSocketAddress address: mWorkerAddressToId.keySet()) {
          if (index == 0) {
            LOG.debug("getRandomWorker: " + address);
            return new NetAddress(address.getHostName(), address.getPort());
          }
          index --;
        }
        for (InetSocketAddress address: mWorkerAddressToId.keySet()) {
          LOG.debug("getRandomWorker: " + address);
          return new NetAddress(address.getHostName(), address.getPort());
        }
      } else {
        for (InetSocketAddress address: mWorkerAddressToId.keySet()) {
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
    throw new NoLocalWorkerException("getLocalWorker: no local worker on " + host);
  }

  public int getWorkerCount() {
    synchronized (mWorkers) {
      return mWorkers.size();
    }
  }

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

  public List<ClientWorkerInfo> getWorkersInfo() {
    List<ClientWorkerInfo> ret = new ArrayList<ClientWorkerInfo>();

    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers.values()) {
        ret.add(worker.generateClientWorkerInfo());
      }
    }

    return ret;
  }

  public List<String> getWhiteList() {
    return mWhiteList.getList();
  }

  public List<Integer> listFiles(String path, boolean recursive) 
      throws InvalidPathException, FileDoesNotExistException {
    List<Integer> ret = new ArrayList<Integer>(); 
    synchronized (mRoot) {
      Inode inode = getInode(path);
      if (inode == null) {
        throw new FileDoesNotExistException(path);
      }

      if (inode.isFile()) {
        ret.add(inode.getId());
      } else if (recursive) {
        Queue<Integer> queue = new LinkedList<Integer>();
        queue.addAll(((InodeFolder) inode).getChildrenIds());

        while (!queue.isEmpty()) {
          int id = queue.poll();
          inode = mInodes.get(id);

          if (inode.isDirectory()) {
            queue.addAll(((InodeFolder) inode).getChildrenIds());
          } else {
            ret.add(id);
          }
        }
      }
    }

    return ret;
  }

  public List<String> ls(String path, boolean recursive) 
      throws InvalidPathException, FileDoesNotExistException {
    List<String> ret = new ArrayList<String>();

    Inode inode = getInode(path);

    if (inode == null) {
      throw new FileDoesNotExistException(path);
    }

    if (inode.isFile()) {
      ret.add(path);
    } else if (recursive) {
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

  public int mkdir(String path)
      throws FileAlreadyExistException, InvalidPathException {
    return createFile(true, path, true, -1, null, 0);
  }

  private void recoveryFromFile(String fileName, String msg) throws IOException {
    MasterLogReader reader;

    UnderFileSystem ufs = UnderFileSystem.get(fileName);
    if (!ufs.exists(fileName)) {
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

  private void recoveryFromLog() throws IOException {
    recoveryFromFile(MASTER_CONF.CHECKPOINT_FILE, "Master Checkpoint file ");
    recoveryFromFile(MASTER_CONF.LOG_FILE, "Master Log file ");
  }

  public long registerWorker(NetAddress workerNetAddress, long totalBytes,
      long usedBytes, List<Long> currentBlockIds) throws BlockInfoException {
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
      for (long blockId: currentBlockIds) {
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

  private void rename(Inode srcInode, String dstPath)
      throws FileAlreadyExistException, InvalidPathException, FileDoesNotExistException {
    if (getInode(dstPath) != null) {
      throw new FileAlreadyExistException("Failed to rename: " + dstPath + " already exist");
    }

    String dstName = getName(dstPath);
    String dstFolderPath = dstPath.substring(0, dstPath.length() - dstName.length() - 1);

    // If we are renaming into the root folder
    if (dstFolderPath.isEmpty()) {
      dstFolderPath = "/";
    }

    Inode dstFolderInode = getInode(dstFolderPath);
    if (dstFolderInode == null || dstFolderInode.isFile()) {
      throw new FileDoesNotExistException("Failed to rename: " + dstFolderPath + 
          " does not exist.");
    }

    srcInode.setName(dstName);
    InodeFolder parent = (InodeFolder) mInodes.get(srcInode.getParentId());
    parent.removeChild(srcInode.getId());
    srcInode.setParentId(dstFolderInode.getId());
    ((InodeFolder) dstFolderInode).addChild(srcInode.getId());

    // TODO The following should be done atomically.
    mMasterLogWriter.append(parent, false);
    mMasterLogWriter.append(dstFolderInode, false);
    mMasterLogWriter.append(srcInode, false);
    mMasterLogWriter.flush();
  }

  public void rename(int fileId, String dstPath) 
      throws FileDoesNotExistException, FileAlreadyExistException, InvalidPathException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(fileId);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + fileId + " does not exist");
      }

      rename(inode, dstPath);
    }
  }

  public void rename(String srcPath, String dstPath)
      throws FileAlreadyExistException, FileDoesNotExistException, InvalidPathException {
    synchronized (mRoot) {
      Inode inode = getInode(srcPath);
      if (inode == null) {
        throw new FileDoesNotExistException("Failed to rename: " + srcPath + " does not exist");
      }

      rename(inode, dstPath);
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

      ((InodeFile) inode).setPin(false);
      synchronized (mFileIdPinList) {
        mFileIdPinList.remove(fileId);
      }

      mMasterLogWriter.append(inode, true);
    }
  }

  public void updateRawTableMetadata(int tableId, ByteBuffer metadata)
      throws TableDoesNotExistException {
    synchronized (mRoot) {
      Inode inode = mInodes.get(tableId);

      if (inode == null || inode.getInodeType() != InodeType.RawTable) {
        throw new TableDoesNotExistException("Table " + tableId + " does not exist.");
      }

      ((InodeRawTable) inode).updateMetadata(metadata);

      mMasterLogWriter.append(inode, true);
    }
  }

  public Command workerHeartbeat(long workerId, long usedBytes, List<Long> removedBlockIds) 
      throws BlockInfoException {
    LOG.debug("WorkerId: " + workerId);
    synchronized (mWorkers) {
      MasterWorkerInfo tWorkerInfo = mWorkers.get(workerId);

      if (tWorkerInfo == null) {
        LOG.info("worker_heartbeat(): Does not contain worker with ID " + workerId +
            " . Send command to let it re-register.");
        return new Command(CommandType.Register, new ArrayList<Long>());
      }

      tWorkerInfo.updateUsedBytes(usedBytes);
      tWorkerInfo.updateBlocks(false, removedBlockIds);
      tWorkerInfo.updateToRemovedBlocks(false, removedBlockIds);
      tWorkerInfo.updateLastUpdatedTimeMs();

      synchronized (mRoot) {
        for (long blockId : removedBlockIds) {
          int fileId = BlockInfo.computeInodeId(blockId);
          int blockIndex = BlockInfo.computeBlockIndex(blockId);
          Inode inode = mInodes.get(fileId);
          if (inode == null) {
            LOG.error("File " + fileId + " does not exist");
          } else if (inode.isFile()) {
            ((InodeFile) inode).removeLocation(blockIndex, workerId);
            LOG.debug("File " + fileId + " block " + blockIndex + 
                " was evicted from worker " + workerId);
          }
        }
      }

      List<Long> toRemovedBlocks = tWorkerInfo.getToRemovedBlocks();
      if (toRemovedBlocks.size() != 0) {
        return new Command(CommandType.Free, toRemovedBlocks);
      }
    }

    return new Command(CommandType.Nothing, new ArrayList<Long>());
  }

  private void writeCheckpoint() throws IOException {
    LOG.info("Write checkpoint on files recoveried from logs. ");
    MasterLogWriter checkpointWriter =
        new MasterLogWriter(MASTER_CONF.CHECKPOINT_FILE + ".tmp");
    Queue<Inode> nodesQueue = new LinkedList<Inode>();
    synchronized (mRoot) {
      checkpointWriter.append(mRoot, true);
      nodesQueue.add(mRoot);
      while (!nodesQueue.isEmpty()) {
        InodeFolder tFolder = (InodeFolder) nodesQueue.poll();

        List<Integer> childrenIds = tFolder.getChildrenIds();
        for (int id : childrenIds) {
          Inode tInode = mInodes.get(id);
          checkpointWriter.append(tInode, true);
          if (tInode.isDirectory()) {
            nodesQueue.add(tInode);
          } else if (((InodeFile) tInode).isPin()) {
            synchronized (mFileIdPinList) {
              mFileIdPinList.add(tInode.getId());
            }
          }
        }
      }

      checkpointWriter.appendAndFlush(new CheckpointInfo(mInodeCounter.get()));
      checkpointWriter.close();

      UnderFileSystem ufs = UnderFileSystem.get(MASTER_CONF.CHECKPOINT_FILE);
      ufs.delete(MASTER_CONF.CHECKPOINT_FILE, false);
      ufs.rename(MASTER_CONF.CHECKPOINT_FILE + ".tmp", MASTER_CONF.CHECKPOINT_FILE);
      ufs.delete(MASTER_CONF.CHECKPOINT_FILE + ".tmp", false);

      ufs = UnderFileSystem.get(MASTER_CONF.LOG_FILE);
      ufs.delete(MASTER_CONF.LOG_FILE, false);
    }
    LOG.info("Files recovery done. Current mInodeCounter: " + mInodeCounter.get());
  }

  public void stop() {
    mHeartbeatThread.shutdown();
  }
}