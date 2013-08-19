package tachyon.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.MasterClient;
import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.client.table.RawTable;
import tachyon.conf.UserConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;

/**
 * Tachyon's user client API. It contains a MasterClient and several WorkerClients
 * depending on how many workers the client program is interacting with.
 */
public class TachyonFS {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final long USER_QUOTA_UNIT_BYTES = UserConf.get().QUOTA_UNIT_BYTES;
  private final int USER_FAILED_SPACE_REQUEST_LIMITS = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  // The RPC client talks to the system master.
  private MasterClient mMasterClient = null;
  // The Master address.
  private InetSocketAddress mMasterAddress = null;
  // Whether use ZooKeeper or not
  private boolean mZookeeperMode;
  // Cached ClientFileInfo
  private Map<String, ClientFileInfo> mCachedClientFileInfos = 
      new HashMap<String, ClientFileInfo>();
  private Map<Integer, ClientFileInfo> mClientFileInfos = new HashMap<Integer, ClientFileInfo>();
  // Cached ClientBlockInfo
  //  private Map<Long, ClientBlockInfo> mClientBlockInfos = new HashMap<Long, ClientBlockInfo>();
  // The RPC client talks to the local worker if there is one.
  private WorkerClient mWorkerClient = null;
  // The local root data folder.
  private String mLocalDataFolder = null;
  // Whether the client is local or remote.
  private boolean mIsWorkerLocal = false;
  // The local data folder.
  private String mUserTempFolder = null;
  // The HDFS data folder
  private String mUserUnderfsTempFolder = null;
  private UnderFileSystem mUnderFileSystem = null;

  // The user id of the client.
  private long mUserId = 0;

  // All Blocks has been locked.
  private Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  // Each user facing block has a unique block lock id.
  private AtomicInteger mBlockLockId = new AtomicInteger(0);

  // Available memory space for this client.
  private Long mAvailableSpaceBytes;

  private boolean mConnected = false;

  private TachyonFS(InetSocketAddress masterAddress, boolean zookeeperMode) {
    mMasterAddress = masterAddress;
    mZookeeperMode = zookeeperMode;
    mAvailableSpaceBytes = 0L;
  }

  public static synchronized TachyonFS get(InetSocketAddress tachyonAddress) {
    return get(tachyonAddress, false);
  }

  public static synchronized TachyonFS get(InetSocketAddress tachyonAddress, boolean zookeeper) {
    return new TachyonFS(tachyonAddress, zookeeper);
  }

  public static synchronized TachyonFS get(String tachyonAddress) {
    boolean zookeeperMode = false;
    String tempAddress = tachyonAddress;
    if (tachyonAddress.startsWith("zookeeper://")) {
      zookeeperMode = true;
      tempAddress = tachyonAddress.substring(12);
    }
    String[] address = tempAddress.split(":");
    if (address.length != 2) {
      CommonUtils.illegalArgumentException("Illegal Tachyon Master Address: " + tachyonAddress);
    }
    return get(new InetSocketAddress(address[0], Integer.parseInt(address[1])), zookeeperMode);
  }

  public synchronized void accessLocalBlock(long blockId) {
    connect();
    if (mWorkerClient != null && mIsWorkerLocal) {
      try {
        mWorkerClient.accessBlock(blockId);
        return;
      } catch (TException e) {
        mWorkerClient = null;
        LOG.error(e.getMessage(), e);
      }
    }

    LOG.error("TachyonClient accessLocalBlock(" + blockId + ") failed");
  }

  public synchronized void addCheckpoint(int fid) throws IOException {
    connect();
    if (!mConnected) {
      throw new IOException("Failed to add checkpoint for file " + fid);
    }
    if (mWorkerClient != null) {
      try {
        mWorkerClient.addCheckpoint(mUserId, fid);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerClient = null;
        throw new IOException(e);
      } 
    }
  }

  public synchronized void cacheBlock(long blockId) throws IOException  {
    connect();
    if (!mConnected) {
      return;
    }

    if (mWorkerClient != null) {
      try {
        mWorkerClient.cacheBlock(mUserId, blockId);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerClient = null;
        throw new IOException(e);
      } 
    }
  }

  public synchronized void close() throws TException {
    if (mMasterClient != null) {
      mMasterClient.disconnect();
    }

    if (mWorkerClient != null) {
      mWorkerClient.returnSpace(mUserId, mAvailableSpaceBytes);
      mWorkerClient.close();
    }
  }

  // Lazy connection TODO This should be removed since the Thrift server has been fixed.
  public synchronized void connect() {
    if (mMasterClient != null) {
      return;
    }
    LOG.info("Trying to connect master @ " + mMasterAddress);
    mMasterClient = new MasterClient(mMasterAddress, mZookeeperMode);
    mConnected = mMasterClient.connect();

    if (!mConnected) {
      return;
    }

    try {
      mUserId = mMasterClient.getUserId();
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return;
    }

    InetSocketAddress workerAddress = null;
    NetAddress workerNetAddress = null;
    mIsWorkerLocal = false;
    try {
      String localHostName = InetAddress.getLocalHost().getCanonicalHostName();
      LOG.info("Trying to get local worker host : " + localHostName);
      workerNetAddress = mMasterClient.user_getWorker(false, localHostName);
      mIsWorkerLocal = true;
    } catch (NoWorkerException e) {
      LOG.info(e.getMessage());
      workerNetAddress = null;
    } catch (UnknownHostException e) {
      LOG.error(e.getMessage());
      workerNetAddress = null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      workerNetAddress = null;
    }

    if (workerNetAddress == null) {
      try {
        workerNetAddress = mMasterClient.user_getWorker(true, "");
      } catch (NoWorkerException e) {
        LOG.info(e.getMessage());
        workerNetAddress = null;
      } catch (TException e) {
        LOG.error(e.getMessage());
        mConnected = false;
        workerNetAddress = null;
      }
    }

    if (workerNetAddress == null) {
      LOG.info("No worker running in the system");
      return;
    }

    workerAddress = new InetSocketAddress(workerNetAddress.mHost, workerNetAddress.mPort);

    LOG.info("Connecting " + (mIsWorkerLocal ? "local" : "remote") + " worker @ " + workerAddress);
    mWorkerClient = new WorkerClient(workerAddress, mUserId);
    if (!mWorkerClient.open()) {
      LOG.error("Failed to connect " + (mIsWorkerLocal ? "local" : "remote") + 
          " worker @ " + workerAddress);
      mWorkerClient = null;
      return;
    }

    try {
      mLocalDataFolder = mWorkerClient.getDataFolder();
      mUserTempFolder = mWorkerClient.getUserTempFolder(mUserId);
      mUserUnderfsTempFolder = mWorkerClient.getUserUnderfsTempFolder(mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mLocalDataFolder = null;
      mUserTempFolder = null;
      mWorkerClient = null;
      return;
    }
  }

  public synchronized void completeFile(int fId) throws IOException {
    connect();
    try {
      mMasterClient.user_completeFile(fId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized File createAndGetUserTempFolder() {
    connect();

    if (mUserTempFolder == null) {
      return null;
    }

    File ret = new File(mUserTempFolder);

    if (!ret.exists()) {
      if (ret.mkdir()) {
        LOG.info("Folder " + ret + " was created!");
      } else {
        LOG.error("Failed to create folder " + ret);
        return null;
      }
    }

    return ret;
  }

  public synchronized String createAndGetUserUnderfsTempFolder() throws IOException {
    connect();

    if (mUserUnderfsTempFolder == null) {
      return null;
    }

    if (mUnderFileSystem == null) {
      mUnderFileSystem = UnderFileSystem.get(mUserUnderfsTempFolder);
    }

    mUnderFileSystem.mkdirs(mUserUnderfsTempFolder, true);

    return mUserUnderfsTempFolder;
  }

  public synchronized int createRawTable(String path, int columns) throws IOException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  public synchronized int createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);

    if (columns < 1 || columns > Constants.MAX_COLUMNS) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " +
          "bigger than " + Constants.MAX_COLUMNS);
    }

    try {
      return mMasterClient.user_createRawTable(path, columns, metadata);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
  }

  public synchronized int createFile(String path) throws IOException {
    return createFile(path, UserConf.get().DEFAULT_BLOCK_SIZE_BYTE);
  }

  public synchronized int createFile(String path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    int fid = -1;
    try {
      fid = mMasterClient.user_createFile(path, blockSizeByte);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return fid;
  }

  public synchronized int createFile(String path, String checkpointPath) throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    int fid = -1;
    try {
      fid = mMasterClient.user_createFileOnCheckpoint(path, checkpointPath);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return fid;
  }

  public synchronized boolean delete(int fid, boolean recursive) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      return mMasterClient.user_delete(fid, recursive);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized boolean delete(String path, boolean recursive)
      throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      return mMasterClient.user_delete(path, recursive);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public synchronized boolean exist(String path) throws IOException {
    return getFileId(path) != -1;
  }

  public synchronized long getBlockId(int fId, int blockIndex) throws IOException {
    ClientFileInfo info = mClientFileInfos.get(fId);
    if (info == null) {
      info = fetchClientFileInfo(fId);
      if (info != null) {
        mClientFileInfos.put(fId, info);
      } else {
        throw new IOException("File " + fId + " does not exist.");
      }
    }

    if (info.blockIds.size() > blockIndex) {
      return info.blockIds.get(blockIndex);
    }

    connect();
    try {
      return mMasterClient.user_getBlockId(fId, blockIndex);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public synchronized long getBlockIdBasedOnOffset(int fId, long offset) 
      throws IOException {
    ClientFileInfo info;
    if (!mClientFileInfos.containsKey(fId)) {
      info = fetchClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }
    info = mClientFileInfos.get(fId);

    int index = (int) (offset / info.getBlockSizeByte());

    return getBlockId(fId, index);
  }

  public synchronized ClientBlockInfo getClientBlockInfo(int fId, int blockIndex) 
      throws IOException {
    boolean fetch = false;
    if (!mClientFileInfos.containsKey(fId)) {
      fetch = true;
    }
    ClientFileInfo info = null;
    if (!fetch) {
      info = mClientFileInfos.get(fId);
      if (info.blockIds.size() <= blockIndex) {
        fetch = true;
      }
    }

    if (fetch) {
      connect();
      info = fetchClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }

    if (info == null) {
      throw new IOException("File " + fId + " does not exist.");
    }
    if (info.blockIds.size() <= blockIndex) {
      throw new IOException("BlockIndex " + blockIndex + " is out of the bound in file " + info);
    }

    try {
      return mMasterClient.user_getClientBlockInfo(info.blockIds.get(blockIndex));
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  private synchronized ClientFileInfo getClientFileInfo(String path, boolean useCachedMetadata)
      throws IOException { 
    connect();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret;
    path = CommonUtils.cleanPath(path);
    if (useCachedMetadata && mCachedClientFileInfos.containsKey(path)) {
      return mCachedClientFileInfos.get(path);
    }
    try {
      ret = mMasterClient.user_getClientFileInfoByPath(path);
    } catch (IOException e) {
      LOG.info(e.getMessage() + path);
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    // TODO LRU on this Map.
    if (ret != null && useCachedMetadata) {
      mCachedClientFileInfos.put(path, ret);
    } else {
      mCachedClientFileInfos.remove(path);
    }

    return ret;
  }

  private synchronized ClientFileInfo fetchClientFileInfo(int fid) {
    connect();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret = null;
    try {
      ret = mMasterClient.user_getClientFileInfoById(fid);
    } catch (IOException e) {
      LOG.info(e.getMessage() + fid);
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  public synchronized long getBlockSizeByte(int fId) {
    return mClientFileInfos.get(fId).getBlockSizeByte();
  }

  synchronized String getCheckpointPath(int fid) {
    ClientFileInfo info = mClientFileInfos.get(fid);
    if (info == null || !info.getCheckpointPath().equals("")) {
      info = fetchClientFileInfo(fid);
      mClientFileInfos.put(fid, info);
    }

    return info.getCheckpointPath();
  }

  public synchronized long getCreationTimeMs(int fId) {
    return mClientFileInfos.get(fId).getCreationTimeMs();
  }

  public synchronized List<Long> getFileBlockIdList(int fId) throws IOException {
    connect();

    ClientFileInfo info = mClientFileInfos.get(fId);
    if (info == null || !info.isComplete()) {
      info = fetchClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }

    if (info == null) {
      throw new IOException("File " + fId + " does not exist.");
    }

    return info.blockIds;
  }

  public synchronized List<ClientBlockInfo> getFileBlocks(int fid)
      throws IOException {
    // TODO Should read from mClientFileInfos if possible. Should add timeout to improve this.
    connect();
    if (!mConnected) {
      return null;
    }

    try {
      return mMasterClient.user_getFileBlocks(fid);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Get <code>TachyonFile</code> based on the path.
   * @param path file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(path, false);
  }

  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata) 
      throws IOException {
    path = CommonUtils.cleanPath(path);
    ClientFileInfo clientFileInfo = getClientFileInfo(path, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    mClientFileInfos.put(clientFileInfo.getId(), clientFileInfo);
    return new TachyonFile(this, clientFileInfo.getId());
  }

  /**
   * Get <code>TachyonFile</code> based on the file id.
   * @param fid file id.
   * @return TachyonFile of the file id, or null if the first does not exist.
   */
  public synchronized TachyonFile getFile(int fid) {
    if (!mClientFileInfos.containsKey(fid)) {
      ClientFileInfo clientFileInfo = fetchClientFileInfo(fid);
      if (clientFileInfo == null) {
        return null;
      }
      mClientFileInfos.put(fid, clientFileInfo);
    }
    return new TachyonFile(this, fid);
  }

  public synchronized int getFileId(String path) throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    int fid = -1;
    path = CommonUtils.cleanPath(path);
    try {
      fid = mMasterClient.user_getFileId(path);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
    return fid;
  }

  synchronized long getFileLength(int fid) {
    if (!mClientFileInfos.get(fid).isComplete()) {
      ClientFileInfo info = fetchClientFileInfo(fid);
      mClientFileInfos.put(fid, info);
    }
    return mClientFileInfos.get(fid).getLength();
  }

  synchronized long getNextBlockId(int fId) throws IOException {
    connect();
    try {
      return mMasterClient.user_createNewBlock(fId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  synchronized int getNumberOfBlocks(int fId) throws IOException {
    ClientFileInfo info = mClientFileInfos.get(fId);
    if (info == null || !info.isComplete()) {
      info = fetchClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }
    if (info == null) {
      throw new IOException("File " + fId + " does not exist.");
    }
    return info.getBlockIds().size();
  }

  public synchronized int getNumberOfFiles(String folderPath) 
      throws IOException {
    connect();
    try {
      return mMasterClient.user_getNumberOfFiles(folderPath);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  synchronized String getPath(int fid) {
    return mClientFileInfos.get(fid).getPath();
  }

  public synchronized RawTable getRawTable(String path)
      throws IOException {
    connect();
    path = CommonUtils.cleanPath(path);
    ClientRawTableInfo clientRawTableInfo;
    try {
      clientRawTableInfo = mMasterClient.user_getClientRawTableInfoByPath(path);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized RawTable getRawTable(int id) throws IOException {
    connect();
    ClientRawTableInfo clientRawTableInfo = null;
    try {
      clientRawTableInfo = mMasterClient.user_getClientRawTableInfoById(id);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized String getRootFolder() {
    connect();
    return mLocalDataFolder;
  }

  public int getBlockLockId() {
    return mBlockLockId.getAndIncrement();
  }

  public synchronized String getUnderfsAddress() throws IOException {
    connect();
    try {
      return mMasterClient.user_getUnderfsAddress();
    } catch (TException e) {
      throw new IOException(e.getMessage());
    }
  }

  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws IOException {
    connect();
    try {
      return mMasterClient.getWorkersInfo();
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized boolean hasLocalWorker() {
    connect();
    return (mIsWorkerLocal && mWorkerClient != null);
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  synchronized boolean isDirectory(int fid) {
    return mClientFileInfos.get(fid).isFolder();
  }

  synchronized boolean isInMemory(int fid) {
    ClientFileInfo info = fetchClientFileInfo(fid);
    mClientFileInfos.put(info.getId(), info);
    return info.isInMemory();
  }

  synchronized boolean isNeedPin(int fid) {
    return mClientFileInfos.get(fid).isNeedPin();
  }

  synchronized boolean isComplete(int fid) {
    if (!mClientFileInfos.get(fid).isComplete()) {
      mClientFileInfos.put(fid, fetchClientFileInfo(fid));
    }
    return mClientFileInfos.get(fid).isComplete();
  }

  public synchronized List<Integer> listFiles(String path, boolean recursive) throws IOException {
    connect();
    try {
      return mMasterClient.user_listFiles(path, recursive);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the 
   * <code>path</code> is a file, return its ClientFileInfo. 
   * @param path the target directory/file path
   * @return A list of ClientFileInfo
   * @throws IOException
   */
  public synchronized List<ClientFileInfo> listStatus(String path)
      throws IOException {
    connect();
    try {
      return mMasterClient.listStatus(path);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized List<String> ls(String path, boolean recursive) throws IOException {
    connect();
    try {
      return mMasterClient.user_ls(path, recursive);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Lock a block in the current TachyonFS.
   * @param blockId The id of the block to lock. <code>blockId</code> must be positive.
   * @param blockLockId The block lock id of the block of lock. 
   * <code>blockLockId</code> must be non-negative.
   * @return true if successfully lock the block, false otherwise (or invalid parameter).
   */
  synchronized boolean lockBlock(long blockId, int blockLockId) {
    if (blockId <= 0 || blockLockId < 0) {
      return false;
    }

    if (mLockedBlockIds.containsKey(blockId)) {
      mLockedBlockIds.get(blockId).add(blockLockId);
      return true;
    }

    connect();
    if (!mConnected || mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    try {
      mWorkerClient.lockBlock(blockId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    Set<Integer> lockIds = new HashSet<Integer>(4);
    lockIds.add(blockLockId);
    mLockedBlockIds.put(blockId, lockIds);
    return true;
  }

  /**
   * Create a directory if it does not exist.
   * @param path Directory path.
   * @return true if the folder is created succeefully. faluse otherwise.
   * @throws IOException
   */
  public synchronized boolean mkdir(String path) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }
    path = CommonUtils.cleanPath(path);
    try {
      return mMasterClient.user_mkdir(path);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public synchronized void outOfMemoryForPinFile(int fid) {
    connect();
    if (mConnected) {
      try {
        mMasterClient.user_outOfMemoryForPinFile(fid);
      } catch (TException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  /**
   * Read the whole local block.
   * @param blockId The id of the block to read.
   * @return <code>TachyonByteBuffer</code> containing the whole block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(long blockId) throws IOException {
    return readLocalByteBuffer(blockId, 0, -1);
  }

  /**
   * Read local block return a TachyonByteBuffer
   * @param blockId The id of the block.
   * @param offset The start position to read.
   * @param len The length to read. -1 represents read the whole block.
   * @return <code>TachyonByteBuffer</code> containing the block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(long blockId, long offset, long len) throws IOException {
    if (offset < 0) {
      throw new IOException("Offset can not be negative: " + offset);
    }
    if (len < 0 && len != -1) {
      throw new IOException("Length can not be negative except -1: " + len);
    }

    int blockLockId = getBlockLockId();
    if (!lockBlock(blockId, blockLockId)) {
      return null;
    }
    String rootFolder = getRootFolder();
    if (rootFolder != null) {
      String localFileName = rootFolder + Constants.PATH_SEPARATOR + blockId;
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");

        long fileLength = localFile.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error = String.format("Offset(%d) plus length(%d) is larger than file length(%d)", 
              offset, len, fileLength);
        }
        if (error != null) {
          localFile.close();
          throw new IOException(error);
        }

        if (len == -1) {
          len = fileLength - offset;
        }

        FileChannel localFileChannel = localFile.getChannel();
        ByteBuffer buf = localFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, len);
        localFileChannel.close();
        localFile.close();
        accessLocalBlock(blockId);
        return new TachyonByteBuffer(this, buf, blockId, blockLockId);
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.info("Failed to read local file " + localFileName + " because: \n" + e.getMessage());
      }
    }

    unlockBlock(blockId, blockLockId);
    return null;
  }

  public synchronized void releaseSpace(long releaseSpaceBytes) {
    mAvailableSpaceBytes += releaseSpaceBytes;
  }

  public synchronized boolean rename(String srcPath, String dstPath) 
      throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      if (srcPath.equals(dstPath) && exist(srcPath)) {
        return true;
      }

      mMasterClient.user_rename(srcPath, dstPath);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized boolean rename(int fId, String path) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_renameTo(fId, path);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized boolean requestSpace(long requestSpaceBytes) {
    connect();
    if (mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    int failedTimes = 0;
    while (mAvailableSpaceBytes < requestSpaceBytes) {
      if (mWorkerClient == null) {
        LOG.error("The current host does not have a Tachyon worker.");
        return false;
      }
      try {
        long toRequestSpaceBytes = 
            Math.max(requestSpaceBytes - mAvailableSpaceBytes, USER_QUOTA_UNIT_BYTES); 
        if (mWorkerClient.requestSpace(mUserId, toRequestSpaceBytes)) {
          mAvailableSpaceBytes += toRequestSpaceBytes;
        } else {
          LOG.info("Failed to request " + toRequestSpaceBytes + " bytes local space. " +
              "Time " + (failedTimes ++));
          if (failedTimes == USER_FAILED_SPACE_REQUEST_LIMITS) {
            return false;
          }
        }
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerClient = null;
        return false;
      }
    }

    if (mAvailableSpaceBytes < requestSpaceBytes) {
      return false;
    }

    mAvailableSpaceBytes -= requestSpaceBytes;

    return true;
  }

  public synchronized boolean unpinFile(int fid) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_unpinFile(fid);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  /**
   * Unlock a block in the current TachyonFS.
   * @param blockId The id of the block to unlock. <code>blockId</code> must be positive.
   * @param blockLockId The block lock id of the block of unlock. 
   * <code>blockLockId</code> must be non-negative.
   * @return true if successfully unlock the block with <code>blockLockId</code>,
   * false otherwise (or invalid parameter).
   */
  synchronized boolean unlockBlock(long blockId, int blockLockId) {
    if (blockId <= 0 || blockLockId < 0) {
      return false;
    }

    if (!mLockedBlockIds.containsKey(blockId)) {
      return true;
    }
    Set<Integer> lockIds = mLockedBlockIds.get(blockId);
    lockIds.remove(blockLockId);
    if (!lockIds.isEmpty()) {
      return true;
    }

    connect();
    if (!mConnected || mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    try {
      mWorkerClient.unlockBlock(blockId, mUserId);
      mLockedBlockIds.remove(blockId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public synchronized void updateRawTableMetadata(int id, ByteBuffer metadata)
      throws IOException {
    connect();
    try {
      mMasterClient.user_updateRawTableMetadata(id, metadata);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}