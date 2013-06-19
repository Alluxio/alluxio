package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.MasterClient;
import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;

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
  // Cached ClientFileInfo
  private Map<String, ClientFileInfo> mCachedClientFileInfos = 
      new HashMap<String, ClientFileInfo>();
  private Map<Integer, ClientFileInfo> mClientFileInfos = new HashMap<Integer, ClientFileInfo>();
  // Cached ClientBlockInfo
  private Map<Long, ClientBlockInfo> mClientBlockInfos = new HashMap<Long, ClientBlockInfo>();
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

  // All files has been locked.
  private Set<Long> mLockedBlockIds = new HashSet<Long>();

  // Available memory space for this client.
  private Long mAvailableSpaceBytes;

  private boolean mConnected = false;

  private TachyonFS(InetSocketAddress masterAddress) {
    mMasterAddress = masterAddress;
    mAvailableSpaceBytes = 0L;
  }

  public static synchronized TachyonFS get(InetSocketAddress tachyonAddress) {
    return new TachyonFS(tachyonAddress);
  }

  public static synchronized TachyonFS get(String tachyonAddress) {
    String[] address = tachyonAddress.split(":");
    if (address.length != 2) {
      CommonUtils.illegalArgumentException("Illegal Tachyon Master Address: " + tachyonAddress);
    }
    return get(new InetSocketAddress(address[0], Integer.parseInt(address[1])));
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

    LOG.error("TachyonClient accessLocalFile(" + fid + ") failed");
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

  /**
   * This API is not recommended to use.
   * 
   * @param id file id
   * @param path existing checkpoint path
   * @return true if the checkpoint path is added successfully, false otherwise.
   * @throws IOException
   * @throws TException
   */
//  public synchronized boolean addCheckpointPath(int id, String path)
//      throws IOException, TException {
//    connect();
//
//    UnderFileSystem hdfsClient = UnderFileSystem.get(path);
//    long fileSizeBytes = hdfsClient.getFileSize(path);
//
//    try {
//      if (mMasterClient.addCheckpoint(-1, id, fileSizeBytes, path)) {
//        ClientFileInfo tInfo = mClientFileInfos.get(id);
//        tInfo.length = fileSizeBytes;
//        tInfo.checkpointPath = path;
//        return true;
//      }
//    } catch (FileDoesNotExistException e) {
//      throw new IOException(e);
//    } catch (SuspectedFileSizeException e) {
//      throw new IOException(e);
//    } catch (BlockInfoException e) {
//      throw new IOException(e);
//    }
//
//    return false;
//  }

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

  // Lazy connection
  // TODO This should be removed since the Thrift server has been fixed.
  public synchronized void connect() {
    if (mMasterClient != null) {
      return;
    }
    LOG.info("Trying to connect master @ " + mMasterAddress);
    mMasterClient = new MasterClient(mMasterAddress);
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
    } catch (NoLocalWorkerException e) {
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
      } catch (NoLocalWorkerException e) {
        LOG.info(e.getMessage());
        workerNetAddress = null;
      } catch (TException e) {
        LOG.error(e.getMessage());
        mConnected = false;
        workerNetAddress = null;
      }
    }

    if (workerNetAddress == null) {
      LOG.error("No worker running in the system");
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

  public synchronized void close() throws TException {
    if (mMasterClient != null) {
      mMasterClient.disconnect();
    }

    if (mWorkerClient != null) {
      mWorkerClient.returnSpace(mUserId, mAvailableSpaceBytes);
      mWorkerClient.close();
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

  public synchronized boolean rename(String srcPath, String dstPath) 
      throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_renameFile(srcPath, dstPath);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized long getBlockId(int fId, int blockIndex) throws IOException {
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
      info = getClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }

    if (info == null) {
      throw new IOException("File " + fId + " does not exist.");
    }
    if (info.blockIds.size() <= blockIndex) {
      throw new IOException("BlockIndex " + blockIndex + " is out of the bound in file " + info);
    }

    return info.blockIds.get(blockIndex);
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
      info = getClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }

    if (info == null) {
      throw new IOException("File " + fId + " does not exist.");
    }
    if (info.blockIds.size() <= blockIndex) {
      throw new IOException("BlockIndex " + blockIndex + " is out of the bound in file " + info);
    }

    throw new RuntimeException("Implement");
//    return info.blockIds.get(blockIndex);
  }

  private synchronized ClientFileInfo getClientFileInfo(String path, boolean useCachedMetadata) { 
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

  private synchronized ClientFileInfo getClientFileInfo(int fid) {
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

  synchronized String getCheckpointPath(int fid) {
    if (mClientFileInfos.get(fid).getCheckpointPath().equals("")) {
      mClientFileInfos.put(fid, getClientFileInfo(fid));
    }
    return mClientFileInfos.get(fid).getCheckpointPath();
  }

  public synchronized List<Long> getFileBlockIdList(int fId) throws IOException {
    connect();
    ClientFileInfo info = getClientFileInfo(fId);
    mClientFileInfos.put(fId, info);

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

  public long getBlockSizeByte(int fId) {
    return mClientFileInfos.get(fId).getBlockSizeByte();
  }

//  public synchronized List<List<NetAddress>> getFilesNetAddresses(List<Integer> fids) 
//      throws IOException {
//    List<List<NetAddress>> ret = new ArrayList<List<NetAddress>>();
//    for (int k = 0; k < fids.size(); k ++) {
//      ret.add(getFileBlocks(fids.get(k)));
//    }
//
//    return ret;
//  }

//  public synchronized List<ClientBlockInfo> getFileHosts(int fid)
//      throws IOException {
//    connect();
//    if (!mConnected) {
//      return null;
//    }
//
//    List<NetAddress> adresses = getFileBlocks(fid);
//    List<String> ret = new ArrayList<String>(adresses.size());
//    for (NetAddress address: adresses) {
//      ret.add(address.mHost);
//      if (address.mHost.endsWith(".ec2.internal")) {
//        ret.add(address.mHost.substring(0, address.mHost.length() - 13));
//      }
//    }
//
//    return ret;
//  }
//
//  public synchronized List<List<String>> getFilesHosts(List<Integer> fids) 
//      throws IOException {
//    List<List<String>> ret = new ArrayList<List<String>>();
//    for (int k = 0; k < fids.size(); k ++) {
//      ret.add(getFileHosts(fids.get(k)));
//    }
//
//    return ret;
//  }

  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(path, false);
  }

  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata) {
    path = CommonUtils.cleanPath(path);
    ClientFileInfo clientFileInfo = getClientFileInfo(path, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    mClientFileInfos.put(clientFileInfo.getId(), clientFileInfo);
    return new TachyonFile(this, clientFileInfo.getId());
  }

  public synchronized TachyonFile getFile(int fid) {
    if (!mClientFileInfos.containsKey(fid)) {
      ClientFileInfo clientFileInfo = getClientFileInfo(fid);
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
    return mClientFileInfos.get(fid).getLength();
  }

  synchronized long getNextBlockId(int fId) {
    // TODO: implement it.
    throw new RuntimeException("Implement it");
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

  synchronized boolean isFolder(int fid) {
    return mClientFileInfos.get(fid).isFolder();
  }

  synchronized boolean isInMemory(int fid) {
    // TODO Make this query the master.
    return mClientFileInfos.get(fid).isInMemory();
  }

  synchronized boolean isNeedPin(int fid) {
    return mClientFileInfos.get(fid).isNeedPin();
  }

  synchronized boolean isComplete(int fid) {
    if (!mClientFileInfos.get(fid).isComplete()) {
      mClientFileInfos.put(fid, getClientFileInfo(fid));
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

  public synchronized List<ClientFileInfo> listStatus(String path)
      throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }
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

  public synchronized boolean lockBlock(long blockId) {
    if (mLockedBlockIds.contains(blockId)) {
      return true;
    }
    connect();
    if (!mConnected || mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    try {
      mWorkerClient.lockBlock(blockId, mUserId);
      mLockedBlockIds.add(blockId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Create a directory if it does not exist.
   * @param path Directory path.
   * @return The inode ID of the directory if it is successfully created. -1 if not.
   * @throws IOException
   */
  public synchronized int mkdir(String path) throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    int id = -1;
    try {
      id = mMasterClient.user_mkdir(path);
    } catch (TException e) {
      LOG.info(e.getMessage());
      id = -1;
    }
    return id;
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

  public synchronized void releaseSpace(long releaseSpaceBytes) {
    mAvailableSpaceBytes += releaseSpaceBytes;
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

  public synchronized boolean unlockBlock(long blockId) {
    if (!mLockedBlockIds.contains(blockId)) {
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

  public synchronized boolean rename(int fId, String path) {
    // TODO Auto-generated method stub
    throw new RuntimeException("Implement");
  }

  public int getNumberOfBlocks(int fId) {
    // TODO Auto-generated method stub
    return 0;
  }
}