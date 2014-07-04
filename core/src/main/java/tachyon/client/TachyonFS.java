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
import java.util.ArrayList;
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
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.thrift.TachyonException;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.WorkerClient;

/**
 * Tachyon's user client API. It contains a MasterClient and several WorkerClients
 * depending on how many workers the client program is interacting with.
 */
public class TachyonFS {
  /**
   * Create a TachyonFS handler.
   *
   * @param tachyonPath
   *          a Tachyon path contains master address. e.g., tachyon://localhost:19998,
   *          tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS hanlder
   * @throws IOException
   */
  public static synchronized TachyonFS get(String tachyonPath) throws IOException {
    boolean zookeeperMode = false;
    String tempAddress = tachyonPath;
    if (tachyonPath.startsWith(Constants.HEADER)) {
      tempAddress = tachyonPath.substring(Constants.HEADER.length());
    } else if (tachyonPath.startsWith(Constants.HEADER_FT)) {
      zookeeperMode = true;
      tempAddress = tachyonPath.substring(Constants.HEADER_FT.length());
    } else {
      throw new IOException("Invalid Path: " + tachyonPath + ". Use " + Constants.HEADER
          + "host:port/ ," + Constants.HEADER_FT + "host:port/");
    }
    String masterAddress = tempAddress;
    if (tempAddress.contains(Constants.PATH_SEPARATOR)) {
      masterAddress = tempAddress.substring(0, tempAddress.indexOf(Constants.PATH_SEPARATOR));
    }
    if (masterAddress.split(":").length != 2) {
      CommonUtils.illegalArgumentException("Illegal Tachyon Master Address: " + tachyonPath);
    }
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new TachyonFS(new InetSocketAddress(masterHost, masterPort), zookeeperMode);
  }

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
  // private Map<Long, ClientBlockInfo> mClientBlockInfos = new HashMap<Long, ClientBlockInfo>();
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

  public synchronized void accessLocalBlock(long blockId) throws IOException {
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

  public synchronized boolean asyncCheckpoint(int fid) throws IOException {
    connect();
    try {
      return mWorkerClient.asyncCheckpoint(fid);
    } catch (TachyonException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized void cacheBlock(long blockId) throws IOException {
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

  /**
   * Cleans the given path, throwing an IOException rather than an InvalidPathException.
   *
   * @param path
   *          The path to clean
   * @return the cleaned path
   */
  private synchronized String cleanPathIOException(String path) throws IOException {
    try {
      return CommonUtils.cleanPath(path);
    } catch (InvalidPathException e) {
      throw new IOException(e.getMessage());
    }
  }

  public synchronized void close() throws TException {
    if (mMasterClient != null) {
      mMasterClient.cleanConnect();
    }

    if (mWorkerClient != null && mWorkerClient.isConnected()) {
      mWorkerClient.returnSpace(mUserId, mAvailableSpaceBytes);
      mWorkerClient.close();
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

  // Lazy connection TODO This should be removed since the Thrift server has been fixed.
  public synchronized void connect() throws IOException {
    if (mMasterClient != null) {
      return;
    }
    LOG.info("Trying to connect master @ " + mMasterAddress);
    mMasterClient = new MasterClient(mMasterAddress, mZookeeperMode);

    try {
      mMasterClient.connect();
      mConnected = true;
    } catch (TException e) {
      throw new IOException(e.getMessage(), e);
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
      String localHostName;
      try {
        localHostName =
            NetworkUtils.resolveHostName(InetAddress.getLocalHost().getCanonicalHostName());
      } catch (UnknownHostException e) {
        localHostName = InetAddress.getLocalHost().getCanonicalHostName();
      }
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
      LOG.error("Failed to connect " + (mIsWorkerLocal ? "local" : "remote") + " worker @ "
          + workerAddress);
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

  public synchronized File createAndGetUserTempFolder() throws IOException {
    connect();

    if (mUserTempFolder == null) {
      return null;
    }

    File ret = new File(mUserTempFolder);

    if (!ret.exists()) {
      if (ret.mkdir()) {
        CommonUtils.changeLocalFileToFullPermission(ret.getAbsolutePath());
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

  public synchronized int createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    connect();
    try {
      return mMasterClient.user_createDependency(parents, children, commandPrefix, data, comment,
          framework, frameworkVersion, dependencyType, childrenBlockSizeByte);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Create a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(String path) throws IOException {
    return createFile(path, UserConf.get().DEFAULT_BLOCK_SIZE_BYTE);
  }

  /**
   * Create a file in the system. It also creates necessary folders along the path.
   * // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @param blockSizeByte
   *          the block size of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(String path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    connect();
    if (!mConnected) {
      return -1;
    }
    path = cleanPathIOException(path);
    int fid = -1;
    try {
      fid = mMasterClient.user_createFile(path, blockSizeByte);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return fid;
  }

  /**
   * Create a file in the system with a pre-defined underfsPath. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file in Tachyon
   * @param underfsPath
   *          the path of the file in the underfs
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   */
  public synchronized int createFile(String path, String underfsPath) throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = cleanPathIOException(path);
    int fid = -1;
    try {
      fid = mMasterClient.user_createFileOnCheckpoint(path, underfsPath);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return fid;
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
    path = cleanPathIOException(path);
    if (columns < 1 || columns > CommonConf.get().MAX_COLUMNS) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " + "bigger than "
          + CommonConf.get().MAX_COLUMNS);
    }

    try {
      return mMasterClient.user_createRawTable(path, columns, metadata);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
  }

  /**
   * Delete the file denoted by the file id.
   *
   * @param fid
   *          file id
   * @param recursive
   *          if delete the path recursively.
   * @return true if deletion succeed (including the case the file does not exist in the first
   *         place), false otherwise.
   * @throws IOException
   */
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

  /**
   * Delete the file denoted by the path.
   *
   * @param path
   *          the file path
   * @param recursive
   *          if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   */
  public synchronized boolean delete(String path, boolean recursive) throws IOException {
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

  private synchronized ClientFileInfo fetchClientFileInfo(int fid) throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret = null;
    try {
      ret = mMasterClient.getClientFileInfoById(fid);
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

  public synchronized long getBlockIdBasedOnOffset(int fId, long offset) throws IOException {
    ClientFileInfo info;
    if (!mClientFileInfos.containsKey(fId)) {
      info = fetchClientFileInfo(fId);
      mClientFileInfos.put(fId, info);
    }
    info = mClientFileInfos.get(fId);

    int index = (int) (offset / info.getBlockSizeByte());

    return getBlockId(fId, index);
  }

  public int getBlockLockId() {
    return mBlockLockId.getAndIncrement();
  }

  public synchronized long getBlockSizeByte(int fId) {
    return mClientFileInfos.get(fId).getBlockSizeByte();
  }

  synchronized String getCheckpointPath(int fid) throws IOException {
    ClientFileInfo info = mClientFileInfos.get(fid);
    if (info == null || !info.getCheckpointPath().equals("")) {
      info = fetchClientFileInfo(fid);
      mClientFileInfos.put(fid, info);
    }

    return info.getCheckpointPath();
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
      if (info.isFolder() || info.blockIds.size() <= blockIndex) {
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
    if (info.isFolder()) {
      throw new IOException(new FileDoesNotExistException("File " + fId + " is a folder."));
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

  public synchronized ClientDependencyInfo getClientDependencyInfo(int did) throws IOException {
    connect();
    try {
      return mMasterClient.getClientDependencyInfo(did);
    } catch (TException e) {
      mConnected = false;
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
    path = cleanPathIOException(path);
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

  public synchronized long getCreationTimeMs(int fId) {
    return mClientFileInfos.get(fId).getCreationTimeMs();
  }

  /**
   * Get <code>TachyonFile</code> based on the file id.
   *
   * NOTE: This *will* use cached file metadata, and so will not see changes to dynamic properties,
   * such as the pinned flag. This is also different from the behavior of getFile(path), which
   * by default will not use cached metadata.
   *
   * @param fid
   *          file id.
   * @return TachyonFile of the file id, or null if the file does not exist.
   */
  public synchronized TachyonFile getFile(int fid) throws IOException {
    return getFile(fid, true);
  }

  /**
   * Get <code>TachyonFile</code> based on the file id. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   *
   * @return TachyonFile of the file id, or null if the file does not exist.
   */
  public synchronized TachyonFile getFile(int fid, boolean useCachedMetadata) throws IOException {
    if (!useCachedMetadata || !mClientFileInfos.containsKey(fid)) {
      ClientFileInfo clientFileInfo = fetchClientFileInfo(fid);
      if (clientFileInfo == null) {
        return null;
      }
      mClientFileInfos.put(fid, clientFileInfo);
    }
    return new TachyonFile(this, fid);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   *
   * @param path
   *          file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(path, false);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   */
  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata)
      throws IOException {
    path = cleanPathIOException(path);
    ClientFileInfo clientFileInfo = getClientFileInfo(path, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    mClientFileInfos.put(clientFileInfo.getId(), clientFileInfo);
    return new TachyonFile(this, clientFileInfo.getId());
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

  public synchronized List<ClientBlockInfo> getFileBlocks(int fid) throws IOException {
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

  public synchronized List<String> getFileHosts(int fileId) throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }

    List<NetAddress> adresses = getFileNetAddresses(fileId);
    List<String> ret = new ArrayList<String>(adresses.size());
    for (NetAddress address : adresses) {
      ret.add(address.mHost);
      if (address.mHost.endsWith(".ec2.internal")) {
        ret.add(address.mHost.substring(0, address.mHost.length() - 13));
      }
    }

    return ret;
  }

  public synchronized int getFileId(String path) throws IOException {
    connect();
    if (!mConnected) {
      return -1;
    }
    int fid = -1;
    path = cleanPathIOException(path);
    try {
      fid = mMasterClient.user_getFileId(path);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
    return fid;
  }

  synchronized long getFileLength(int fid) throws IOException {
    if (!mClientFileInfos.get(fid).isComplete()) {
      ClientFileInfo info = fetchClientFileInfo(fid);
      mClientFileInfos.put(fid, info);
    }
    return mClientFileInfos.get(fid).getLength();
  }

  public synchronized List<NetAddress> getFileNetAddresses(int fileId) throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }

    List<NetAddress> ret = new ArrayList<NetAddress>();
    try {
      List<ClientBlockInfo> blocks = mMasterClient.user_getFileBlocks(fileId);
      Set<NetAddress> locationSet = new HashSet<NetAddress>();
      for (ClientBlockInfo info : blocks) {
        locationSet.addAll(info.getLocations());
      }
      ret.addAll(locationSet);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return ret;
  }

  public synchronized List<List<String>> getFilesHosts(List<Integer> fileIds) throws IOException {
    List<List<String>> ret = new ArrayList<List<String>>();
    for (int k = 0; k < fileIds.size(); k ++) {
      ret.add(getFileHosts(fileIds.get(k)));
    }

    return ret;
  }

  public synchronized List<List<NetAddress>> getFilesNetAddresses(List<Integer> fileIds)
      throws IOException {
    List<List<NetAddress>> ret = new ArrayList<List<NetAddress>>();
    for (int k = 0; k < fileIds.size(); k ++) {
      ret.add(getFileNetAddresses(fileIds.get(k)));
    }

    return ret;
  }

  /**
   * Returns the local filename for the block if that file exists on the local file system. This is
   * an alpha power-api feature for applications that want short-circuit-read files directly. There
   * is no guarantee that the file still exists after this call returns, as Tachyon may evict blocks
   * from memory at any time.
   *
   * @param blockId
   *          The id of the block.
   * @return filename on local file system or null if file not present on local file system.
   */
  String getLocalFilename(long blockId) throws IOException {
    String rootFolder = getRootFolder();
    if (rootFolder != null) {
      String localFileName = CommonUtils.concat(rootFolder, blockId);
      File file = new File(localFileName);
      if (file.exists()) {
        return localFileName;
      }
    }
    return null;
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

  public synchronized int getNumberOfFiles(String folderPath) throws IOException {
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

  public synchronized RawTable getRawTable(String path) throws IOException {
    connect();
    path = cleanPathIOException(path);
    ClientRawTableInfo clientRawTableInfo;
    try {
      clientRawTableInfo = mMasterClient.user_getClientRawTableInfoByPath(path);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized String getRootFolder() throws IOException {
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

  public synchronized boolean hasLocalWorker() throws IOException {
    connect();
    return (mIsWorkerLocal && mWorkerClient != null);
  }

  synchronized boolean isComplete(int fid) throws IOException {
    if (!mClientFileInfos.get(fid).isComplete()) {
      mClientFileInfos.put(fid, fetchClientFileInfo(fid));
    }
    return mClientFileInfos.get(fid).isComplete();
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  synchronized boolean isDirectory(int fid) {
    return mClientFileInfos.get(fid).isFolder();
  }

  synchronized boolean isInMemory(int fid) throws IOException {
    ClientFileInfo info = fetchClientFileInfo(fid);
    mClientFileInfos.put(info.getId(), info);
    return info.isInMemory();
  }

  synchronized boolean isNeedPin(int fid) {
    return mClientFileInfos.get(fid).isNeedPin();
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
   *
   * @param path
   *          the target directory/file path
   * @return A list of ClientFileInfo
   * @throws IOException
   */
  public synchronized List<ClientFileInfo> listStatus(String path) throws IOException {
    connect();
    try {
      return mMasterClient.listStatus(path);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Lock a block in the current TachyonFS.
   *
   * @param blockId
   *          The id of the block to lock. <code>blockId</code> must be positive.
   * @param blockLockId
   *          The block lock id of the block of lock. <code>blockLockId</code> must be non-negative.
   * @return true if successfully lock the block, false otherwise (or invalid parameter).
   */
  synchronized boolean lockBlock(long blockId, int blockLockId) throws IOException {
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
   * Return a list of files/directories under the given path.
   *
   * @param path
   *          the path in the TFS.
   * @param recursive
   *          whether or not to list files/directories under path recursively.
   * @return a list of files/directories under path if recursive is false, or files/directories
   *         under its subdirectories (sub-subdirectories, and so forth) if recursive is true, or
   *         null
   *         if the content of path is empty, i.e., no files found under path.
   * @throws IOException
   *           if some TException is thrown when trying to read content of path.
   */
  public synchronized List<String> ls(String path, boolean recursive) throws IOException {
    connect();
    try {
      return mMasterClient.user_ls(path, recursive);
    } catch (FileDoesNotExistException e) {
      mConnected = false;
      return null;
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Create a directory if it does not exist. The method also creates necessary non-existing
   * parent folders.
   *
   * @param path
   *          Directory path.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  public synchronized boolean mkdir(String path) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }
    path = cleanPathIOException(path);
    try {
      return mMasterClient.user_mkdir(path);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public synchronized void outOfMemoryForPinFile(int fid) throws IOException {
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
   *
   * @param blockId
   *          The id of the block to read.
   * @return <code>TachyonByteBuffer</code> containing the whole block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(long blockId) throws IOException {
    return readLocalByteBuffer(blockId, 0, -1);
  }

  /**
   * Read local block return a TachyonByteBuffer
   *
   * @param blockId
   *          The id of the block.
   * @param offset
   *          The start position to read.
   * @param len
   *          The length to read. -1 represents read the whole block.
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
    String localFileName = getLocalFilename(blockId);
    if (localFileName != null) {
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");

        long fileLength = localFile.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
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

  /**
   * Rename the srcPath to the dstPath
   *
   * @param srcPath
   * @param dstPath
   * @return true if succeed, false otherwise.
   * @throws IOException
   */
  public synchronized boolean rename(String srcPath, String dstPath) throws IOException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      if (srcPath.equals(dstPath) && exist(srcPath)) {
        return true;
      }

      return mMasterClient.user_rename(srcPath, dstPath);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
  }

  public synchronized void reportLostFile(int fileId) throws IOException {
    connect();
    try {
      mMasterClient.user_reportLostFile(fileId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized void requestFilesInDependency(int depId) throws IOException {
    connect();
    try {
      mMasterClient.user_requestFilesInDependency(depId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized boolean requestSpace(long requestSpaceBytes) throws IOException {
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
          LOG.info("Failed to request " + toRequestSpaceBytes + " bytes local space. " + "Time "
              + (failedTimes ++));
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

  /**
   * Print out the string representation of this Tachyon server address.
   *
   * @return the string representation like tachyon://host:port or tachyon-ft://host:port
   */
  @Override
  public String toString() {
    return (mZookeeperMode ? Constants.HEADER_FT : Constants.HEADER) + mMasterAddress.toString();
  }

  /**
   * Unlock a block in the current TachyonFS.
   *
   * @param blockId
   *          The id of the block to unlock. <code>blockId</code> must be positive.
   * @param blockLockId
   *          The block lock id of the block of unlock. <code>blockLockId</code> must be
   *          non-negative.
   * @return true if successfully unlock the block with <code>blockLockId</code>,
   *         false otherwise (or invalid parameter).
   */
  synchronized boolean unlockBlock(long blockId, int blockLockId) throws IOException {
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

  /**
   * Sets the "pinned" flag for the given file. Pinned files are never evicted
   * by Tachyon until they are unpinned.
   *
   * Calling setPinned() on a folder will recursively set the "pinned" flag on
   * all of that folder's children. This may be an expensive operation for
   * folders with many files/subfolders.
   */
  public synchronized void setPinned(int fid, boolean pinned) throws IOException {
    connect();
    if (!mConnected) {
      throw new IOException("Could not connect to Tachyon Master");
    }

    try {
      mMasterClient.user_setPinned(fid, pinned);
    } catch (TException e) {
      LOG.error(e.getMessage());
      CommonUtils.runtimeException(e);
    }
  }

  /** Alias for setPinned(fid, true). */
  public synchronized void pinFile(int fid) throws IOException {
    setPinned(fid, true);
  }

  /** Alias for setPinned(fid, false). */
  public synchronized void unpinFile(int fid) throws IOException {
    setPinned(fid, false);
  }

  /** Returns true if the given file or folder has its "pinned" flag set. */
  public synchronized boolean isPinned(int fid, boolean useCachedMetadata)
      throws IOException {
    ClientFileInfo info;
    if (!useCachedMetadata || !mClientFileInfos.containsKey(fid)) {
      info = fetchClientFileInfo(fid);
      mClientFileInfos.put(fid, info);
    }
    info = mClientFileInfos.get(fid);

    return info.isNeedPin();
  }

  public synchronized void updateRawTableMetadata(int id, ByteBuffer metadata) throws IOException {
    connect();
    try {
      mMasterClient.user_updateRawTableMetadata(id, metadata);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }
}
