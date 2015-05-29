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

package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.client.table.RawTable;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterClient;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.WorkerClient;

/**
 * Tachyon's user client API. It contains a MasterClient and several WorkerClients depending on how
 * many workers the client program is interacting with.
 */
public class TachyonFS extends AbstractTachyonFS {

  /**
   * Create a TachyonFS handler.
   *
   * @param tachyonPath a Tachyon path contains master address. e.g., tachyon://localhost:19998,
   *        tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @throws IOException
   * @see #get(tachyon.TachyonURI, tachyon.conf.TachyonConf)
   */
  @Deprecated
  public static synchronized TachyonFS get(String tachyonPath) throws IOException {
    return get(new TachyonURI(tachyonPath), new TachyonConf());
  }

  /**
   * Create a TachyonFS handler.
   * 
   * @param tachyonURI a Tachyon URI contains master address. e.g., tachyon://localhost:19998,
   *        tachyon://localhost:19998/ab/c.txt
   * @param tachyonConf The TachyonConf instance.
   * @return the corresponding TachyonFS handler
   * @throws IOException
   */
  public static synchronized TachyonFS get(final TachyonURI tachyonURI, TachyonConf tachyonConf)
      throws IOException {
    Preconditions.checkNotNull(tachyonConf, "Could not pass null TachyonConf instance.");
    if (tachyonURI == null) {
      throw new IOException("Tachyon Uri cannot be null. Use " + Constants.HEADER + "host:port/ ,"
          + Constants.HEADER_FT + "host:port/");
    } else {
      String scheme = tachyonURI.getScheme();
      if (scheme == null || tachyonURI.getHost() == null || tachyonURI.getPort() == -1
          || (!Constants.SCHEME.equals(scheme) && !Constants.SCHEME_FT.equals(scheme))) {
        throw new IOException("Invalid Tachyon URI: " + tachyonURI + ". Use " + Constants.HEADER
            + "host:port/ ," + Constants.HEADER_FT + "host:port/");
      }

      boolean useZookeeper = scheme.equals(Constants.SCHEME_FT);
      tachyonConf.set(Constants.USE_ZOOKEEPER, Boolean.toString(useZookeeper));
      tachyonConf.set(Constants.MASTER_HOSTNAME, tachyonURI.getHost());
      tachyonConf.set(Constants.MASTER_PORT, Integer.toString(tachyonURI.getPort()));

      return get(tachyonConf);
    }
  }

  /**
   * Create a TachyonFS handler.
   *
   * @param masterHost master host details
   * @param masterPort port master listens on
   * @param zkMode use zookeeper
   * @return the corresponding TachyonFS handler
   * @throws IOException
   */
  public static synchronized TachyonFS get(String masterHost, int masterPort, boolean zkMode)
      throws IOException {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.MASTER_HOSTNAME, masterHost);
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(masterPort));
    tachyonConf.set(Constants.USE_ZOOKEEPER, Boolean.toString(zkMode));
    return get(tachyonConf);
  }

  /**
   * Create a TachyonFS handler.
   *
   * @param tachyonConf The TachyonConf instance.
   *
   * @return the corresponding TachyonFS handler
   * @throws IOException
   */
  public static synchronized TachyonFS get(TachyonConf tachyonConf) throws IOException {
    Preconditions.checkArgument(tachyonConf != null, "Could not pass null TachyonConf instance.");
    return new TachyonFS(tachyonConf);
  }

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final int mUserFailedSpaceRequestLimits;
  private final ExecutorService mExecutorService;

  // The RPC client talks to the system master.
  private final MasterClient mMasterClient;
  // The Master address.
  private final InetSocketAddress mMasterAddress;
  // The RPC client talks to the local worker if there is one.
  private final WorkerClient mWorkerClient;
  private final Closer mCloser = Closer.create();
  // Whether use ZooKeeper or not
  private final boolean mZookeeperMode;
  // Cached ClientFileInfo
  private final Map<String, ClientFileInfo> mPathToClientFileInfo =
      new HashMap<String, ClientFileInfo>();
  private final Map<Integer, ClientFileInfo> mIdToClientFileInfo =
      new HashMap<Integer, ClientFileInfo>();

  private UnderFileSystem mUnderFileSystem;

  // All Blocks has been locked.
  private final Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  // Mapping from block id to path of the block locked
  private final Map<Long, String> mLockedBlockIdToPath = new HashMap<Long, String>();

  // Each user facing block has a unique block lock id.
  private final AtomicInteger mBlockLockId = new AtomicInteger(0);

  private TachyonURI mRootUri = null;

  private TachyonFS(TachyonConf tachyonConf) throws IOException {
    super(tachyonConf);

    String masterHost = tachyonConf.get(Constants.MASTER_HOSTNAME, NetworkUtils.getLocalHostName());
    int masterPort = tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);

    mMasterAddress = new InetSocketAddress(masterHost, masterPort);
    mZookeeperMode = mTachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false);

    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.daemon("client-heartbeat-%d"));

    mMasterClient =
        mCloser.register(new MasterClient(mMasterAddress, mExecutorService, mTachyonConf));
    mWorkerClient = mCloser.register(new WorkerClient(mMasterClient, mExecutorService,
        mTachyonConf));

    mUserFailedSpaceRequestLimits =
        mTachyonConf.getInt(Constants.USER_FAILED_SPACE_REQUEST_LIMITS, 0);

    String scheme = mZookeeperMode ? Constants.SCHEME_FT : Constants.SCHEME;
    String authority = mMasterAddress.getHostName() + ":" + mMasterAddress.getPort();
    mRootUri = new TachyonURI(scheme, authority, TachyonURI.SEPARATOR);
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId the local block's id
   * @throws IOException
   */
  synchronized void accessLocalBlock(long blockId) throws IOException {
    if (mWorkerClient.isLocal()) {
      mWorkerClient.accessBlock(blockId);
    }
  }

  /**
   * Notify the worker that the checkpoint file of the file mFileId has been added.
   * 
   * @param fid the file id
   * @throws IOException
   */
  synchronized void addCheckpoint(int fid) throws IOException {
    mWorkerClient.addCheckpoint(fid);
  }

  /**
   * Notify the worker to checkpoint the file asynchronously
   * 
   * @param fid the file id
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  synchronized boolean asyncCheckpoint(int fid) throws IOException {
    return mWorkerClient.asyncCheckpoint(fid);
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param blockId the block id
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mWorkerClient.cacheBlock(blockId);
  }

  /**
   * Notify the worker the block is canceled.
   * 
   * @param blockId the block id
   * @throws IOException
   */
  public synchronized void cancelBlock(long blockId) throws IOException {
    mWorkerClient.cancelBlock(blockId);
  }

  /**
   * Close the client. Close the connections to both to master and worker
   * 
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      mCloser.close();
    } finally {
      mExecutorService.shutdown();
    }
  }

  /**
   * The file is complete.
   * 
   * @param fid the file id
   * @throws IOException
   */
  synchronized void completeFile(int fid) throws IOException {
    mMasterClient.user_completeFile(fid);
  }

  /**
   * Create a user UnderFileSystem temporary folder and return it
   * 
   * @param ufsConf the configuration of UnderFileSystem
   * @return the UnderFileSystem temporary folder
   * @throws IOException
   */
  synchronized String createAndGetUserUfsTempFolder(Object ufsConf) throws IOException {
    String tmpFolder = mWorkerClient.getUserUfsTempFolder();
    if (tmpFolder == null) {
      return null;
    }

    if (mUnderFileSystem == null) {
      mUnderFileSystem = UnderFileSystem.get(tmpFolder, ufsConf, mTachyonConf);
    }

    mUnderFileSystem.mkdirs(tmpFolder, true);

    return tmpFolder;
  }

  /**
   * Create a Dependency
   * 
   * @param parents the dependency's input files
   * @param children the dependency's output files
   * @param commandPrefix
   * @param data
   * @param comment
   * @param framework
   * @param frameworkVersion
   * @param dependencyType the dependency's type, Wide or Narrow
   * @param childrenBlockSizeByte the block size of the dependency's output files
   * @return the dependency's id
   * @throws IOException
   */
  public synchronized int createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    return mMasterClient.user_createDependency(parents, children, commandPrefix, data, comment,
        framework, frameworkVersion, dependencyType, childrenBlockSizeByte);
  }

  /**
   * Creates a new file in the file system.
   * 
   * @param path The path of the file
   * @param ufsPath The path of the file in the under file system. If this is empty, the file does
   *        not exist in the under file system yet.
   * @param blockSizeByte The size of the block in bytes. It is -1 iff ufsPath is non-empty.
   * @param recursive Creates necessary parent folders if true, not otherwise.
   * @return The file id, which is globally unique.
   */
  @Override
  public synchronized int createFile(TachyonURI path, TachyonURI ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    validateUri(path);
    return mMasterClient.user_createFile(path.getPath(), ufsPath.toString(), blockSizeByte,
        recursive);
  }

  /**
   * Create a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   * 
   * @param path the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException If file already exists, or path is invalid.
   */
  @Deprecated
  public synchronized int createFile(String path) throws IOException {
    return createFile(new TachyonURI(path));
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path the RawTable's path
   * @param columns number of columns it has
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(TachyonURI path, int columns) throws IOException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path the RawTable's path
   * @param columns number of columns it has
   * @param metadata the meta data of the RawTable
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(TachyonURI path, int columns, ByteBuffer metadata)
      throws IOException {
    validateUri(path);
    int maxColumns = mTachyonConf.getInt(Constants.MAX_COLUMNS, 1000);
    if (columns < 1 || columns > maxColumns) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " + "bigger than "
          + maxColumns);
    }

    return mMasterClient.user_createRawTable(path.getPath(), columns, metadata);
  }

  /**
   * Deletes a file or folder
   * 
   * @param fileId The id of the file / folder. If it is not -1, path parameter is ignored.
   *        Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty iff id is not -1.
   * @param recursive If fileId or path represents a non-empty folder, delete the folder recursively
   *        or not
   * @return true if deletes successfully, false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean delete(int fileId, TachyonURI path, boolean recursive)
      throws IOException {
    validateUri(path);
    return mMasterClient.user_delete(fileId, path.getPath(), recursive);
  }

  /**
   * Delete the file denoted by the path.
   * 
   * @param path the file path
   * @param recursive if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   */
  @Deprecated
  public synchronized boolean delete(String path, boolean recursive) throws IOException {
    return delete(new TachyonURI(path), recursive);
  }

  /**
   * Return whether the file exists or not
   * 
   * @param path the file's path in Tachyon file system
   * @return true if it exists, false otherwise
   * @throws IOException
   */
  public synchronized boolean exist(TachyonURI path) throws IOException {
    return getFileStatus(-1, path, false) != null;
  }

  /**
   * Get the block id by the file id and block index. it will check whether the file and the block
   * exist.
   * 
   * @param fileId the file id
   * @param blockIndex The index of the block in the file.
   * @return the block id if exists
   * @throws IOException if the file does not exist, or connection issue.
   */
  public synchronized long getBlockId(int fileId, int blockIndex) throws IOException {
    ClientFileInfo info = getFileStatus(fileId, true);

    if (info == null) {
      throw new IOException("File " + fileId + " does not exist.");
    }

    if (info.blockIds.size() > blockIndex) {
      return info.blockIds.get(blockIndex);
    }

    return mMasterClient.user_getBlockId(fileId, blockIndex);
  }

  /**
   * @return a new block lock id
   */
  synchronized int getBlockLockId() {
    return mBlockLockId.getAndIncrement();
  }

  /**
   * Get a ClientBlockInfo by blockId
   * 
   * @param blockId the id of the block
   * @return the ClientBlockInfo of the specified block
   * @throws IOException
   */
  synchronized ClientBlockInfo getClientBlockInfo(long blockId) throws IOException {
    return mMasterClient.user_getClientBlockInfo(blockId);
  }

  /**
   * Get a ClientDependencyInfo by the dependency id
   * 
   * @param depId the dependency id
   * @return the ClientDependencyInfo of the specified dependency
   * @throws IOException
   */
  public synchronized ClientDependencyInfo getClientDependencyInfo(int depId) throws IOException {
    return mMasterClient.getClientDependencyInfo(depId);
  }

  /**
   * Get <code>TachyonFile</code> based on the file id.
   * 
   * NOTE: This *will* use cached file metadata, and so will not see changes to dynamic properties,
   * such as the pinned flag. This is also different from the behavior of getFile(path), which by
   * default will not use cached metadata.
   * 
   * @param fid file id.
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
    ClientFileInfo clientFileInfo = getFileStatus(fid, TachyonURI.EMPTY_URI, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, fid, mTachyonConf);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   * 
   * @param path file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  public synchronized TachyonFile getFile(TachyonURI path) throws IOException {
    validateUri(path);
    return getFile(path, false);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   * 
   * @param path file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  @Deprecated
  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(new TachyonURI(path));
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see changes
   * to the file's pin setting, or other dynamic properties.
   */
  @Deprecated
  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata)
      throws IOException {
    return getFile(new TachyonURI(path), useCachedMetadata);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see changes
   * to the file's pin setting, or other dynamic properties.
   */
  public synchronized TachyonFile getFile(TachyonURI path, boolean useCachedMetadata)
      throws IOException {
    validateUri(path);
    ClientFileInfo clientFileInfo = getFileStatus(-1, path, useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo.getId(), mTachyonConf);
  }

  /**
   * Get all the blocks' info of the file
   * 
   * @param fid the file id
   * @return the list of the blocks' info
   * @throws IOException
   */
  public synchronized List<ClientBlockInfo> getFileBlocks(int fid) throws IOException {
    // TODO Should read from mClientFileInfos if possible. Should add timeout to improve this.
    return mMasterClient.user_getFileBlocks(fid, "");
  }

  /**
   * Get file id by the path. It will check if the path exists.
   * 
   * @param path the path in Tachyon file system
   * @return the file id if exists, -1 otherwise
   */
  public synchronized int getFileId(TachyonURI path) {
    try {
      ClientFileInfo fileInfo = getFileStatus(-1, path, false);
      return fileInfo == null ? -1 : fileInfo.getId();
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Gets file status.
   * 
   * @param cache ClientFileInfo cache.
   * @param key the key in the cache.
   * @param fileId the id of the queried file. If it is -1, uses path.
   * @param path the path of the queried file. If fielId is not -1, this parameter is ignored.
   * @param useCachedMetaData whether to use the cached data or not.
   * @return the clientFileInfo.
   * @throws IOException
   */
  private synchronized <K> ClientFileInfo getFileStatus(Map<K, ClientFileInfo> cache, K key,
      int fileId, String path, boolean useCachedMetaData) throws IOException {
    ClientFileInfo info = null;
    if (useCachedMetaData) {
      info = cache.get(key);
      if (info != null) {
        return info;
      }
    }

    info = mMasterClient.getFileStatus(fileId, path);
    fileId = info.getId();
    if (fileId == -1) {
      cache.remove(key);
      return null;
    }
    path = info.getPath();

    // TODO: LRU
    mIdToClientFileInfo.put(fileId, info);
    mPathToClientFileInfo.put(path, info);

    return info;
  }

  /**
   * Advanced API.
   * 
   * Gets the ClientFileInfo object that represents the fileId, or the path if fileId is -1.
   * 
   * @param fileId the file id of the file or folder.
   * @param path the path of the file or folder. valid iff fileId is -1.
   * @param useCachedMetadata if true use the local cached meta data
   * @return the ClientFileInfo of the file. null if the file does not exist.
   * @throws IOException
   */
  public synchronized ClientFileInfo getFileStatus(int fileId, TachyonURI path,
      boolean useCachedMetadata) throws IOException {
    if (fileId != -1) {
      return getFileStatus(mIdToClientFileInfo, Integer.valueOf(fileId), fileId,
          TachyonURI.EMPTY_URI.getPath(), useCachedMetadata);
    } else {
      validateUri(path);
      String p = path.getPath();
      return getFileStatus(mPathToClientFileInfo, p, fileId, p, useCachedMetadata);
    }
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, TachyonURI path) throws IOException {
    return getFileStatus(fileId, path, false);
  }

  /**
   * Get ClientFileInfo object based on fileId.
   * 
   * @param fileId the file id of the file or folder.
   * @param useCachedMetadata if true use the local cached meta data
   * @return the ClientFileInfo of the file. null if the file does not exist.
   * @throws IOException
   */
  public synchronized ClientFileInfo getFileStatus(int fileId, boolean useCachedMetadata)
      throws IOException {
    return getFileStatus(fileId, TachyonURI.EMPTY_URI, useCachedMetadata);
  }

  /**
   * Get block's temporary path from worker with initial space allocated.
   * 
   * @param blockId the id of the block
   * @param initialBytes the initial bytes allocated for the block file
   * @return the temporary path of the block file
   * @throws IOException
   */
  public synchronized String getLocalBlockTemporaryPath(long blockId, long initialBytes)
      throws IOException {
    String blockPath = mWorkerClient.requestBlockLocation(blockId, initialBytes);

    File localTempFolder;
    try {
      localTempFolder = new File(CommonUtils.getParent(blockPath));
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }

    if (!localTempFolder.exists()) {
      if (localTempFolder.mkdirs()) {
        CommonUtils.changeLocalFileToFullPermission(localTempFolder.getAbsolutePath());
        LOG.info("Folder {} was created!", localTempFolder);
      } else {
        throw new IOException("Failed to create folder " + localTempFolder);
      }
    }

    return blockPath;
  }

  /**
   * Get the RawTable by id
   * 
   * @param id the id of the raw table
   * @return the RawTable
   * @throws IOException
   */
  public synchronized RawTable getRawTable(int id) throws IOException {
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfo(id, "");
    return new RawTable(this, clientRawTableInfo);
  }

  /**
   * Get the RawTable by path
   * 
   * @param path the path of the raw table
   * @return the RawTable
   * @throws IOException
   */
  public synchronized RawTable getRawTable(TachyonURI path) throws IOException {
    validateUri(path);
    ClientRawTableInfo clientRawTableInfo =
        mMasterClient.user_getClientRawTableInfo(-1, path.getPath());
    return new RawTable(this, clientRawTableInfo);
  }

  /**
   * @return the address of the UnderFileSystem
   * @throws IOException
   */
  public synchronized String getUfsAddress() throws IOException {
    return mMasterClient.user_getUfsAddress();
  }

  /**
   * @return URI of the root of the filesystem
   */
  @Override
  public synchronized TachyonURI getUri() {
    return mRootUri;
  }

  /**
   * Returns the userId of the master client. This is only used for testing.
   * 
   * @return the userId of the master client
   * @throws IOException
   */
  long getUserId() throws IOException {
    return mMasterClient.getUserId();
  }

  /**
   * @return all the works' info
   * @throws IOException
   */
  public synchronized List<ClientWorkerInfo> getWorkersInfo() throws IOException {
    return mMasterClient.getWorkersInfo();
  }

  /**
   * @return true if there is a local worker, false otherwise
   * @throws IOException
   */
  public synchronized boolean hasLocalWorker() throws IOException {
    return mWorkerClient.isLocal();
  }

  /**
   * @return true if this client is connected to master, false otherwise
   */
  public synchronized boolean isConnected() {
    return mMasterClient.isConnected();
  }

  /**
   * @param fid the file id
   * @return true if the file is a directory, false otherwise
   */
  synchronized boolean isDirectory(int fid) {
    return mIdToClientFileInfo.get(fid).isFolder;
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its ClientFileInfo.
   * 
   * @param path the target directory/file path
   * @return A list of ClientFileInfo, null if the file or folder does not exist.
   * @throws IOException
   */
  @Override
  public synchronized List<ClientFileInfo> listStatus(TachyonURI path) throws IOException {
    validateUri(path);
    return mMasterClient.listStatus(path.getPath());
  }

  /**
   * Lock a block in the current TachyonFS.
   * 
   * @param blockId The id of the block to lock. <code>blockId</code> must be positive.
   * @param blockLockId The block lock id of the block of lock. <code>blockLockId</code> must be
   *        non-negative.
   * @return the path of the block file locked
   * @throws IOException
   */
  synchronized String lockBlock(long blockId, int blockLockId) throws IOException {
    if (blockId <= 0 || blockLockId < 0) {
      return null;
    }

    if (mLockedBlockIds.containsKey(blockId)) {
      mLockedBlockIds.get(blockId).add(blockLockId);
      return mLockedBlockIdToPath.get(blockId);
    }

    if (!mWorkerClient.isLocal()) {
      return null;
    }
    String blockPath = mWorkerClient.lockBlock(blockId);

    if (blockPath != null) {
      Set<Integer> lockIds = new HashSet<Integer>(4);
      lockIds.add(blockLockId);
      mLockedBlockIds.put(blockId, lockIds);
      mLockedBlockIdToPath.put(blockId, blockPath);
      return blockPath;
    }
    return null;
  }

  /**
   * Creates a folder.
   * 
   * @param path the path of the folder to be created
   * @param recursive Creates necessary parent folders if true, not otherwise.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean mkdirs(TachyonURI path, boolean recursive) throws IOException {
    validateUri(path);
    return mMasterClient.user_mkdirs(path.getPath(), recursive);
  }

  /** Alias for setPinned(fid, true). */
  public synchronized void pinFile(int fid) throws IOException {
    setPinned(fid, true);
  }

  /**
   * Frees in memory file or folder
   *
   * @param fileId The id of the file / folder. If it is not -1, path parameter is ignored.
   *        Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty iff id is not -1.
   * @param recursive If fileId or path represents a non-empty folder, free the folder recursively
   *        or not
   * @return true if in-memory free successfully, false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean freepath(int fileId, TachyonURI path, boolean recursive)
      throws IOException {
    validateUri(path);
    return mMasterClient.user_freepath(fileId, path.getPath(), recursive);
  }

  /**
   * Set owner of a path (i.e. a file or a directory). The parameters username and groupname cannot
   * both be null.
   * 
   * @param fileId The id of the file / folder. If it is not -1, path parameter is ignored.
   *        Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty if id is not -1.
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * @param recursive If fileId or path represents a folder, change the folder owner recursively
   * @return true if setOwner successfully, false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean setOwner(int fileId, TachyonURI path, String username,
      String groupname, boolean recursive) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    validateUri(path);
    return mMasterClient.user_setOwner(fileId, path.getPath(), username, groupname, recursive);
  }

  /**
   * Set permission of a path.
   * 
   * @param fileId The id of the file / folder. If it is not -1, path parameter is ignored.
   *        Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty if id is not -1.
   * @param short permission, e.g. 777
   * @param recursive If fileId or path represents a folder, 
   *        change the folder permission recursively
   * @return true if setPermission successfully, false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean setPermission(int fileId, TachyonURI path, short permission,
      boolean recursive) throws IOException {
    validateUri(path);
    return mMasterClient.user_setPermission(fileId, path.getPath(), permission, recursive);
  }

  /**
   * Promote block file back to the top StorageTier, after the block file is accessed.
   * 
   * @param blockId the id of the block
   * @return true if success, false otherwise
   * @throws IOException
   */
  public synchronized boolean promoteBlock(long blockId) throws IOException {
    if (mWorkerClient.isLocal()) {
      return mWorkerClient.promoteBlock(blockId);
    }
    return false;
  }

  /**
   * Renames a file or folder to another path.
   * 
   * @param fileId The id of the source file / folder. If it is not -1, path parameter is ignored.
   *        Otherwise, the method uses the srcPath parameter.
   * @param srcPath The path of the source file / folder. It could be empty iff id is not -1.
   * @param dstPath The path of the destination file / folder. It could be empty iff id is not -1.
   * @return true if renames successfully, false otherwise.
   * @throws IOException
   */
  @Override
  public synchronized boolean rename(int fileId, TachyonURI srcPath, TachyonURI dstPath)
      throws IOException {
    validateUri(srcPath);
    validateUri(dstPath);
    return mMasterClient.user_rename(fileId, srcPath.getPath(), dstPath.getPath());
  }

  /**
   * Report the lost file to master
   * 
   * @param fileId the lost file id
   * @throws IOException
   */
  public synchronized void reportLostFile(int fileId) throws IOException {
    mMasterClient.user_reportLostFile(fileId);
  }

  /**
   * Request the dependency's needed files
   * 
   * @param depId the dependency id
   * @throws IOException
   */
  public synchronized void requestFilesInDependency(int depId) throws IOException {
    mMasterClient.user_requestFilesInDependency(depId);
  }

  /**
   * Try to request space for certain block. Only works when a local worker exists.
   * 
   * @param blockId the id of the block that space will be allocated for
   * @param requestSpaceBytes size to request in bytes
   * @return the size bytes that allocated to the block, -1 if no local worker exists
   * @throws IOException
   */
  public synchronized long requestSpace(long blockId, long requestSpaceBytes)
      throws IOException {

    if (!hasLocalWorker()) {
      return -1;
    }

    long userQuotaUnitBytes = mTachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES,
        8 * Constants.MB);

    long toRequestSpaceBytes = Math.max(requestSpaceBytes, userQuotaUnitBytes);
    for (int attempt = 0; attempt < mUserFailedSpaceRequestLimits; attempt ++) {
      if (mWorkerClient.requestSpace(blockId, toRequestSpaceBytes)) {
        return toRequestSpaceBytes;
      }
    }
    return 0;
  }

  /**
   * Sets the "pinned" flag for the given file. Pinned files are never evicted by Tachyon until they
   * are unpinned.
   * 
   * Calling setPinned() on a folder will recursively set the "pinned" flag on all of that folder's
   * children. This may be an expensive operation for folders with many files/subfolders.
   */
  public synchronized void setPinned(int fid, boolean pinned) throws IOException {
    mMasterClient.user_setPinned(fid, pinned);
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
   * @param blockId The id of the block to unlock. <code>blockId</code> must be positive.
   * @param blockLockId The block lock id of the block of unlock. <code>blockLockId</code> must be
   *        non-negative.
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

    if (!mWorkerClient.isLocal()) {
      return false;
    }

    mLockedBlockIdToPath.remove(blockId);
    mLockedBlockIds.remove(blockId);
    return mWorkerClient.unlockBlock(blockId);
  }

  /** Alias for setPinned(fid, false). */
  public synchronized void unpinFile(int fid) throws IOException {
    setPinned(fid, false);
  }

  /**
   * Update the RawTable's meta data
   * 
   * @param id the raw table's id
   * @param metadata the new meta data
   * @throws IOException
   */
  public synchronized void updateRawTableMetadata(int id, ByteBuffer metadata) throws IOException {
    mMasterClient.user_updateRawTableMetadata(id, metadata);
  }

  /**
   * Validates the given uri, throwing an IOException if the uri is invalid.
   * 
   * @param uri The uri to validate
   */
  private void validateUri(TachyonURI uri) throws IOException {
    if (uri == null || (!uri.isPathAbsolute() && !TachyonURI.EMPTY_URI.equals(uri))
        || (uri.hasScheme() && !mRootUri.getScheme().equals(uri.getScheme()))
        || (uri.hasAuthority() && !mRootUri.getAuthority().equals(uri.getAuthority()))) {
      throw new IOException("Uri " + uri + " is invalid.");
    }
  }
}
