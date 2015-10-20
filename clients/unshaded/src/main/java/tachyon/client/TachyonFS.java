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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystemContext;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.table.RawColumn;
import tachyon.client.table.RawTable;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileInfo;
import tachyon.thrift.RawTableInfo;
import tachyon.thrift.WorkerInfo;
import tachyon.util.IdUtils;
import tachyon.util.io.FileUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.ClientMetrics;
import tachyon.worker.WorkerClient;

/**
 * Client API to use Tachyon as a file system. This API is not compatible with HDFS file system API;
 * while tachyon.hadoop.AbstractTFS provides another API that exposes Tachyon as HDFS file system.
 * Under the hood, this class maintains a MasterClientBase to talk to the master server and
 * WorkerClients to interact with different Tachyon workers.
 *
 * As of 0.8, replaced by {@link tachyon.client.file.TachyonFileSystem}
 */
@PublicApi
@Deprecated
public class TachyonFS extends AbstractTachyonFS {

  /**
   * Creates a <code>TachyonFS</code> handler for the given path.
   *
   * @param tachyonPath a Tachyon path contains master address. e.g., tachyon://localhost:19998,
   *        tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @see #get(tachyon.TachyonURI, tachyon.conf.TachyonConf)
   */
  @Deprecated
  public static synchronized TachyonFS get(String tachyonPath) {
    return get(new TachyonURI(tachyonPath), ClientContext.getConf());
  }

  /**
   * Creates a <code>TachyonFS</code> handler for the given Tachyon URI.
   *
   * @param tachyonURI a Tachyon URI to indicate master address. e.g., tachyon://localhost:19998,
   *        tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @see #get(tachyon.TachyonURI, tachyon.conf.TachyonConf)
   */
  @Deprecated
  public static synchronized TachyonFS get(final TachyonURI tachyonURI) {
    return get(tachyonURI, ClientContext.getConf());
  }

  /**
   * Creates a <code>TachyonFS</code> handler for the given Tachyon URI and configuration.
   *
   * @param tachyonURI a Tachyon URI to indicate master address. e.g., tachyon://localhost:19998,
   *        tachyon://localhost:19998/ab/c.txt
   * @param tachyonConf The TachyonConf instance
   * @return the corresponding TachyonFS handler
   */
  public static synchronized TachyonFS get(final TachyonURI tachyonURI, TachyonConf tachyonConf) {
    Preconditions.checkArgument(tachyonConf != null, "TachyonConf cannot be null.");
    Preconditions.checkArgument(tachyonURI != null, "Tachyon URI cannot be null. Use "
        + Constants.HEADER + "host:port/ ," + Constants.HEADER_FT + "host:port/.");
    String scheme = tachyonURI.getScheme();
    Preconditions.checkNotNull(scheme, "Tachyon scheme cannot be null. Use " + Constants.SCHEME
        + " or " + Constants.SCHEME_FT + ".");
    Preconditions.checkNotNull(tachyonURI.getHost(), "Tachyon hostname cannot be null.");
    Preconditions.checkState(tachyonURI.getPort() != -1, "Tachyon URI must have a port number.");
    Preconditions.checkState(
        (Constants.SCHEME.equals(scheme) || Constants.SCHEME_FT.equals(scheme)),
        "Tachyon scheme must be either " + Constants.SCHEME + " or " + Constants.SCHEME_FT + ".");

    boolean useZookeeper = scheme.equals(Constants.SCHEME_FT);
    tachyonConf.set(Constants.ZOOKEEPER_ENABLED, Boolean.toString(useZookeeper));
    tachyonConf.set(Constants.MASTER_HOSTNAME, tachyonURI.getHost());
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(tachyonURI.getPort()));

    return get(tachyonConf);
  }

  /**
   * Creates a <code>TachyonFS</code> handler for the given hostname, port, and Zookeeper mode.
   *
   * @param masterHost master host details
   * @param masterPort port master listens on
   * @param zkMode use zookeeper
   * @return the corresponding TachyonFS handler
   */
  public static synchronized TachyonFS get(String masterHost, int masterPort, boolean zkMode) {
    TachyonConf tachyonConf = ClientContext.getConf();
    tachyonConf.set(Constants.MASTER_HOSTNAME, masterHost);
    tachyonConf.set(Constants.MASTER_PORT, Integer.toString(masterPort));
    tachyonConf.set(Constants.ZOOKEEPER_ENABLED, Boolean.toString(zkMode));
    return get(tachyonConf);
  }

  /**
   * Creates a <code>TachyonFS</code> handler for the given Tachyon configuration.
   *
   * @param tachyonConf The TachyonConf instance
   * @return the corresponding TachyonFS handler
   */
  public static synchronized TachyonFS get(TachyonConf tachyonConf) {
    Preconditions.checkArgument(tachyonConf != null, "TachyonConf cannot be null.");
    return new TachyonFS(tachyonConf);
  }

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final int mUserFailedSpaceRequestLimits;

  /** The RPC client talks to the file system master. */
  private final FileSystemMasterClient mFSMasterClient;
  /** The RPC client talks to the block store master. */
  private final BlockMasterClient mBlockMasterClient;
  /** The RPC client talks to the raw table master. */
  private final RawTableMasterClient mRawTableMasterClient;
  /** The Master address. */
  private final InetSocketAddress mMasterAddress;
  /** The RPC client talks to the local worker if there is one. */
  private final WorkerClient mWorkerClient;
  private final Closer mCloser = Closer.create();
  /** Whether to use ZooKeeper or not */
  private final boolean mZookeeperMode;
  // Cached FileInfo
  private final Map<String, FileInfo> mPathToClientFileInfo = new HashMap<String, FileInfo>();
  private final Map<Long, FileInfo> mIdToClientFileInfo = new HashMap<Long, FileInfo>();

  /** All Blocks that have been locked. */
  private final Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  /** Mapping from block id to path of the block locked */
  private final Map<Long, String> mLockedBlockIdToPath = new HashMap<Long, String>();
  /** Each user facing block has a unique block lock id. */
  private final AtomicInteger mBlockLockId = new AtomicInteger(0);

  private TachyonURI mRootUri = null;
  private ClientMetrics mClientMetrics = new ClientMetrics();

  private TachyonFS(TachyonConf tachyonConf) {
    super(tachyonConf);

    mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, tachyonConf);
    mZookeeperMode = mTachyonConf.getBoolean(Constants.ZOOKEEPER_ENABLED);
    mFSMasterClient = mCloser.register(FileSystemContext.INSTANCE.acquireMasterClient());
    mBlockMasterClient = mCloser.register(BlockStoreContext.INSTANCE.acquireMasterClient());
    mRawTableMasterClient =
        mCloser.register(new RawTableMasterClient(mMasterAddress, mTachyonConf));
    mWorkerClient = mCloser.register(BlockStoreContext.INSTANCE.acquireWorkerClient());
    mUserFailedSpaceRequestLimits = mTachyonConf.getInt(Constants.USER_FAILED_SPACE_REQUEST_LIMITS);
    String scheme = mZookeeperMode ? Constants.SCHEME_FT : Constants.SCHEME;
    String authority = mMasterAddress.getHostName() + ":" + mMasterAddress.getPort();
    mRootUri = new TachyonURI(scheme, authority, TachyonURI.SEPARATOR);
  }

  /**
   * Updates the latest block access time on the worker.
   *
   * @param blockId the local block's id
   * @throws IOException if the block cannot be accessed
   */
  synchronized void accessLocalBlock(long blockId) throws IOException {
    if (mWorkerClient.isLocal()) {
      mWorkerClient.accessBlock(blockId);
    }
  }

  /**
   * Notifies the worker that the checkpoint file of the indicated file id has been added.
   *
   * @param fid the file id
   * @throws IOException if the underlying worker RPC fails
   */
  synchronized void addCheckpoint(long fid) throws IOException {
    throw new UnsupportedOperationException("AddCheckpoint is not unsupported");
  }

  /**
   * Notifies the worker to checkpoint the file of the indicated file id asynchronously
   *
   * @param fid the file id
   * @return true if succeed, false otherwise
   * @throws IOException if the underlying worker RPC fails
   */
  synchronized boolean asyncCheckpoint(long fid) throws IOException {
    return mWorkerClient.asyncCheckpoint(fid);
  }

  /**
   * Notifies the worker that the block is cached.
   *
   * @param blockId the block id
   * @throws IOException if the underlying worker RPC fails
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mWorkerClient.cacheBlock(blockId);
  }

  /**
   * Notifies the worker the block is canceled.
   *
   * @param blockId the block id
   * @throws IOException if the underlying worker RPC fails
   */
  public synchronized void cancelBlock(long blockId) throws IOException {
    mWorkerClient.cancelBlock(blockId);
  }

  /**
   * Closes the client connections to both the master and worker.
   *
   * @throws IOException if the connections could not be closed
   */
  @Override
  public synchronized void close() throws IOException {
    mCloser.close();
  }

  /**
   * Marks the file as complete.
   *
   * @param fid the file id
   * @throws IOException if the underlying master RPC fails
   */
  synchronized void completeFile(long fid) throws IOException {
    try {
      mFSMasterClient.completeFile(fid);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a dependency.
   *
   * @param parents the dependency's input files
   * @param children the dependency's output files
   * @param commandPrefix identifies a command prefix
   * @param data stores dependency data
   * @param comment records a dependency comment
   * @param framework identifies the framework
   * @param frameworkVersion identifies the framework version
   * @param dependencyType the dependency's type, Wide or Narrow
   * @param childrenBlockSizeByte the block size of the dependency's output files
   * @return the dependency's id
   * @throws IOException if the underlying master RPC fails
   */
  @Deprecated
  public synchronized int createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    throw new UnsupportedOperationException("operation not supported");
  }

  /**
   * Creates a new file in the file system.
   *
   * @param path The path of the file
   * @param ufsPath The path of the file in the under file system. If this is empty, the file does
   *        not exist in the under file system yet.
   * @param blockSizeBytes The size of the block in bytes. It is -1 iff ufsPath is non-empty
   * @param recursive Creates necessary parent folders if true, not otherwise
   * @return The file id, which is globally unique
   * @throws IOException if the underlying master RPC fails
   */
  @Override
  public synchronized long createFile(TachyonURI path, TachyonURI ufsPath, long blockSizeBytes,
      boolean recursive) throws IOException {
    validateUri(path);
    try {
      if (blockSizeBytes > 0) {
        UnderStorageType underStorageType =
            mTachyonConf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class)
                .isThrough() ? UnderStorageType.SYNC_PERSIST : UnderStorageType.NO_PERSIST;
        CreateOptions options =
            new CreateOptions.Builder(ClientContext.getConf()).setBlockSizeBytes(blockSizeBytes)
                .setRecursive(recursive).setUnderStorageType(underStorageType).build();
        return mFSMasterClient.create(path.getPath(), options);
      } else {
        return mFSMasterClient.loadMetadata(path.getPath(), recursive);
      }
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a <code>RawTable</code> and returns its id.
   *
   * @param path the RawTable's path
   * @param columns number of columns it has
   * @return the id if succeed, -1 otherwise
   * @throws IOException if the number of columns is invalid or the underlying master RPC fails
   */
  public synchronized long createRawTable(TachyonURI path, int columns) throws IOException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  /**
   * Creates a <code>RawTable</code> and returns its id.
   *
   * Currently unsupported.
   *
   * @param path the RawTable's path
   * @param columns number of columns it has
   * @param metadata the meta data of the RawTable
   * @return the id if succeed, {@link tachyon.util.IdUtils#INVALID_FILE_ID} otherwise
   * @throws IOException if the number of columns is invalid or the underlying master RPC fails
   */
  public synchronized long createRawTable(TachyonURI path, int columns, ByteBuffer metadata)
      throws IOException {
    validateUri(path);
    int maxColumns = mTachyonConf.getInt(Constants.MAX_COLUMNS);
    if (columns < 1 || columns > maxColumns) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " + "bigger than "
          + maxColumns);
    }
    long tableId = mRawTableMasterClient.createRawTable(path, columns, metadata);
    if (tableId != IdUtils.INVALID_FILE_ID) {
      mkdirs(path, true);
      for (int i = 0; i < columns; i ++) {
        mkdirs(new TachyonURI(RawColumn.getColumnPath(path.toString(), i)), true);
      }
    }
    return tableId;
  }

  /**
   * Deletes a file or folder.
   *
   * @param fileId The id of the file / folder. If it is not INVALID_FILE_ID, path parameter is
   *        ignored. Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty iff id is not INVALID_FILE_ID
   * @param recursive If fileId or path represents a non-empty folder, delete the folder recursively
   *        or not.
   * @return true if deletes successfully, false otherwise
   * @throws IOException if the underlying master RPC fails
   */
  @Override
  public synchronized boolean delete(long fileId, TachyonURI path, boolean recursive)
      throws IOException {
    validateUri(path);
    fileId = getValidFileId(fileId, path.getPath());
    try {
      return mFSMasterClient.deleteFile(fileId, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Returns whether the file exists or not.
   *
   * @param path the file's path in Tachyon file system
   * @return true if it exists, false otherwise
   * @throws IOException if the underlying master RPC fails
   */
  // TODO(calvin): Consider making an exists function.
  public synchronized boolean exist(TachyonURI path) throws IOException {
    try {
      FileInfo info = getFileStatus(IdUtils.INVALID_FILE_ID, path, false);
      return info != null;
    } catch (IOException ioe) {
      return false;
    }
  }

  /**
   * Get the block id by the file id and block index. it will check whether the file and the block
   * exist.
   *
   * @param fileId the file id
   * @param blockIndex The index of the block in the file
   * @return the block id if exists
   * @throws IOException if the file does not exist, or connection issue was encountered
   */
  public synchronized long getBlockId(long fileId, int blockIndex) throws IOException {
    FileInfo info = getFileStatus(fileId, true);

    if (info == null) {
      throw new IOException("File " + fileId + " does not exist.");
    }

    if (info.blockIds.size() > blockIndex) {
      return info.blockIds.get(blockIndex);
    }

    try {
      return mFSMasterClient.getFileBlockInfo(fileId, blockIndex).blockInfo.getBlockId();
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return a new block lock id
   */
  synchronized int getBlockLockId() {
    return mBlockLockId.getAndIncrement();
  }

  /**
   * Gets a ClientBlockInfo by the file and index.
   *
   * @param fileId the id of the file
   * @param blockIndex the index of the block in the file, starting from 0
   * @return the ClientBlockInfo of the specified block
   * @throws IOException if the underlying master RPC fails
   */
  synchronized FileBlockInfo getClientBlockInfo(long fileId, int blockIndex) throws IOException {
    try {
      return mFSMasterClient.getFileBlockInfo(fileId, blockIndex);
    } catch (TachyonException e) {
      // TODO: Should not cast this to Tachyon Exception
      throw new IOException(e);
    }
  }

  /**
   *
   * @param depId the dependency id
   * @return the DependencyInfo of the specified dependency
   * @throws IOException if the underlying master RPC fails
   */
  @Deprecated
  public synchronized DependencyInfo getClientDependencyInfo(int depId) throws IOException {
    throw new UnsupportedOperationException("operation not supported");
  }

  /**
   * Gets the user's <code>ClientMetrics</code>.
   *
   * @return the <code>ClientMetrics</code> object
   */
  ClientMetrics getClientMetrics() {
    return mClientMetrics;
  }

  /**
   * Gets <code>TachyonFile</code> based on the file id.
   *
   * NOTE: This *will* use cached file metadata, and so will not see changes to dynamic properties,
   * such as the pinned flag. This is also different from the behavior of getFile(path), which by
   * default will not use cached metadata.
   *
   * @param fid file id
   * @return <code>TachyonFile</code> of the file id, or null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized TachyonFile getFile(long fid) throws IOException {
    return getFile(fid, true);
  }

  /**
   * Gets <code>TachyonFile</code> based on the file id. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   *
   * @param fid the file id
   * @param useCachedMetadata whether to use cached metadata
   * @return TachyonFile of the file id, or null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized TachyonFile getFile(long fid, boolean useCachedMetadata) throws IOException {
    FileInfo fileInfo = getFileStatus(fid, TachyonURI.EMPTY_URI, useCachedMetadata);
    if (fileInfo == null) {
      return null;
    }
    return new TachyonFile(this, fid, mTachyonConf);
  }

  /**
   * Gets <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   *
   * @param path file path
   * @return TachyonFile of the path, or null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized TachyonFile getFile(TachyonURI path) throws IOException {
    validateUri(path);
    return getFile(path, false);
  }

  /**
   * Gets <code>TachyonFile</code> based on the path. If useCachedMetadata is true, this will not
   * see changes to the file's pin setting, or other dynamic properties.
   *
   * @param path file path
   * @param useCachedMetadata whether to use the file metadata cache
   * @return TachyonFile of the path, or null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized TachyonFile getFile(TachyonURI path, boolean useCachedMetadata)
      throws IOException {
    validateUri(path);
    FileInfo fileInfo = getFileStatus(IdUtils.INVALID_FILE_ID, path, useCachedMetadata);
    if (fileInfo == null) {
      return null;
    }
    return new TachyonFile(this, fileInfo.getFileId(), mTachyonConf);
  }

  /**
   * If the given file id is INVALID_FILE_ID, tries to fetch the file id for the given path
   *
   * @param fileId
   * @param path the path to search for if fileId is INVALID_FILE_ID
   * @return the original fileId if it wasn't INVALID_FILE_ID, or a new fileId corresponding to the
   *         given path
   * @throws IOException if the given fileId is INVALID_FILE_ID and no fileId was found for the
   *         given path
   */
  private long getValidFileId(long fileId, String path) throws IOException {
    if (fileId == IdUtils.INVALID_FILE_ID) {
      fileId = mFSMasterClient.getFileId(path);
      if (fileId == IdUtils.INVALID_FILE_ID) {
        throw new IOException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }
    }
    return fileId;
  }

  /**
   * Gets all the blocks' info of the file.
   *
   * @param fid the file id
   * @return the list of the blocks' info
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized List<FileBlockInfo> getFileBlocks(long fid) throws IOException {
    // TODO(hy) Should read from mClientFileInfos if possible.
    // TODO(hy) Should add timeout to improve this.
    try {
      return mFSMasterClient.getFileBlockInfoList(fid);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Get file id by the path. It will check if the path exists.
   *
   * @param path the path in Tachyon file system
   * @return the file id if exists, INVALID_FILE_ID otherwise
   */
  public synchronized long getFileId(TachyonURI path) {
    try {
      FileInfo fileInfo = getFileStatus(IdUtils.INVALID_FILE_ID, path, false);
      return fileInfo == null ? -IdUtils.INVALID_FILE_ID : fileInfo.getFileId();
    } catch (IOException e) {
      return IdUtils.INVALID_FILE_ID;
    }
  }

  /**
   * Gets a file status.
   *
   * @param cache FileInfo cache
   * @param key the key in the cache
   * @param fileId the id of the queried file. If it is INVALID_FILE_ID, uses path
   * @param path the path of the queried file. If fielId is not INVALID_FILE_ID, this parameter is
   *        ignored.
   * @param useCachedMetaData whether to use the cached data or not
   * @return the clientFileInfo
   * @throws IOException if the underlying master RPC fails
   */
  private synchronized <K> FileInfo getFileStatus(Map<K, FileInfo> cache, K key, long fileId,
      String path, boolean useCachedMetaData) throws IOException {
    FileInfo info = null;
    if (useCachedMetaData) {
      info = cache.get(key);
      if (info != null) {
        return info;
      }
    }

    fileId = getValidFileId(fileId, path);
    try {
      info = mFSMasterClient.getFileInfo(fileId);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
    path = info.getPath();

    // TODO(hy): LRU
    mIdToClientFileInfo.put(fileId, info);
    mPathToClientFileInfo.put(path, info);

    return info;
  }

  /**
   * Advanced API.
   *
   * Gets the FileInfo object that represents the fileId, or the path if fileId is INVALID_FILE_ID.
   *
   * @param fileId the file id of the file or folder
   * @param path the path of the file or folder. valid iff fileId is INVALID_FILE_ID
   * @param useCachedMetadata if true use the local cached meta data
   * @return the FileInfo of the file. null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized FileInfo getFileStatus(long fileId, TachyonURI path,
      boolean useCachedMetadata) throws IOException {
    fileId = getValidFileId(fileId, path.getPath());
    return getFileStatus(mIdToClientFileInfo, fileId, fileId, TachyonURI.EMPTY_URI.getPath(),
        useCachedMetadata);
  }

  @Override
  public FileInfo getFileStatus(long fileId, TachyonURI path) throws IOException {
    return getFileStatus(fileId, path, false);
  }

  /**
   * Gets <code>ClientFileInfo</code> object based on fileId.
   *
   * @param fileId the file id of the file or folder
   * @param useCachedMetadata if true use the local cached meta data
   * @return the FileInfo of the file. null if the file does not exist
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized FileInfo getFileStatus(long fileId, boolean useCachedMetadata)
      throws IOException {
    return getFileStatus(fileId, TachyonURI.EMPTY_URI, useCachedMetadata);
  }

  /**
   * Gets block's temporary path from worker with initial space allocated.
   *
   * @param blockId the id of the block
   * @param initialBytes the initial bytes allocated for the block file
   * @return the temporary path of the block file
   * @throws IOException if the underlying worker RPC fails
   */
  public synchronized String getLocalBlockTemporaryPath(long blockId, long initialBytes)
      throws IOException {
    String blockPath = mWorkerClient.requestBlockLocation(blockId, initialBytes);
    // TODO(haoyuan): Handle this in the worker?
    FileUtils.createBlockPath(blockPath);
    return blockPath;
  }

  /**
   * Gets <code>RawTable</code> by id.
   *
   * Currently unsupported.
   *
   * @param id the id of the raw table
   * @return the RawTable
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized RawTable getRawTable(long id) throws IOException {
    RawTableInfo rawTableInfo = mRawTableMasterClient.getClientRawTableInfo(id);
    return new RawTable(this, rawTableInfo);
  }

  /**
   * Get the <code>RawTable</code> by path.
   *
   * Currently unsupported.
   *
   * @param path the path of the raw table
   * @return the RawTable
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized RawTable getRawTable(TachyonURI path) throws IOException {
    validateUri(path);
    RawTableInfo rawTableInfo = mRawTableMasterClient.getClientRawTableInfo(path);
    return new RawTable(this, rawTableInfo);
  }

  /**
   * @return the address of the UnderFileSystem
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized String getUfsAddress() throws IOException {
    return mFSMasterClient.getUfsAddress();
  }

  /**
   * @return URI of the root of the filesystem
   */
  @Override
  public synchronized TachyonURI getUri() {
    return mRootUri;
  }

  /**
   * @return the sessionId of the worker client. This is only used for testing
   * @throws IOException when the underlying master RPC fails
   */
  long getSessionId() throws IOException {
    return mWorkerClient.getSessionId();
  }

  /**
   * Currently unsupported.
   *
   * @return get the total number of bytes used in Tachyon cluster
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized long getUsedBytes() throws IOException {
    throw new UnsupportedOperationException("Currently unsupported.");
  }

  /**
   * Currently unsupported.
   *
   * @return get the capacity of Tachyon cluster
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized long getCapacityBytes() throws IOException {
    throw new UnsupportedOperationException("Currently unsupported.");
  }

  /**
   * @return The address of the data server on the worker
   */
  public synchronized InetSocketAddress getWorkerDataServerAddress() {
    return mWorkerClient.getDataServerAddress();
  }

  /**
   * @return all the works' info
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized List<WorkerInfo> getWorkersInfo() throws IOException {
    return mBlockMasterClient.getWorkerInfoList();
  }

  /**
   * @return true if there is a local worker, false otherwise
   */
  public synchronized boolean hasLocalWorker() {
    return mWorkerClient.isLocal();
  }

  /**
   * Checks if this client is connected to master.
   *
   * @return true if this client is connected to master, false otherwise
   */
  public synchronized boolean isConnected() {
    return mFSMasterClient.isConnected();
  }

  /**
   * Checks if the indicated file id is a directory.
   *
   * @param fid the file id
   * @return true if the file is a directory, false otherwise
   */
  synchronized boolean isDirectory(int fid) {
    return mIdToClientFileInfo.get(fid).isFolder;
  }

  /**
   * If the <code>path</code> is a directory, returns all the direct entries in it. If the
   * <code>path</code> is a file, returns its ClientFileInfo.
   *
   * @param path the target directory/file path
   * @return A list of FileInfo, null if the file or folder does not exist
   * @throws IOException when the underlying master RPC fails
   */
  @Override
  public synchronized List<FileInfo> listStatus(TachyonURI path) throws IOException {
    validateUri(path);
    try {
      return mFSMasterClient.getFileInfoList(
          getFileStatus(IdUtils.INVALID_FILE_ID, path).getFileId());
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Locks a block in the current TachyonFS.
   *
   * @param blockId The id of the block to lock. <code>blockId</code> must be positive
   * @param blockLockId The block lock id of the block of lock. <code>blockLockId</code> must be
   *        non-negative.
   * @return the path of the block file locked
   * @throws IOException if the underlying worker RPC fails
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
   * @param recursive Creates necessary parent folders if true, not otherwise
   * @return true if the folder is created successfully or already existing. false otherwise
   * @throws IOException if the underlying master RPC fails
   */
  @Override
  public synchronized boolean mkdirs(TachyonURI path, boolean recursive) throws IOException {
    validateUri(path);
    try {
      UnderStorageType underStorageType =
          mTachyonConf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).isThrough()
              ? UnderStorageType.SYNC_PERSIST : UnderStorageType.NO_PERSIST;
      MkdirOptions options =
          new MkdirOptions.Builder(ClientContext.getConf()).setRecursive(recursive)
              .setUnderStorageType(underStorageType).build();
      return mFSMasterClient.mkdir(path.getPath(), options);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * An alias for setPinned(fid, true).
   *
   * @throws IOException when the underlying worker RPC fails
   * @see #setPinned(long, boolean)
   */
  public synchronized void pinFile(long fid) throws IOException {
    setPinned(fid, true);
  }

  /**
   * Frees an in-memory file or folder.
   *
   * @param fileId The id of the file / folder. If it is not INVALID_FILE_ID, path parameter is
   *        ignored. Otherwise, the method uses the path parameter.
   * @param path The path of the file / folder. It could be empty iff id is not INVALID_FILE_ID
   * @param recursive If fileId or path represents a non-empty folder, free the folder recursively
   *        or not
   * @return true if in-memory free successfully, false otherwise
   * @throws IOException if the underlying master RPC fails
   */
  @Override
  public synchronized boolean freepath(long fileId, TachyonURI path, boolean recursive)
      throws IOException {
    validateUri(path);
    fileId = getValidFileId(fileId, path.getPath());
    try {
      return mFSMasterClient.free(fileId, recursive);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Promotes a block to the top StorageTier, after the block file is accessed.
   *
   * @param blockId the id of the block
   * @return true if success, false otherwise
   * @throws IOException if the underlying worker RPC fails
   */
  public synchronized boolean promoteBlock(long blockId) throws IOException {
    if (mWorkerClient.isLocal()) {
      return mWorkerClient.promoteBlock(blockId);
    }
    return false;
  }

  /**
   * Renames a file or folder to the indicated new path.
   *
   * @param fileId The id of the source file / folder. If it is not INVALID_FILE_ID, path parameter
   *        is ignored. Otherwise, the method uses the srcPath parameter.
   * @param srcPath The path of the source file / folder. It could be empty iff id is not
   *        INVALID_FILE_ID.
   * @param dstPath The path of the destination file / folder. It could be empty iff id is not
   *        INVALID_FILE_ID.
   * @return true if renames successfully, false otherwise
   * @throws IOException if the underlying master RPC fails
   */
  @Override
  public synchronized boolean rename(long fileId, TachyonURI srcPath, TachyonURI dstPath)
      throws IOException {
    validateUri(srcPath);
    validateUri(dstPath);
    fileId = getValidFileId(fileId, srcPath.getPath());
    try {
      return mFSMasterClient.renameFile(fileId, dstPath.getPath());
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Reports the lost file to master.
   *
   * @param fileId the lost file id
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized void reportLostFile(long fileId) throws IOException {
    try {
      mFSMasterClient.reportLostFile(fileId);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Requests the dependency's needed files.
   *
   * @param depId the dependency id
   * @throws IOException if the underlying master RPC fails
   */
  @Deprecated
  public synchronized void requestFilesInDependency(int depId) throws IOException {
    throw new UnsupportedOperationException("operation not supported");
  }

  /**
   * Tries to request space for certain block. Only works when a local worker exists.
   *
   * @param blockId the id of the block that space will be allocated for
   * @param requestSpaceBytes size to request in bytes
   * @return the size bytes that allocated to the block, -1 if no local worker exists
   * @throws IOException if the underlying file does not exist
   */
  public synchronized long requestSpace(long blockId, long requestSpaceBytes) throws IOException {
    if (!hasLocalWorker()) {
      return -1;
    }

    long userQuotaUnitBytes = mTachyonConf.getBytes(Constants.USER_QUOTA_UNIT_BYTES);

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
   *
   * @param fid the file id
   * @param pinned the target "pinned" flag value
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized void setPinned(long fid, boolean pinned) throws IOException {
    try {
      mFSMasterClient.setPinned(fid, pinned);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  /**
   * Prints out the string representation of this Tachyon server address.
   *
   * @return the string representation like tachyon://host:port or tachyon-ft://host:port
   */
  @Override
  public String toString() {
    return (mZookeeperMode ? Constants.HEADER_FT : Constants.HEADER) + mMasterAddress.toString();
  }

  /**
   * Unlocks a block in the current TachyonFS.
   *
   * @param blockId The id of the block to unlock. <code>blockId</code> must be positive
   * @param blockLockId The block lock id of the block of unlock. <code>blockLockId</code> must be
   *        non-negative.
   * @throws IOException if the underlying worker RPC fails
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

  /**
   * An alias for setPinned(fid, false).
   *
   * @throws IOException when the underlying worker RPC fails
   * @see #setPinned(long, boolean)
   */
  public synchronized void unpinFile(long fid) throws IOException {
    setPinned(fid, false);
  }

  /**
   * Updates the RawTable's meta data.
   *
   * Currently unsupported.
   *
   * @param id the raw table's id
   * @param metadata the new meta data
   * @throws IOException if the underlying master RPC fails
   */
  public synchronized void updateRawTableMetadata(long id, ByteBuffer metadata) throws IOException {
    mRawTableMasterClient.updateRawTableMetadata(id, metadata);
  }

  /**
   * Validates the given uri, throws an appropriate Exception if the uri is invalid.
   *
   * @param uri the uri to validate
   */
  private void validateUri(TachyonURI uri) {
    Preconditions.checkNotNull(uri, "URI cannot be null.");
    Preconditions.checkArgument(uri.isPathAbsolute() || TachyonURI.EMPTY_URI.equals(uri),
        "URI must be absolute, unless it's empty.");
    Preconditions.checkArgument(
        !uri.hasScheme() || mRootUri.getScheme().equals(uri.getScheme()),
        "URI's scheme: " + uri.getScheme() + " must match the file system's scheme: "
            + mRootUri.getScheme() + ", unless it doesn't have a scheme.");
    Preconditions.checkArgument(
        !uri.hasAuthority() || mRootUri.getAuthority().equals(uri.getAuthority()),
        "URI's authority: " + uri.getAuthority() + " must match the file system's authority: "
            + mRootUri.getAuthority() + ", unless it doesn't have an authority.");
  }
}
