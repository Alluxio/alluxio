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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.UnderFileSystem;
import tachyon.client.table.RawTable;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;
import tachyon.master.MasterClient;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientDependencyInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.ClientWorkerInfo;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerClient;

/**
 * Tachyon's user client API. It contains a MasterClient and several WorkerClients
 * depending on how many workers the client program is interacting with.
 */
public class TachyonFS extends AbstractTachyonFS {
  /**
   * Create a TachyonFS handler.
   *
   * @param tachyonPath
   *          a Tachyon path contains master address. e.g., tachyon://localhost:19998,
   *          tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @throws IOException
   * @deprecated use {@link #get(TachyonURI)} instead.
   */
  @Deprecated
  public static synchronized TachyonFS get(final String tachyonPath) throws IOException {
    return get(new TachyonURI(tachyonPath));
  }

  /**
   * Create a TachyonFS handler.
   *
   * @param tachyonURI
   *          a Tachyon URI contains master address. e.g., tachyon://localhost:19998,
   *          tachyon://localhost:19998/ab/c.txt
   * @return the corresponding TachyonFS handler
   * @throws IOException
   */
   public static synchronized TachyonFS get(final TachyonURI tachyonURI) throws IOException {
     if (tachyonURI == null) {
       throw new IOException("Tachyon Uri cannot be null. Use " + Constants.HEADER + "host:port/ ,"
           + Constants.HEADER_FT + "host:port/");
     } else {
       String scheme = tachyonURI.getScheme();
       if (scheme == null || tachyonURI.getHost() == null || tachyonURI.getPort() == -1 ||
           (!Constants.SCHEME.equals(scheme) && !Constants.SCHEME_FT.equals(scheme))) {
         throw new IOException("Invalid Tachyon URI: " + tachyonURI + ". Use " + Constants.HEADER
             + "host:port/ ," + Constants.HEADER_FT + "host:port/");
       }
       return new TachyonFS(tachyonURI);
     }
   }

  /**
   * Create a TachyonFS handler.
   * 
   * @param masterHost
   *          master host details
   * @param masterPort
   *          port master listens on
   * @param zookeeperMode
   *          use zookeeper
   * 
   * @return the corresponding TachyonFS hanlder
   * @throws IOException
   */
  public static synchronized TachyonFS
      get(String masterHost, int masterPort, boolean zookeeperMode) throws IOException {
    return new TachyonFS(new InetSocketAddress(masterHost, masterPort), zookeeperMode);
  }

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final long mUserQuotaUnitBytes = UserConf.get().QUOTA_UNIT_BYTES;
  private final int mUserFailedSpaceRequestLimits = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  // The RPC client talks to the system master.
  private MasterClient mMasterClient = null;
  // The Master address.
  private InetSocketAddress mMasterAddress = null;
  // The RPC client talks to the local worker if there is one.
  private WorkerClient mWorkerClient = null;
  // Whether use ZooKeeper or not
  private boolean mZookeeperMode;
  // Cached ClientFileInfo
  private Map<String, ClientFileInfo> mPathToClientFileInfo =
      new HashMap<String, ClientFileInfo>();
  private Map<Integer, ClientFileInfo> mIdToClientFileInfo =
      new HashMap<Integer, ClientFileInfo>();

  private UnderFileSystem mUnderFileSystem = null;

  // All Blocks has been locked.
  private Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();

  // Each user facing block has a unique block lock id.
  private AtomicInteger mBlockLockId = new AtomicInteger(0);

  // Available memory space for this client.
  private Long mAvailableSpaceBytes;

  private TachyonFS(TachyonURI tachyonURI) throws IOException {
    this(new InetSocketAddress(tachyonURI.getHost(), tachyonURI.getPort()),
        tachyonURI.getScheme().equals(Constants.SCHEME_FT));
  }

  private TachyonFS(InetSocketAddress masterAddress, boolean zookeeperMode) throws IOException {
    mMasterAddress = masterAddress;
    mZookeeperMode = zookeeperMode;
    mAvailableSpaceBytes = 0L;

    mMasterClient = new MasterClient(mMasterAddress, mZookeeperMode);
    mWorkerClient = new WorkerClient(mMasterClient);
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId
   *          the local block's id
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
   * @param fid
   *          the file id
   * @throws IOException
   */
  synchronized void addCheckpoint(int fid) throws IOException {
    mWorkerClient.addCheckpoint(mMasterClient.getUserId(), fid);
  }

  /**
   * Notify the worker to checkpoint the file asynchronously
   * 
   * @param fid
   *          the file id
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  synchronized boolean asyncCheckpoint(int fid) throws IOException {
    return mWorkerClient.asyncCheckpoint(fid);
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param blockId
   *          the block id
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mWorkerClient.cacheBlock(blockId);
  }

  /**
   * Close the client. Close the connections to both to master and worker
   * 
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    if (mWorkerClient.isConnected()) {
      mWorkerClient.returnSpace(mMasterClient.getUserId(), mAvailableSpaceBytes);
      mWorkerClient.close();
    }

    if (mMasterClient != null) {
      mMasterClient.close();
    }
  }

  /**
   * The file is complete.
   * 
   * @param fid
   *          the file id
   * @throws IOException
   */
  synchronized void completeFile(int fid) throws IOException {
    mMasterClient.user_completeFile(fid);
  }

  /**
   * Create a user local temporary folder and return it
   * 
   * @return the local temporary folder for the user
   * @throws IOException
   */
  synchronized File createAndGetUserLocalTempFolder() throws IOException {
    String userTempFolder = mWorkerClient.getUserTempFolder();

    if (userTempFolder == null) {
      return null;
    }

    File ret = new File(userTempFolder);
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

  /**
   * Create a user UnderFileSystem temporary folder and return it
   * 
   * @return the UnderFileSystem temporary folder
   * @throws IOException
   */
  synchronized String createAndGetUserUfsTempFolder() throws IOException {
    String tmpFolder = mWorkerClient.getUserUfsTempFolder();
    if (tmpFolder == null) {
      return null;
    }

    if (mUnderFileSystem == null) {
      mUnderFileSystem = UnderFileSystem.get(tmpFolder);
    }

    mUnderFileSystem.mkdirs(tmpFolder, true);

    return tmpFolder;
  }

  /**
   * Create a Dependency
   * 
   * @param parents
   *          the dependency's input files
   * @param children
   *          the dependency's output files
   * @param commandPrefix
   * @param data
   * @param comment
   * @param framework
   * @param frameworkVersion
   * @param dependencyType
   *          the dependency's type, Wide or Narrow
   * @param childrenBlockSizeByte
   *          the block size of the dependency's output files
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
   * Creates a file with the default block size (1GB) in the system. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI)} instead
   */
  @Deprecated
  public synchronized int createFile(String path) throws IOException {
    return createFile(new TachyonURI(path));
  }

  /**
   * Creates a file in the system. It also creates necessary folders along the path.
   * // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file
   * @param blockSizeByte
   *          the block size of the file
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI, long)} instead
   */
  @Deprecated
  public synchronized int createFile(String path, long blockSizeByte) throws IOException {
    if (blockSizeByte > (long) Constants.GB * 2) {
      throw new IOException("Block size must be less than 2GB: " + blockSizeByte);
    }

    return createFile(new TachyonURI(path), blockSizeByte);
  }

  /**
   * Creates a file in the system with a pre-defined underfsPath. It also creates necessary
   * folders along the path. // TODO It should not create necessary path.
   *
   * @param path
   *          the path of the file in Tachyon
   * @param ufsPath
   *          the path of the file in the underfs
   * @return The unique file id. It returns -1 if the creation failed.
   * @throws IOException
   *           If file already exists, or path is invalid.
   * @deprecated use {@link #createFile(TachyonURI, TachyonURI)} instead
   */
  @Deprecated
  public synchronized int createFile(String path, String ufsPath) throws IOException {
    return createFile(new TachyonURI(path), new TachyonURI(ufsPath));
  }

  /**
   * Creates a new file in the file system.
   *
   * @param path
   *          The path of the file
   * @param ufsPath
   *          The path of the file in the under file system. If this is empty, the file does
   *          not exist in the under file system yet.
   * @param blockSizeByte
   *          The size of the block in bytes. It is -1 iff ufsPath is non-empty.
   * @param recursive
   *          Creates necessary parent folders if true, not otherwise.
   * @return The file id, which is globally unique.
   * @deprecated use {@link #createFile(TachyonURI, TachyonURI, long, boolean)} instead.
   */
  @Deprecated
  public synchronized int createFile(String path, String ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    return createFile(new TachyonURI(path), new TachyonURI(ufsPath), blockSizeByte, recursive);
  }

  @Override
  public synchronized int createFile(TachyonURI path, TachyonURI ufsPath, long blockSizeByte,
      boolean recursive) throws IOException {
    validateUri(path);
    return mMasterClient.user_createFile(path.getPath(), ufsPath.toString(), blockSizeByte, recursive);
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   * @deprecated use {@link #createRawTable(TachyonURI, int)} instead
   */
  @Deprecated
  public synchronized int createRawTable(String path, int columns) throws IOException {
    return createRawTable(new TachyonURI(path), columns);
  }

  /**
   * Create a RawTable and return its id
   *
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(TachyonURI path, int columns) throws IOException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  /**
   * Create a RawTable and return its id
   * 
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @param metadata
   *          the meta data of the RawTable
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   * @deprecated use {@link #createRawTable(TachyonURI, int, java.nio.ByteBuffer)} instead
   */
  @Deprecated
  public synchronized int createRawTable(String path, int columns, ByteBuffer metadata)
      throws IOException {
    return createRawTable(new TachyonURI(path), columns, metadata);
  }

  /**
   * Create a RawTable and return its id
   *
   * @param path
   *          the RawTable's path
   * @param columns
   *          number of columns it has
   * @param metadata
   *          the meta data of the RawTable
   * @return the id if succeed, -1 otherwise
   * @throws IOException
   */
  public synchronized int createRawTable(TachyonURI path, int columns, ByteBuffer metadata)
      throws IOException {
    validateUri(path);
    if (columns < 1 || columns > CommonConf.get().MAX_COLUMNS) {
      throw new IOException("Column count " + columns + " is smaller than 1 or " + "bigger than "
          + CommonConf.get().MAX_COLUMNS);
    }

    return mMasterClient.user_createRawTable(path.getPath(), columns, metadata);
  }

  /**
   * Deletes the file denoted by the path.
   *
   * @param path
   *          the file path
   * @param recursive
   *          if delete the path recursively.
   * @return true if the deletion succeed (including the case that the path does not exist in the
   *         first place), false otherwise.
   * @throws IOException
   * @deprecated use {@link #delete(TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized boolean delete(String path, boolean recursive) throws IOException {
    return delete(new TachyonURI(path), recursive);
  }

  /**
   * Deletes a file or folder
   *
   * @param fileId
   *          The id of the file / folder. If it is not -1, path parameter is ignored.
   *          Otherwise, the method uses the path parameter.
   * @param path
   *          The path of the file / folder. It could be empty iff id is not -1.
   * @param recursive
   *          If fileId or path represents a non-empty folder, delete the folder recursively
   *          or not
   * @return true if deletes successfully, false otherwise.
   * @throws IOException
   * @deprecated use {@link #delete(int, TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized boolean delete(int fileId, String path, boolean recursive)
      throws IOException {
    return delete(fileId, new TachyonURI(path), recursive);
  }

  @Override
  public synchronized boolean delete(int fileId, TachyonURI path, boolean recursive)
      throws IOException {
    validateUri(path);
    return mMasterClient.user_delete(fileId, path.getPath(), recursive);
  }

  /**
   * Return whether the file exists or not
   * 
   * @param path
   *          the file's path in Tachyon file system
   * @return true if it exists, false otherwise
   * @throws IOException
   * @deprecated use {@link #exist(TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean exist(String path) throws IOException {
    return exist(new TachyonURI(path));
  }

  /**
   * Return whether the file exists or not
   *
   * @param path
   *          the file's path in Tachyon file system
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
   * @param fileId
   *          the file id
   * @param blockIndex
   *          The index of the block in the file.
   * @return the block id if exists
   * @throws IOException
   *           if the file does not exist, or connection issue.
   */
  public synchronized long getBlockId(int fileId, int blockIndex) throws IOException {
    ClientFileInfo info = getFileStatus(fileId, "", true);

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
   * @param blockId
   *          the id of the block
   * @return the ClientBlockInfo of the specified block
   * @throws IOException
   */
  synchronized ClientBlockInfo getClientBlockInfo(long blockId) throws IOException {
    return mMasterClient.user_getClientBlockInfo(blockId);
  }

  /**
   * Get a ClientDependencyInfo by the dependency id
   * 
   * @param depId
   *          the dependency id
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
    if (!useCachedMetadata || !mIdToClientFileInfo.containsKey(fid)) {
      ClientFileInfo clientFileInfo = getFileStatus(fid, "");
      if (clientFileInfo == null) {
        return null;
      }
      mIdToClientFileInfo.put(fid, clientFileInfo);
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
   * @deprecated use {@link #getFile(TachyonURI)} instead
   */
  @Deprecated
  public synchronized TachyonFile getFile(String path) throws IOException {
    return getFile(new TachyonURI(path));
  }

  /**
   * Get <code>TachyonFile</code> based on the path. Does not utilize the file metadata cache.
   *
   * @param path
   *          file path.
   * @return TachyonFile of the path, or null if the file does not exist.
   * @throws IOException
   */
  public synchronized TachyonFile getFile(TachyonURI path) throws IOException {
    validateUri(path);
    return getFile(path, false);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   *
   * @deprecated use {@link #getFile(TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized TachyonFile getFile(String path, boolean useCachedMetadata)
      throws IOException {
    return getFile(new TachyonURI(path), useCachedMetadata);
  }

  /**
   * Get <code>TachyonFile</code> based on the path. If useCachedMetadata, this will not see
   * changes to the file's pin setting, or other dynamic properties.
   */
  public synchronized TachyonFile getFile(TachyonURI path, boolean useCachedMetadata)
      throws IOException {
    validateUri(path);
    ClientFileInfo clientFileInfo = getFileStatus(-1, path.getPath(), useCachedMetadata);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo.getId());
  }

  /**
   * Get all the blocks' info of the file
   * 
   * @param fid
   *          the file id
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
   * @param path
   *          the path in Tachyon file system
   * @return the file id if exists, -1 otherwise
   * @throws IOException
   * @deprecated use {@link #getFileId(TachyonURI)} instead
   */
  @Deprecated
  public synchronized int getFileId(String path) throws IOException {
    try {
      return getFileStatus(-1, new TachyonURI(path), false).getId();
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Get file id by the path. It will check if the path exists.
   *
   * @param path
   *          the path in Tachyon file system
   * @return the file id if exists, -1 otherwise
   * @throws IOException
   */
  public synchronized int getFileId(TachyonURI path) throws IOException {
    try {
      return getFileStatus(-1, path, false).getId();
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Gets the ClientFileInfo object that represents the fileId, or the path if fileId is -1.
   *
   * @param fileId
   *          the file id of the file or folder.
   * @param path
   *          the path of the file or folder. valid iff fileId is -1.
   * @return the ClientFileInfo of the file or folder, null if the file or folder does not exist.
   * @throws IOException
   * @deprecated use {@link #getFileStatus(int, TachyonURI)} instead
   */
  @Deprecated
  public ClientFileInfo getFileStatus(int fileId, String path) throws IOException {
    return getFileStatus(fileId, new TachyonURI(path));
  }

  @Override
  public ClientFileInfo getFileStatus(int fileId, TachyonURI path) throws IOException {
    validateUri(path);
    ClientFileInfo info = mMasterClient.getFileStatus(fileId, path.getPath());
    return info.getId() == -1 ? null : info;
  }

  /**
   * Advanced API.
   *
   * Gets the ClientFileInfo object that represents the fileId, or the path if fileId is -1.
   *
   * @param fileId
   *          the file id of the file or folder.
   * @param path
   *          the path of the file or folder. valid iff fileId is -1.
   * @param useCachedMetadata
   *          if true use the local cached meta data
   * @return the ClientFileInfo of the file. null if the file does not exist.
   * @throws IOException
   * @deprecated use {@link #getFileStatus(int, TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized ClientFileInfo getFileStatus(int fileId, String path,
      boolean useCachedMetadata) throws IOException {
    return getFileStatus(fileId, new TachyonURI(path), useCachedMetadata);
  }

  /**
   * Advanced API.
   * 
   * Gets the ClientFileInfo object that represents the fileId, or the path if fileId is -1.
   * 
   * @param fileId
   *          the file id of the file or folder.
   * @param path
   *          the path of the file or folder. valid iff fileId is -1.
   * @param useCachedMetadata
   *          if true use the local cached meta data
   * @return the ClientFileInfo of the file. null if the file does not exist.
   * @throws IOException
   */
  public synchronized ClientFileInfo getFileStatus(int fileId, TachyonURI path,
      boolean useCachedMetadata) throws IOException {
    ClientFileInfo info = null;
    boolean updated = false;


    if (fileId != -1) {
      info = mIdToClientFileInfo.get(fileId);
      if (!useCachedMetadata || info == null) {
        info = getFileStatus(fileId, TachyonURI.EMPTY_URI);
        updated = true;
      }

      if (info == null) {
        mIdToClientFileInfo.remove(fileId);
        return null;
      }

      path = new TachyonURI(info.getPath());
    } else {
      info = mPathToClientFileInfo.get(path);
      if (!useCachedMetadata || info == null) {
        info = getFileStatus(-1, path);
        updated = true;
      }

      if (info == null) {
        mPathToClientFileInfo.remove(path);
        return null;
      }

      fileId = info.getId();
    }

    if (updated) {
      // TODO LRU on this Map.
      mIdToClientFileInfo.put(fileId, info);
      mPathToClientFileInfo.put(path.getPath(), info);
    }

    return info;
  }

  /**
   * Get the RawTable by id
   * 
   * @param id
   *          the id of the raw table
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
   * @param path
   *          the path of the raw table
   * @return the RawTable
   * @throws IOException
   * @deprecated use {@link #getRawTable(TachyonURI)} instead
   */
  @Deprecated
  public synchronized RawTable getRawTable(String path) throws IOException {
    return getRawTable(new TachyonURI(path));
  }

  /**
   * Get the RawTable by path
   *
   * @param path
   *          the path of the raw table
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
   * @return the local root data folder
   * @throws IOException
   */
  synchronized String getLocalDataFolder() throws IOException {
    return mWorkerClient.getDataFolder();
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
    String scheme = CommonConf.get().USE_ZOOKEEPER ? Constants.SCHEME_FT : Constants.SCHEME;
    String authority = mMasterAddress.getHostName() + ":" + mMasterAddress.getPort();
    return new TachyonURI(scheme, authority, TachyonURI.SEPARATOR);
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
   * @param fid
   *          the file id
   * @return true if the file is a directory, false otherwise
   */
  synchronized boolean isDirectory(int fid) {
    return mIdToClientFileInfo.get(fid).isFolder;
  }

  /**
   * If the <code>path</code> is a directory, return all the direct entries in it. If the
   * <code>path</code> is a file, return its ClientFileInfo.
   *
   * @param path
   *          the target directory/file path
   * @return A list of ClientFileInfo, null if the file or folder does not exist.
   * @throws IOException
   * @deprecated use {@link #listStatus(TachyonURI)} instead
   */
  @Deprecated
  public synchronized List<ClientFileInfo> listStatus(String path) throws IOException {
    return listStatus(new TachyonURI(path));
  }

  @Override
  public synchronized List<ClientFileInfo> listStatus(TachyonURI path) throws IOException {
    validateUri(path);
    return mMasterClient.listStatus(path.getPath());
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

    if (!mWorkerClient.isLocal()) {
      return false;
    }
    mWorkerClient.lockBlock(blockId, mMasterClient.getUserId());

    Set<Integer> lockIds = new HashSet<Integer>(4);
    lockIds.add(blockLockId);
    mLockedBlockIds.put(blockId, lockIds);
    return true;
  }

  /**
   * Create a directory if it does not exist. The method also creates necessary non-existing
   * parent folders.
   *
   * @param path
   *          Directory path.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   * @deprecated use {@link #mkdir(TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean mkdir(String path) throws IOException {
    return mkdir(new TachyonURI(path));
  }

  /**
   * Creates a folder.
   *
   * @param path
   *          the path of the folder to be created
   * @param recursive
   *          Creates necessary parent folders if true, not otherwise.
   * @return true if the folder is created successfully or already existing. false otherwise.
   * @throws IOException
   * @deprecated use {@link #mkdirs(TachyonURI, boolean)} instead
   */
  @Deprecated
  public synchronized boolean mkdirs(String path, boolean recursive) throws IOException {
    return mkdirs(new TachyonURI(path), recursive);
  }

  @Override
  public synchronized boolean mkdirs(TachyonURI path, boolean recursive) throws IOException {
    validateUri(path);
    return mMasterClient.user_mkdirs(path.getPath(), recursive);
  }

  /** Alias for setPinned(fid, true). */
  public synchronized void pinFile(int fid) throws IOException {
    setPinned(fid, true);
  }

  public synchronized void releaseSpace(long releaseSpaceBytes) {
    mAvailableSpaceBytes += releaseSpaceBytes;
  }

  /**
   * Renames the file
   *
   * @param fileId
   *          the file id
   * @param dstPath
   *          the new path of the file in the file system.
   * @return true if succeed, false otherwise
   * @throws IOException
   * @deprecated use {@link #rename(int, TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean rename(int fileId, String dstPath) throws IOException {
    return rename(fileId, new TachyonURI(dstPath));
  }

  /**
   * Rename the srcPath to the dstPath
   *
   * @param srcPath
   *          The path of the source file / folder.
   * @param dstPath
   *          The path of the destination file / folder.
   * @return true if succeed, false otherwise.
   * @throws IOException
   * @deprecated use {@link #rename(TachyonURI, TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean rename(String srcPath, String dstPath) throws IOException {
    return rename(new TachyonURI(srcPath), new TachyonURI(dstPath));
  }

  /**
   * Renames a file or folder to another path.
   *
   * @param fileId
   *          The id of the source file / folder. If it is not -1, path parameter is ignored.
   *          Otherwise, the method uses the srcPath parameter.
   * @param srcPath
   *          The path of the source file / folder. It could be empty iff id is not -1.
   * @param dstPath
   *          The path of the destination file / folder. It could be empty iff id is not -1.
   * @return true if renames successfully, false otherwise.
   * @throws IOException
   * @deprecated use {@link #rename(int, TachyonURI, TachyonURI)} instead
   */
  @Deprecated
  public synchronized boolean rename(int fileId, String srcPath, String dstPath)
      throws IOException {
    return rename(fileId, new TachyonURI(srcPath), new TachyonURI(dstPath));
  }

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
   * @param fileId
   *          the lost file id
   * @throws IOException
   */
  public synchronized void reportLostFile(int fileId) throws IOException {
    mMasterClient.user_reportLostFile(fileId);
  }

  /**
   * Request the dependency's needed files
   * 
   * @param depId
   *          the dependency id
   * @throws IOException
   */
  public synchronized void requestFilesInDependency(int depId) throws IOException {
    mMasterClient.user_requestFilesInDependency(depId);
  }

  /**
   * Try to request space from worker. Only works when a local worker exists.
   * 
   * @param requestSpaceBytes
   *          the space size in bytes
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long requestSpaceBytes) throws IOException {
    if (!mWorkerClient.isLocal()) {
      return false;
    }
    int failedTimes = 0;
    while (mAvailableSpaceBytes < requestSpaceBytes) {
      long toRequestSpaceBytes =
          Math.max(requestSpaceBytes - mAvailableSpaceBytes, mUserQuotaUnitBytes);
      if (mWorkerClient.requestSpace(mMasterClient.getUserId(), toRequestSpaceBytes)) {
        mAvailableSpaceBytes += toRequestSpaceBytes;
      } else {
        LOG.info("Failed to request " + toRequestSpaceBytes + " bytes local space. " + "Time "
            + (failedTimes ++));
        if (failedTimes == mUserFailedSpaceRequestLimits) {
          return false;
        }
      }
    }

    if (mAvailableSpaceBytes < requestSpaceBytes) {
      return false;
    }

    mAvailableSpaceBytes -= requestSpaceBytes;

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

    if (!mWorkerClient.isLocal()) {
      return false;
    }

    mWorkerClient.unlockBlock(blockId, mMasterClient.getUserId());
    mLockedBlockIds.remove(blockId);

    return true;
  }

  /** Alias for setPinned(fid, false). */
  public synchronized void unpinFile(int fid) throws IOException {
    setPinned(fid, false);
  }

  /**
   * Update the RawTable's meta data
   * 
   * @param id
   *          the raw table's id
   * @param metadata
   *          the new meta data
   * @throws IOException
   */
  public synchronized void updateRawTableMetadata(int id, ByteBuffer metadata) throws IOException {
    mMasterClient.user_updateRawTableMetadata(id, metadata);
  }

  /**
   * Validates the given uri, throwing an IOException if the uri is invalid.
   *
   * @param uri
   *          The uri to validate
   */
  private void validateUri(TachyonURI uri) throws IOException {
    TachyonURI thisFs = getUri();
    if (uri == null || (!uri.isPathAbsolute() && !TachyonURI.EMPTY_URI.equals(uri)) ||
        (uri.hasScheme() && !thisFs.getScheme().equals(uri.getScheme())) ||
        (uri.hasAuthority() && !thisFs.getAuthority().equals(uri.getAuthority()))) {
      throw new IOException("Uri " + uri + " is invalid.");
    }
  }
}
