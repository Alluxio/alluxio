package tachyon.client;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.HdfsClient;
import tachyon.MasterClient;
import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TableColumnException;
import tachyon.thrift.TableDoesNotExistException;

/**
 * Major Tachyon system user facing class. It contains a MasterClient and several WorkerClients
 * depending on how many workers the client program interact with.
 */
public class TachyonClient {
  private final Logger LOG = Logger.getLogger(CommonConf.LOGGER_TYPE);
  
  private final long USER_QUOTA_UNIT_BYTES = UserConf.get().QUOTA_UNIT_BYTES;
  private final int USER_FAILED_SPACE_REQUEST_LIMITS = UserConf.get().FAILED_SPACE_REQUEST_LIMITS;

  // The RPC client talks to the system master.
  private MasterClient mMasterClient = null;
  // The Master address.
  private InetSocketAddress mMasterAddress = null;
  // The RPC client talks to the local worker if there is one.
  private WorkerClient mWorkerClient = null;
  // The local root data folder.
  private String mDataFolder = null;
  // Whether the client is local or remote.
  private boolean mIsWorkerLocal = false;
  // The local data folder.
  private String mUserTempFolder = null;
  // The HDFS data folder
  private String mUserHdfsTempFolder = null;
  private HdfsClient mHdfsClient = null;

  private long mUserId = 0;

  // Available memory space for this client.
  private Long mAvailableSpaceBytes;

  private ClientToWorkerHeartbeat mToWorkerHeartbeat = null;

  private boolean mConnected = false;

  public synchronized void accessLocalFile(int fileId) {
    connect();
    if (mWorkerClient != null && mIsWorkerLocal) {
      try {
        mWorkerClient.accessFile(fileId);
        return;
      } catch (TException e) {
        mWorkerClient = null;
        LOG.error(e.getMessage(), e);
      }
    }

    LOG.error("TachyonClient accessLocalFile(" + fileId + ") failed");
  }

  public synchronized void addCheckpoint(int fileId) 
      throws FileDoesNotExistException, SuspectedFileSizeException, FailedToCheckpointException {
    connect();
    if (!mConnected) {
      throw new FailedToCheckpointException("Failed to add checkpoint for file " + fileId);
    }
    if (mWorkerClient != null) {
      try {
        mWorkerClient.addCheckpoint(mUserId, fileId);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerClient = null;
      }
    }
  }

  public synchronized void cacheFile(int fileId) 
      throws FileDoesNotExistException, SuspectedFileSizeException {
    connect();
    if (!mConnected) {
      return;
    }

    if (mWorkerClient != null) {
      try {
        mWorkerClient.cacheFile(mUserId, fileId);
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mWorkerClient = null;
      }
    }
    return;
  }

  // Lazy connection
  // TODO This should be removed since the Thrift server has been fixed.
  public synchronized void connect() {
    if (mMasterClient != null) {
      return;
    }
    LOG.info("Trying to connect master @ " + mMasterAddress);
    mMasterClient = new MasterClient(mMasterAddress);
    mConnected = mMasterClient.open();

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
    mWorkerClient = new WorkerClient(workerAddress);
    if (!mWorkerClient.open()) {
      LOG.error("Failed to connect " + (mIsWorkerLocal ? "local" : "remote") + 
          " worker @ " + workerAddress);
      mWorkerClient = null;
      return;
    }

    try {
      mDataFolder = mWorkerClient.getDataFolder();
      mUserTempFolder = mWorkerClient.getUserTempFolder(mUserId);
      mUserHdfsTempFolder = mWorkerClient.getUserHdfsTempFolder(mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      mDataFolder = null;
      mUserTempFolder = null;
      mWorkerClient = null;
      return;
    }

    if (!mUserHdfsTempFolder.startsWith("hdfs")) {
      mUserHdfsTempFolder = null;
    }

    mToWorkerHeartbeat = new ClientToWorkerHeartbeat(mWorkerClient, mUserId);
    Thread thread = new Thread(mToWorkerHeartbeat);
    thread.setDaemon(true);
    thread.start();
  }

  private TachyonClient(InetSocketAddress masterAddress) {
    mMasterAddress = masterAddress;
    mAvailableSpaceBytes = 0L;
  }

  public static synchronized TachyonClient getClient(InetSocketAddress tachyonAddress) {
    return new TachyonClient(tachyonAddress);
  }

  public static synchronized TachyonClient getClient(String tachyonAddress) {
    String[] address = tachyonAddress.split(":");
    if (address.length != 2) {
      CommonUtils.illegalArgumentException("Illegal Tachyon Master Address: " + tachyonAddress);
    }
    return getClient(new InetSocketAddress(address[0], Integer.parseInt(address[1])));
  }

  /**
   * This API is not recommended to use.
   * @param id file id
   * @param path existing checkpoint path
   * @return true if the checkpoint path is added successfully, false otherwise.
   * @throws TException 
   * @throws SuspectedFileSizeException 
   * @throws FileDoesNotExistException 
   */
  public synchronized boolean addCheckpointPath(int id, String path)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    connect();
    HdfsClient hdfsClient = new HdfsClient(path);
    long fileSizeBytes = hdfsClient.getFileSize(path);
    return mMasterClient.addCheckpoint(-1, id, fileSizeBytes, path);
  }

  public synchronized void close() throws TException {
    if (mMasterClient != null) {
      mMasterClient.close();
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

  public synchronized String createAndGetUserHDFSTempFolder() {
    connect();

    if (mUserHdfsTempFolder == null) {
      return null;
    }

    if (mHdfsClient == null) {
      mHdfsClient = new HdfsClient(mUserHdfsTempFolder);
    }

    mHdfsClient.mkdirs(mUserHdfsTempFolder, null, true);

    return mUserHdfsTempFolder;
  }

  public synchronized int createRawTable(String path, int columns) throws InvalidPathException {
    return createRawTable(path, columns, ByteBuffer.allocate(0));
  }

  public synchronized int createRawTable(String path, int columns, ByteBuffer metadata)
      throws InvalidPathException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);

    if (columns < 1 || columns > Constants.MAX_COLUMNS) {
      CommonUtils.runtimeException("Column count " + columns + " is smaller than 1 or bigger than "
          + Constants.MAX_COLUMNS);
    }

    try {
      return mMasterClient.user_createRawTable(path, columns, metadata);
    } catch (TableColumnException e) {
      LOG.info(e.getMessage());
      return -1;
    } catch (FileAlreadyExistException e) {
      LOG.info(e.getMessage());
      return -1;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
  }

  public synchronized int createFile(String path) throws InvalidPathException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    int fileId = -1;
    try {
      fileId = mMasterClient.user_createFile(path);
    } catch (FileAlreadyExistException e) {
      LOG.info(e.getMessage());
      fileId = -1;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      fileId = -1;
    }
    return fileId;
  }

  public synchronized int mkdir(String path) throws InvalidPathException {
    connect();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    int id = -1;
    try {
      id = mMasterClient.user_mkdir(path);
    } catch (FileAlreadyExistException e) {
      LOG.info(e.getMessage());
      id = -1;
    } catch (TException e) {
      LOG.info(e.getMessage());
      id = -1;
    }
    return id;
  }

  public synchronized boolean deleteFile(int fileId) {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_delete(fileId);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized boolean deleteFile(String path) throws InvalidPathException {
    return deleteFile(getFileId(path));
  }

  public synchronized boolean renameFile(String srcPath, String dstPath) 
      throws InvalidPathException {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_renameFile(srcPath, dstPath);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (FileAlreadyExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized List<NetAddress> getFileNetAddresses(int fileId)
      throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }

    List<NetAddress> ret = null;
    try {
      ret = mMasterClient.user_getFileLocations(fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
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

  public synchronized List<String> getFileHosts(int fileId)
      throws IOException {
    connect();
    if (!mConnected) {
      return null;
    }

    List<NetAddress> adresses = getFileNetAddresses(fileId);
    List<String> ret = new ArrayList<String>(adresses.size());
    for (NetAddress address: adresses) {
      ret.add(address.mHost);
      if (address.mHost.endsWith(".ec2.internal")) {
        ret.add(address.mHost.substring(0, address.mHost.length() - 13));
      }
    }

    return ret;
  }

  public synchronized List<List<String>> getFilesHosts(List<Integer> fileIds) 
      throws IOException {
    List<List<String>> ret = new ArrayList<List<String>>();
    for (int k = 0; k < fileIds.size(); k ++) {
      ret.add(getFileHosts(fileIds.get(k)));
    }

    return ret;
  }

  public synchronized TachyonFile getFile(String path) throws InvalidPathException {
    path = CommonUtils.cleanPath(path);
    ClientFileInfo clientFileInfo = getClientFileInfo(path);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo);
  }

  // TODO fileId should not be exposed to applications, it should be an internal value.
  public synchronized TachyonFile getFile(int fileId) {
    ClientFileInfo clientFileInfo = getClientFileInfo(fileId);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo);
  }

  public synchronized int getFileId(String path) throws InvalidPathException {
    connect();
    if (!mConnected) {
      return -1;
    }
    int fileId = -1;
    path = CommonUtils.cleanPath(path);
    try {
      fileId = mMasterClient.user_getFileId(path);
    } catch (TException e) {
      // TODO Ideally, this exception should be throws to the upper upper layer, and 
      // remove notContainDataset(datasetPath) method. This is for absolutely fall through.
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
    return fileId;
  }

  private synchronized ClientFileInfo getClientFileInfo(String path) throws InvalidPathException { 
    connect();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret;
    path = CommonUtils.cleanPath(path);
    try {
      ret = mMasterClient.user_getClientFileInfoByPath(path);
    } catch (FileDoesNotExistException e) {
      LOG.info("File " + path + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  private synchronized ClientFileInfo getClientFileInfo(int fileId) {
    connect();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret = null;
    try {
      ret = mMasterClient.user_getClientFileInfoById(fileId);
    } catch (FileDoesNotExistException e) {
      LOG.info("File with id " + fileId + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  public synchronized RawTable getRawTable(String path)
      throws TableDoesNotExistException, InvalidPathException, TException {
    connect();
    path = CommonUtils.cleanPath(path);
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfoByPath(path);
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized RawTable getRawTable(int id) throws TableDoesNotExistException, TException {
    connect();
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfoById(id);
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized String getRootFolder() {
    connect();
    return mDataFolder;
  }

  public synchronized boolean hasLocalWorker() {
    connect();
    return (mIsWorkerLocal && mWorkerClient != null);
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  public synchronized List<Integer> listFiles(String path, boolean recursive) throws IOException {
    connect();
    try {
      return mMasterClient.user_listFiles(path, recursive);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized List<ClientFileInfo> listStatus(String path)
      throws FileDoesNotExistException, InvalidPathException, TException {
    connect();
    if (!mConnected) {
      return null;
    }
    return mMasterClient.ls(path);
  }

  public synchronized List<String> ls(String path, boolean recursive) throws IOException {
    connect();
    try {
      return mMasterClient.user_ls(path, recursive);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidPathException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  public synchronized boolean lockFile(int fileId) {
    connect();
    if (!mConnected || mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    try {
      mWorkerClient.lockFile(fileId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public synchronized void outOfMemoryForPinFile(int fileId) {
    connect();
    if (mConnected) {
      try {
        mMasterClient.user_outOfMemoryForPinFile(fileId);
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
        if (mWorkerClient.requestSpace(mUserId, USER_QUOTA_UNIT_BYTES)) {
          mAvailableSpaceBytes += USER_QUOTA_UNIT_BYTES;
        } else {
          LOG.info("Failed to request " + USER_QUOTA_UNIT_BYTES + " bytes local space. " +
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

  public synchronized boolean unpinFile(int fileId) {
    connect();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_unpinFile(fileId);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public synchronized boolean unlockFile(int fileId) {
    connect();
    if (!mConnected || mWorkerClient == null || !mIsWorkerLocal) {
      return false;
    }
    try {
      mWorkerClient.unlockFile(fileId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public synchronized int getNumberOfFiles(String folderPath) 
      throws FileDoesNotExistException, InvalidPathException, TException {
    connect();
    return mMasterClient.user_getNumberOfFiles(folderPath);
  }
}