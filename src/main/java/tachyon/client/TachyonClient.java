package tachyon.client;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.HdfsClient;
import tachyon.MasterClient;
import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.ClientRawTableInfo;
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
 * 
 * @author haoyuan
 */
public class TachyonClient {
  private final Logger LOG = LoggerFactory.getLogger(TachyonClient.class);

  // The local root data folder.
  private String sDataFolder = null;
  // The local work ComId
  private InetSocketAddress sLocalWorkerAddress = null;

  // The RPC client talks to the system master.
  private MasterClient mMasterClient = null;
  // The Master address.
  private InetSocketAddress mMasterAddress = null;
  // The RPC client talks to the local worker if there is one.
  private WorkerClient mLocalWorkerClient = null;

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
    connectAndGetLocalWorker();
    if (mLocalWorkerClient != null) {
      try {
        mLocalWorkerClient.accessFile(fileId);
        return;
      } catch (TException e) {
        mLocalWorkerClient = null;
        LOG.error(e.getMessage(), e);
      }
    }

    LOG.error("TachyonClient accessLocalFile(" + fileId + ") failed");
  }

  public synchronized boolean addDoneFile(int fileId, boolean writeThrough)
      throws FileDoesNotExistException, SuspectedFileSizeException, FileAlreadyExistException {
    connectAndGetLocalWorker();
    if (mLocalWorkerClient != null) {
      try {
        mLocalWorkerClient.addDoneFile(mUserId, fileId, writeThrough);
        return true;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mLocalWorkerClient = null;
        return false;
      }
    }
    return false;
  }

  // Lazy connection
  // TODO This should be removed since the Thrift server has been fixed.
  public synchronized void connectAndGetLocalWorker() {
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

    try {
      String localHostName = InetAddress.getLocalHost().getHostName();
      LOG.info("Trying to get local worker host : " + localHostName);
      NetAddress localWorker = mMasterClient.user_getLocalWorker(localHostName); 
      sLocalWorkerAddress = new InetSocketAddress(localWorker.mHost, localWorker.mPort);
    } catch (NoLocalWorkerException e) {
      LOG.info(e.getMessage());
      sLocalWorkerAddress = null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      sLocalWorkerAddress = null;
    } catch (UnknownHostException e) {
      LOG.error(e.getMessage());
      sLocalWorkerAddress = null;
    }

    if (sLocalWorkerAddress == null) {
      return;
    }

    LOG.info("Trying to connect local worker @ " + sLocalWorkerAddress);
    mLocalWorkerClient = new WorkerClient(sLocalWorkerAddress);
    if (!mLocalWorkerClient.open()) {
      LOG.error("Failed to connect local worker @ " + sLocalWorkerAddress);
      mLocalWorkerClient = null;
      return;
    }

    try {
      sDataFolder = mLocalWorkerClient.getDataFolder();
      mUserTempFolder = mLocalWorkerClient.getUserTempFolder(mUserId);
      mUserHdfsTempFolder = mLocalWorkerClient.getUserHdfsTempFolder(mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      sDataFolder = null;
      mUserTempFolder = null;
      mLocalWorkerClient = null;
      return;
    }

    if (!mUserHdfsTempFolder.startsWith("hdfs")) {
      mUserHdfsTempFolder = null;
    }

    mToWorkerHeartbeat = new ClientToWorkerHeartbeat(mLocalWorkerClient, mUserId);
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

  public synchronized void close() throws TException {
    if (mMasterClient != null) {
      mMasterClient.close();
    }

    if (mLocalWorkerClient != null) {
      mLocalWorkerClient.returnSpace(mUserId, mAvailableSpaceBytes);
      mLocalWorkerClient.close();
    }
  }

  public synchronized File createAndGetUserTempFolder() {
    connectAndGetLocalWorker();

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
    connectAndGetLocalWorker();

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
    return createRawTable(path, columns, new ArrayList<Byte>(0));
  }

  public synchronized int createRawTable(String path, int columns, List<Byte> metadata)
      throws InvalidPathException {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);

    if (columns < 1 || columns > Config.MAX_COLUMNS) {
      CommonUtils.runtimeException("Column count " + columns + " is smaller than 1 or bigger than "
          + Config.MAX_COLUMNS);
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
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    path = CommonUtils.cleanPath(path);
    // TODO HDFS_TEMP_FILE should also be in Tachyon, but not flashed to disk if not necessary.
    if (path.contains(Config.HDFS_TEMP_FILE)) {
      return -1;
    }
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
    connectAndGetLocalWorker();
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
    connectAndGetLocalWorker();
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

  // TODO For now, we assume this is for partition read only.
  public synchronized List<NetAddress> getFileLocations(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }

    List<NetAddress> ret = null;
    try {
      ret = mMasterClient.user_getFileLocations(fileId);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage());
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
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

  public synchronized TachyonFile getFile(int fileId) {
    ClientFileInfo clientFileInfo = getClientFileInfo(fileId);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo);
  }

  public synchronized int getFileId(String path) throws InvalidPathException {
    connectAndGetLocalWorker();
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
    connectAndGetLocalWorker();
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
    connectAndGetLocalWorker();
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
    connectAndGetLocalWorker();
    path = CommonUtils.cleanPath(path);
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfoByPath(path);
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized RawTable getRawTable(int id) throws TableDoesNotExistException, TException {
    connectAndGetLocalWorker();
    ClientRawTableInfo clientRawTableInfo = mMasterClient.user_getClientRawTableInfoById(id);
    return new RawTable(this, clientRawTableInfo);
  }

  public synchronized String getRootFolder() {
    connectAndGetLocalWorker();
    return sDataFolder;
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  public synchronized void outOfMemoryForPinFile(int fileId) {
    connectAndGetLocalWorker();
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
    connectAndGetLocalWorker();
    if (mLocalWorkerClient == null) {
      return false;
    }
    int failedTimes = 0;
    while (mAvailableSpaceBytes < requestSpaceBytes) {
      if (mLocalWorkerClient == null) {
        LOG.error("The current host does not have a Tachyon worker.");
        return false;
      }
      try {
        if (mLocalWorkerClient.requestSpace(mUserId, Config.USER_QUOTA_UNIT_BYTES)) {
          mAvailableSpaceBytes += Config.USER_QUOTA_UNIT_BYTES;
        } else {
          LOG.info("Failed to request local space. Time " + (failedTimes ++));
          if (failedTimes == Config.USER_FAILED_SPACE_REQUEST_LIMITS) {
            return false;
          }
        }
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mLocalWorkerClient = null;
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
    connectAndGetLocalWorker();
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

  public synchronized boolean lockFile(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected || mLocalWorkerClient == null) {
      return false;
    }
    try {
      mLocalWorkerClient.lockFile(fileId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public synchronized boolean unlockFile(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected || mLocalWorkerClient == null) {
      return false;
    }
    try {
      mLocalWorkerClient.unlockFile(fileId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public synchronized int getNumberOfFiles(String folderPath) 
      throws FileDoesNotExistException, InvalidPathException, TException {
    connectAndGetLocalWorker();
    return mMasterClient.getNumberOfFiles(folderPath);
  }
}