package tachyon.client;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.SuspectedFileSizeException;

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
        mLocalWorkerClient.addFile(mUserId, fileId, writeThrough);
        return true;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mLocalWorkerClient = null;
        return false;
      }
    }
    return false;
  }

//  public synchronized boolean addDoneRCDPartition(int datasetId, int partitionId, int sizeBytes) {
//    connectAndGetLocalWorker();
//    if (mLocalWorkerClient != null) {
//      try {
//        mLocalWorkerClient.addRCDPartition(datasetId, partitionId, sizeBytes);
//        return true;
//      } catch (PartitionDoesNotExistException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      } catch (SuspectedPartitionSizeException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      } catch (PartitionAlreadyExistException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      } catch (TException e) {
//        LOG.error(e.getMessage(), e);
//        mLocalWorkerClient = null;
//        return false;
//      }
//    }
//    return false;
//  }

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
    mLocalWorkerClient = WorkerClient.createWorkerClient(sLocalWorkerAddress);
    if (!mLocalWorkerClient.open()) {
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
    if (address.length != 1) {
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

//  public synchronized int createRawColumnDataset(String datasetPath, int columns,  int partitions) {
//    connectAndGetLocalWorker();
//    if (!mConnected) {
//      return -1;
//    }
//    datasetPath = CommonUtils.cleanPath(datasetPath);
//    int rawColumnDatasetId = -1;
//    try {
//      rawColumnDatasetId = mMasterClient.user_createRawColumnDataset(datasetPath, columns, partitions);
//    } catch (DatasetAlreadyExistException e) {
//      LOG.info(e.getMessage());
//      rawColumnDatasetId = -1;
//    } catch (InvalidPathException e) {
//      LOG.error(e.getMessage());
//      rawColumnDatasetId = -1;
//    } catch (TException e) {
//      LOG.error(e.getMessage());
//      mConnected = false;
//      rawColumnDatasetId = -1;
//    }
//
//    return rawColumnDatasetId;
//  }

  public synchronized int createFile(String filePath) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    filePath = CommonUtils.cleanPath(filePath);
    // TODO HDFS_TEMP_FILE should also be in Tachyon, but not flashed to disk if not necessary.
    if (filePath.contains(Config.HDFS_TEMP_FILE)) {
      return -1;
    }
    int fileId = -1;
    try {
      fileId = mMasterClient.user_createFile(filePath);
    } catch (FileAlreadyExistException e) {
      LOG.info(e.getMessage());
      fileId = -1;
    } catch (InvalidPathException e) {
      LOG.error(e.getMessage());
      fileId = -1;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      fileId = -1;
    }
    return fileId;
  }

  public synchronized boolean deleteFile(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_deleteFile(fileId);
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

  public synchronized TachyonFile getFile(String filePath) {
    filePath = CommonUtils.cleanPath(filePath);
    ClientFileInfo clientFileInfo = getClientFileInfo(filePath);
    if (clientFileInfo == null) {
      return null;
    }
    return new TachyonFile(this, clientFileInfo, filePath);
  }

  public synchronized Dataset getDataset(int datasetId) {
    DatasetInfo datasetInfo = getDatasetInfo(datasetId);
    if (datasetInfo == null) {
      return null;
    }
    return new Dataset(this, datasetInfo);
  }

  public synchronized int getDatasetId(String datasetPath) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    int datasetId = -1;
    datasetPath = CommonUtils.cleanPath(datasetPath);
    try {
      datasetId = mMasterClient.user_getDatasetId(datasetPath);
    } catch (TException e) {
      // TODO Ideally, this exception should be throws to the upper upper layer, and 
      // remove notContainDataset(datasetPath) method. This is for absolutely fall through.
      LOG.error(e.getMessage());
      mConnected = false;
      return -1;
    }
    return datasetId;
  }

  private synchronized ClientFileInfo getClientFileInfo(String filePath) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }
    ClientFileInfo ret;
    filePath = CommonUtils.cleanPath(filePath);
    try {
      ret = mMasterClient.user_getDataset(filePath);
    } catch (FileDoesNotExistException e) {
      LOG.info("Dataset with path " + filePath + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  private synchronized DatasetInfo getDatasetInfo(int datasetId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }
    DatasetInfo ret = null;
    try {
      ret = mMasterClient.user_getDataset(datasetId);
    } catch (DatasetDoesNotExistException e) {
      LOG.info("Dataset with id " + datasetId + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  public synchronized RawColumnDataset getRawColumnDataset(String datasetPath) {
    datasetPath = CommonUtils.cleanPath(datasetPath);
    RawColumnDatasetInfo rawColumnDatasetInfo = getRawColumnDatasetInfo(datasetPath);
    if (rawColumnDatasetInfo == null) {
      return null;
    }
    return new RawColumnDataset(this, rawColumnDatasetInfo);
  }

  public synchronized RawColumnDataset getRawColumnDataset(int datasetId) {
    RawColumnDatasetInfo rawColumnDatasetInfo = getRawColumnDatasetInfo(datasetId);
    if (rawColumnDatasetInfo == null) {
      return null;
    }
    return new RawColumnDataset(this, rawColumnDatasetInfo);
  }

  private synchronized RawColumnDatasetInfo getRawColumnDatasetInfo(String datasetPath) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }
    RawColumnDatasetInfo ret;
    datasetPath = CommonUtils.cleanPath(datasetPath);
    try {
      ret = mMasterClient.user_getRawColumnDataset(datasetPath);
    } catch (DatasetDoesNotExistException e) {
      LOG.info("RawColumnDataset with path " + datasetPath + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  private synchronized RawColumnDatasetInfo getRawColumnDatasetInfo(int datasetId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }

    RawColumnDatasetInfo ret = null;
    try {
      ret = mMasterClient.user_getRawColumnDatasetInfo(datasetId);
    } catch (DatasetDoesNotExistException e) {
      LOG.info("RawColumnDataset with id " + datasetId + " does not exist.");
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }

    return ret;
  }

  public synchronized String getRootFolder() {
    connectAndGetLocalWorker();
    return sDataFolder;
  }

  public synchronized boolean isConnected() {
    return mConnected;
  }

  public synchronized void outOfMemoryForPinDataset(int datasetId) {
    connectAndGetLocalWorker();
    if (mConnected) {
      try {
        mMasterClient.user_outOfMemoryForPinDataset(datasetId);
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

  public synchronized boolean unpinDataset(int datasetId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_unpinDataset(datasetId);
    } catch (DatasetDoesNotExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  public boolean lockFile(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected || mLocalWorkerClient == null) {
      return false;
    }
    try {
      mLocalWorkerClient.lockPartition(datasetId, partitionId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }

  public boolean unlockFile(int fileId) {
    connectAndGetLocalWorker();
    if (!mConnected || mLocalWorkerClient == null) {
      return false;
    }
    try {
      mLocalWorkerClient.unlockPartition(datasetId, partitionId, mUserId);
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }
    return true;
  }
}