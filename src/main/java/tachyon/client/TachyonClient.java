package tachyon.client;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.HdfsClient;
import tachyon.MasterClient;
import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoLocalWorkerException;
import tachyon.thrift.PartitionAlreadyExistException;
import tachyon.thrift.PartitionDoesNotExistException;
import tachyon.thrift.PartitionInfo;
import tachyon.thrift.DatasetAlreadyExistException;
import tachyon.thrift.DatasetDoesNotExistException;
import tachyon.thrift.DatasetInfo;
import tachyon.thrift.RawColumnDatasetInfo;
import tachyon.thrift.SuspectedPartitionSizeException;

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

  private ToWorkerHeartbeat mToWorkerHeartbeat = null;

  private boolean mConnected = false;

  public static class ToWorkerHeartbeat implements Runnable {
    final private Logger LOG = LoggerFactory.getLogger(ToWorkerHeartbeat.class);
    final private WorkerClient WORKER_CLIENT;
    final private long USER_ID;

    public ToWorkerHeartbeat(WorkerClient workerClient, long userId) {
      WORKER_CLIENT = workerClient;
      USER_ID = userId;
    }

    @Override
    public void run() {
      while (true) {
        try {
          WORKER_CLIENT.userHeartbeat(USER_ID);
        } catch (TException e) {
          LOG.error(e.getMessage());
          break;
        }

        CommonUtils.sleep(Config.USER_HEARTBEAT_INTERVAL_MS);
      }
    }
  }

  public synchronized void accessLocalPartition(int datasetId, int partitionId) {
    connectAndGetLocalWorker();
    if (mLocalWorkerClient != null) {
      try {
        mLocalWorkerClient.accessPartition(datasetId, partitionId);
        return;
      } catch (TException e) {
        mLocalWorkerClient = null;
        LOG.error(e.getMessage(), e);
      }
    }

    LOG.error("TachyonClient accessLocalPartition(" + datasetId + ", " + partitionId + ") failed");
  }

  public synchronized boolean addDonePartition(int datasetId, int partitionId, boolean writeThrough)
      throws PartitionDoesNotExistException, SuspectedPartitionSizeException,
      PartitionAlreadyExistException {
    connectAndGetLocalWorker();
    if (mLocalWorkerClient != null) {
      try {
        mLocalWorkerClient.addPartition(mUserId, datasetId, partitionId, writeThrough);
        return true;
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mLocalWorkerClient = null;
        return false;
      }
    }
    return false;
  }

  public synchronized boolean addDoneRCDPartition(int datasetId, int partitionId, int sizeBytes) {
    connectAndGetLocalWorker();
    if (mLocalWorkerClient != null) {
      try {
        mLocalWorkerClient.addDoneRCDPartition(datasetId, partitionId, sizeBytes);
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

    mToWorkerHeartbeat = new ToWorkerHeartbeat(mLocalWorkerClient, mUserId);
    Thread thread = new Thread(mToWorkerHeartbeat);
    thread.setDaemon(true);
    thread.start();
  }

  private TachyonClient(InetSocketAddress masterAddress) {
    mMasterAddress = masterAddress;
    mAvailableSpaceBytes = 0L;
  }

  public static synchronized TachyonClient createTachyonClient(InetSocketAddress tachyonAddress) {
    return new TachyonClient(tachyonAddress);
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

  public synchronized String createAndGetUserHDFSTempFolder(HdfsClient hdfsClient) {
    connectAndGetLocalWorker();

    if (mUserHdfsTempFolder == null) {
      return null;
    }

    if (mHdfsClient == null) {
      mHdfsClient = new HdfsClient();
    }

    mHdfsClient.mkdirs(mUserHdfsTempFolder, null, true);

    return mUserHdfsTempFolder;
  }

  public synchronized int createRawColumnDataset(String datasetPath, int columns,  int partitions) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    datasetPath = CommonUtils.cleanPath(datasetPath);
    int rawColumnDatasetId = -1;
    try {
      rawColumnDatasetId = mMasterClient.user_createRawColumnDataset(datasetPath, columns, partitions);
    } catch (DatasetAlreadyExistException e) {
      LOG.info(e.getMessage());
      rawColumnDatasetId = -1;
    } catch (InvalidPathException e) {
      LOG.error(e.getMessage());
      rawColumnDatasetId = -1;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      rawColumnDatasetId = -1;
    }

    return rawColumnDatasetId;
  }

  public synchronized int createDataset(String datasetPath, int partitions) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return -1;
    }
    datasetPath = CommonUtils.cleanPath(datasetPath);
    if (datasetPath.contains(Config.HDFS_TEMP_FILE)) {
      return -1;
    }
    int datasetId = -1;
    try {
      datasetId = mMasterClient.user_createDataset(datasetPath, partitions);
    } catch (DatasetAlreadyExistException e) {
      LOG.info(e.getMessage());
      datasetId = -1;
    } catch (InvalidPathException e) {
      LOG.error(e.getMessage());
      datasetId = -1;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      datasetId = -1;
    }
    return datasetId;
  }

  public synchronized boolean deleteDataset(int datasetId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return false;
    }

    try {
      mMasterClient.user_deleteDataset(datasetId);
    } catch (DatasetDoesNotExistException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (TException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  // TODO For now, we assume this is for partition read only.
  public synchronized PartitionInfo getPartitionInfo(int datasetId, int partitionId) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }

    PartitionInfo ret = null;
    try {
      ret = mMasterClient.user_getPartitionInfo(datasetId, partitionId);
    } catch (PartitionDoesNotExistException e) {
      LOG.error(e.getMessage());
      return null;
    } catch (TException e) {
      LOG.error(e.getMessage());
      mConnected = false;
      return null;
    }
    return ret;
  }

  public synchronized Dataset getDataset(String datasetPath) {
    datasetPath = CommonUtils.cleanPath(datasetPath);
    DatasetInfo datasetInfo = getDatasetInfo(datasetPath);
    if (datasetInfo == null) {
      return null;
    }
    return new Dataset(this, datasetInfo);
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

  private synchronized DatasetInfo getDatasetInfo(String datasetPath) {
    connectAndGetLocalWorker();
    if (!mConnected) {
      return null;
    }
    DatasetInfo ret;
    datasetPath = CommonUtils.cleanPath(datasetPath);
    try {
      ret = mMasterClient.user_getDataset(datasetPath);
    } catch (DatasetDoesNotExistException e) {
      LOG.info("Dataset with path " + datasetPath + " does not exist.");
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
}