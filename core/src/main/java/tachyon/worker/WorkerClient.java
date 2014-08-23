package tachyon.worker;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import tachyon.Constants;
import tachyon.HeartbeatThread;
import tachyon.conf.UserConf;
import tachyon.master.MasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.NoWorkerException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;
import tachyon.util.NetworkUtils;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 * 
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safe.
 */
public class WorkerClient implements Closeable {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient mMasterClient;
  private static final int CONNECTION_RETRY_TIMES = 5;

  private WorkerService.Client mClient;
  private TProtocol mProtocol;
  private InetSocketAddress mWorkerAddress;
  private boolean mConnected = false;
  private boolean mIsLocal = false;
  private String mDataFolder = null;

  private HeartbeatThread mHeartbeatThread = null;

  /**
   * Create a WorkerClient, with a given MasterClient.
   * 
   * @param masterClient
   * @throws IOException
   */
  public WorkerClient(MasterClient masterClient) throws IOException {
    mMasterClient = masterClient;
  }

  /**
   * Update the latest block access time on the worker.
   * 
   * @param blockId
   *          The id of the block
   * @throws IOException
   */
  public synchronized void accessBlock(long blockId) throws IOException {
    if (connect()) {
      try {
        mClient.accessBlock(blockId);
      } catch (TException e) {
        LOG.error("TachyonClient accessLocalBlock(" + blockId + ") failed");
        mConnected = false;
        throw new IOException(e);
      }
    }
  }

  /**
   * Notify the worker that the checkpoint file of the file has been added.
   * 
   * @param userId
   *          The user id of the client who send the notification
   * @param fileId
   *          The id of the checkpointed file
   * @throws IOException
   */
  public synchronized void addCheckpoint(long userId, int fileId) throws IOException {
    mustConnect();

    try {
      mClient.addCheckpoint(userId, fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (FailedToCheckpointException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker to checkpoint the file asynchronously.
   * 
   * @param fid
   *          The id of the file
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean asyncCheckpoint(int fid) throws IOException {
    mustConnect();

    try {
      return mClient.asyncCheckpoint(fid);
    } catch (TachyonException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Notify the worker the block is cached.
   * 
   * @param blockId
   *          The id of the block
   * @throws IOException
   */
  public synchronized void cacheBlock(long blockId) throws IOException {
    mustConnect();

    try {
      mClient.cacheBlock(MASTER_CLIENT.getUserId(), blockId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Close the connection to worker. Shutdown the heartbeat thread.
   */
  @Override
  public synchronized void close() {
    if (mConnected) {
      mProtocol.getTransport().close();
      mHeartbeatThread.shutdown();
      mConnected = false;
    }
  }

  /**
   * Open the connection to the worker. And start the heartbeat thread.
   * 
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  private synchronized boolean connect() throws IOException {
    if (!mConnected) {
      NetAddress workerNetAddress = null;
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
        mIsLocal = true;
      } catch (NoWorkerException e) {
        LOG.info(e.getMessage());
        workerNetAddress = null;
      } catch (UnknownHostException e) {
        LOG.error(e.getMessage(), e);
        workerNetAddress = null;
      }

      if (workerNetAddress == null) {
        try {
          workerNetAddress = mMasterClient.user_getWorker(true, "");
        } catch (NoWorkerException e) {
          LOG.info(e.getMessage());
          workerNetAddress = null;
        }
      }

      if (workerNetAddress == null) {
        LOG.info("No worker running in the system");
        mClient = null;
        return false;
      }

      mWorkerAddress = new InetSocketAddress(workerNetAddress.mHost, workerNetAddress.mPort);
      LOG.info("Connecting " + (mIsLocal ? "local" : "remote") + " worker @ " + mWorkerAddress);

      mProtocol =
          new TBinaryProtocol(new TFramedTransport(new TSocket(mWorkerAddress.getHostName(),
              mWorkerAddress.getPort())));
      mClient = new WorkerService.Client(mProtocol);

      mHeartbeatThread =
          new HeartbeatThread("WorkerClientToWorkerHeartbeat", new WorkerClientHeartbeatExecutor(
              this, mMasterClient.getUserId()), UserConf.get().HEARTBEAT_INTERVAL_MS);

      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mHeartbeatThread.start();
      mConnected = true;
    }

    return mConnected;
  }

  /**
   * @return the address of the worker.
   */
  public synchronized InetSocketAddress getAddress() {
    return mWorkerAddress;
  }

  /**
   * @return The root local data folder of the worker
   * @throws IOException
   */
  public synchronized String getDataFolder() throws IOException {
    if (mDataFolder == null) {
      try {
        mustConnect();
        mDataFolder = mClient.getDataFolder();
      } catch (TException e) {
        mDataFolder = null;
        mConnected = false;
        throw new IOException(e);
      }
    }

    return mDataFolder;
  }

  /**
   * Get the local user temporary folder of the specified user.
   * 
   * @return The local user temporary folder of the specified user
   * @throws IOException
   */
  public synchronized String getUserTempFolder() throws IOException {
    mustConnect();

    try {
      return mClient.getUserTempFolder(mMasterClient.getUserId());
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Get the user temporary folder in the under file system of the specified user.
   * 
   * @return The user temporary folder in the under file system
   * @throws IOException
   */
  public synchronized String getUserUfsTempFolder() throws IOException {
    mustConnect();

    try {
      return mClient.getUserUfsTempFolder(mMasterClient.getUserId());
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * @return true if it's connected to the worker, false otherwise.
   */
  public synchronized boolean isConnected() {
    return mConnected;
  }

  /**
   * @return true if the worker is local, false otherwise.
   */
  public synchronized boolean isLocal() {
    if (!isConnected()) {
      try {
        connect();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return mIsLocal;
  }

  /**
   * Lock the block, therefore, the worker will lock evict the block from the memory untill it is
   * unlocked.
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who wants to lock the block
   * @throws IOException
   */
  public synchronized void lockBlock(long blockId, long userId) throws IOException {
    mustConnect();

    try {
      mClient.lockBlock(blockId, userId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Connect to the worker.
   * 
   * @throws IOException
   */
  public synchronized void mustConnect() throws IOException {
    int tries = 0;
    while (tries ++ <= CONNECTION_RETRY_TIMES) {
      if (connect()) {
        return;
      }
    }
    throw new IOException("Failed to connect to the worker");
  }

  /**
   * Request space from the worker's memory
   * 
   * @param userId
   *          The id of the user who send the request
   * @param requestBytes
   *          The requested space size, in bytes
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public synchronized boolean requestSpace(long userId, long requestBytes) throws IOException {
    mustConnect();

    try {
      return mClient.requestSpace(userId, requestBytes);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Return the space which has been requested
   * 
   * @param userId
   *          The id of the user who wants to return the space
   * @param returnSpaceBytes
   *          The returned space size, in bytes
   * @throws IOException
   */
  public synchronized void returnSpace(long userId, long returnSpaceBytes) throws IOException {
    mustConnect();

    try {
      mClient.returnSpace(userId, returnSpaceBytes);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Unlock the block
   * 
   * @param blockId
   *          The id of the block
   * @param userId
   *          The id of the user who wants to unlock the block
   * @throws IOException
   */
  public synchronized void unlockBlock(long blockId, long userId) throws IOException {
    mustConnect();

    try {
      mClient.unlockBlock(blockId, userId);
    } catch (TException e) {
      mConnected = false;
      throw new IOException(e);
    }
  }

  /**
   * Users' heartbeat to the Worker.
   * 
   * @param userId
   *          The id of the user
   * @throws IOException
   */
  public synchronized void userHeartbeat(long userId) throws IOException {
    mustConnect();

    try {
      mClient.userHeartbeat(userId);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
}
