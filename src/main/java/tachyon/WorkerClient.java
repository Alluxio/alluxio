package tachyon;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.log4j.Logger;

import tachyon.conf.UserConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.WorkerService;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 * 
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safe.
 */
public class WorkerClient {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final WorkerService.Client CLIENT;

  private TProtocol mProtocol;
  private InetSocketAddress mWorkerAddress;
  private boolean mIsConnected = false;
  private long mUserId;
  private HeartbeatThread mHeartbeatThread = null;

  private String mRootFolder = null;

  public WorkerClient(InetSocketAddress address, long userId) {
    mWorkerAddress = address;
    mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(
        mWorkerAddress.getHostName(), mWorkerAddress.getPort())));
    CLIENT = new WorkerService.Client(mProtocol);

    mUserId = userId;
    mHeartbeatThread = new HeartbeatThread("WorkerClientToWorkerHeartbeat", 
        new WorkerClientHeartbeatExecutor(this, mUserId), 
        UserConf.get().HEARTBEAT_INTERVAL_MS);
    mHeartbeatThread.setDaemon(true);
  }

  public synchronized void accessFile(int fileId) throws TException {
    CLIENT.accessBlock(fileId);
  }

  public synchronized void addCheckpoint(long userId, int fileId) 
      throws IOException, TException {
    try {
      CLIENT.addCheckpoint(userId, fileId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
      throw new IOException(e);
    } catch (FailedToCheckpointException e) {
      throw new IOException(e);
    } catch (BlockInfoException e) {
      throw new IOException(e);
    }
  }

  public synchronized void cacheBlock(long userId, long blockId)
      throws IOException, TException {
    try {
      CLIENT.cacheBlock(userId, blockId);
    } catch (FileDoesNotExistException e) {
      throw new IOException(e);
    } catch (SuspectedFileSizeException e) {
    } catch (BlockInfoException e) {
      throw new IOException(e);
    }
  }

  public synchronized void close() {
    if (mIsConnected) {
      mProtocol.getTransport().close();
      mHeartbeatThread.shutdown();
      mIsConnected = false;
    }
  }

  public synchronized String getUserTempFolder(long userId) throws TException {
    return CLIENT.getUserTempFolder(userId);
  }

  public synchronized String getUserUnderfsTempFolder(long userId) throws TException {
    return CLIENT.getUserUnderfsTempFolder(userId);
  }

  public synchronized String getDataFolder() throws TException {
    if (mRootFolder == null) {
      mRootFolder = CLIENT.getDataFolder();
    }

    return mRootFolder;
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized void lockFile(int fileId, long userId) throws TException {
    CLIENT.lockBlock(fileId, userId);
  }

  public synchronized boolean open() {
    if (!mIsConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
      mHeartbeatThread.start();
      mIsConnected = true;
    }

    return mIsConnected;
  }

  public synchronized boolean requestSpace(long userId, long requestBytes) throws TException {
    return CLIENT.requestSpace(userId, requestBytes);
  }

  public synchronized void returnSpace(long userId, long returnSpaceBytes) throws TException {
    CLIENT.returnSpace(userId, returnSpaceBytes);
  }

  public synchronized void unlockFile(int fileId, long userId) throws TException {
    CLIENT.unlockBlock(fileId, userId);
  }

  public synchronized void userHeartbeat(long userId) throws TException {
    CLIENT.userHeartbeat(userId);
  }
}