package tachyon;

import java.net.InetSocketAddress;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.WorkerService;

/**
 * The client talks to a worker server. It keeps sending keep alive message to the worker server.
 * 
 * Since WorkerService.Client is not thread safe, this class has to guarantee thread safe.
 * 
 * @author Haoyuan
 */
public class WorkerClient {
  private final Logger LOG = LoggerFactory.getLogger(WorkerClient.class);
  private final WorkerService.Client CLIENT;

  private TProtocol mProtocol;
  private InetSocketAddress mWorkerAddress;
  private boolean mIsConnected = false;

  private String mRootFolder = null;

  public WorkerClient(InetSocketAddress address) {
    mWorkerAddress = address;
    mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(
        mWorkerAddress.getHostName(), mWorkerAddress.getPort())));
    CLIENT = new WorkerService.Client(mProtocol);
  }

  public synchronized void accessFile(int fileId) throws TException {
    CLIENT.accessFile(fileId);
  }

  public synchronized void addCheckpoint(long userId, int fileId) 
      throws FileDoesNotExistException, SuspectedFileSizeException,
      FailedToCheckpointException, TException {
    CLIENT.addCheckpoint(userId, fileId);
  }

  public synchronized void cacheFile(long userId, int fileId)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    CLIENT.cacheFile(userId, fileId);
  }

  public synchronized void close() {
    mProtocol.getTransport().close();
    mIsConnected = false;
  }

  public synchronized String getUserTempFolder(long userId) throws TException {
    return CLIENT.getUserTempFolder(userId);
  }

  public synchronized String getUserHdfsTempFolder(long userId) throws TException {
    return CLIENT.getUserHdfsTempFolder(userId);
  }

  public synchronized String getDataFolder() throws TException {
    if (mRootFolder == null) {
      mRootFolder = CLIENT.getDataFolder();
    }

    return mRootFolder;
  }

  public synchronized void lockFile(int fileId, long userId) throws TException {
    CLIENT.lockFile(fileId, userId);
  }

  public synchronized boolean isConnected() {
    return mIsConnected;
  }

  public synchronized boolean open() {
    if (!mIsConnected) {
      try {
        mProtocol.getTransport().open();
      } catch (TTransportException e) {
        LOG.error(e.getMessage(), e);
        return false;
      }
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
    CLIENT.unlockFile(fileId, userId);
  }

  public synchronized void userHeartbeat(long userId) throws TException {
    CLIENT.userHeartbeat(userId);
  }
}