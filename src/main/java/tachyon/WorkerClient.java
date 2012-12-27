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

import tachyon.thrift.PartitionAlreadyExistException;
import tachyon.thrift.PartitionDoesNotExistException;
import tachyon.thrift.SuspectedPartitionSizeException;
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

  private WorkerClient(InetSocketAddress address) {
    mWorkerAddress = address;
    mProtocol = new TBinaryProtocol(new TFramedTransport(new TSocket(
        mWorkerAddress.getHostName(), mWorkerAddress.getPort())));
    CLIENT = new WorkerService.Client(mProtocol);
  }

  public synchronized void accessPartition(int datasetId, int partitionId) throws TException {
    CLIENT.accessPartition(datasetId, partitionId);
  }

  public synchronized void addPartition(long userId, int datasetId, int partitionId, 
      boolean writeThrough) 
          throws PartitionDoesNotExistException, SuspectedPartitionSizeException, TException {
    CLIENT.addPartition(userId, datasetId, partitionId, writeThrough);
  }

  public synchronized void addDoneRCDPartition(int datasetId, int partitionId, 
      int sizeBytes) throws PartitionDoesNotExistException, SuspectedPartitionSizeException,
      PartitionAlreadyExistException, TException {
    CLIENT.addRCDPartition(datasetId, partitionId, sizeBytes);

  }

  public synchronized void close() {
    mProtocol.getTransport().close();
    mIsConnected = false;
  }

  public static WorkerClient createWorkerClient(InetSocketAddress address) {
    WorkerClient ret = new WorkerClient(address);

    return ret;
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

  public synchronized boolean requestSpace(long userId, long requestBytes)
      throws TException {
    return CLIENT.requestSpace(userId, requestBytes);
  }

  public synchronized void returnSpace(long userId, long returnSpaceBytes)
      throws TException {
    CLIENT.returnSpace(userId, returnSpaceBytes);
  }

  public synchronized void userHeartbeat(long userId) throws TException {
    CLIENT.userHeartbeat(userId);
  }
}