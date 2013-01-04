package tachyon.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.DataServerMessage;
import tachyon.CommonUtils;
import tachyon.HdfsClient;
import tachyon.thrift.NetAddress;
import tachyon.thrift.OutOfMemoryForPinDatasetException;
import tachyon.thrift.PartitionAlreadyExistException;
import tachyon.thrift.PartitionDoesNotExistException;
import tachyon.thrift.PartitionInfo;
import tachyon.thrift.SuspectedPartitionSizeException;

/**
 * Dataset partition handler.
 * 
 * @author haoyuan
 */
public class Partition {
  private final Logger LOG = LoggerFactory.getLogger(Partition.class);
  private final TachyonClient mTachyonClient;
  private final Dataset mDataset;
  private final int mDatasetId;
  private final int mPartitionId;

  private PartitionInfo mPartitionInfo;
  private boolean mOpen = false;
  private boolean mRead;
  private boolean mWriteThrough;
  private int mSizeBytes;
  private File mFolder;
  private String mFilePath;
  private RandomAccessFile mFile;
  private FileSplit mHDFSFileSplit = null;

  private FileChannel mInChannel;
  private ByteBuffer mInByteBuffer;

  private FileChannel mOutChannel;
  private MappedByteBuffer mOut;
  private ByteBuffer mOutBuffer;

  public Partition(TachyonClient tachyonClient, Dataset dataset, int datasetId, int pId) {
    mTachyonClient = tachyonClient;
    mDataset = dataset;
    mDatasetId = datasetId;
    mPartitionId = pId;
  }

  private synchronized void appendCurrentOutBuffer(int minimalPosition) throws IOException {
    if (mOutBuffer.position() >= minimalPosition) {
      if (mSizeBytes != mFile.length()) {
        CommonUtils.runtimeException(
            String.format("mSize (%d) != mFile.length() (%d)", mSizeBytes, mFile.length()));
      }

      if (!mTachyonClient.requestSpace(mOutBuffer.position())) {
        throw new IOException("Local tachyon worker does not have enough space.");
      }
      mOut = mOutChannel.map(MapMode.READ_WRITE, mSizeBytes, mOutBuffer.position());
      mSizeBytes += mOutBuffer.position();
      mOutBuffer.flip();
      mOut.put(mOutBuffer);
      mOutBuffer.clear();
    }
  }

  public void append(byte b) throws IOException {
    validateIO(false);

    appendCurrentOutBuffer(Config.USER_BUFFER_PER_PARTITION_BYTES);

    mOutBuffer.put(b);
  }

  public void append(int b) throws IOException {
    validateIO(false);

    appendCurrentOutBuffer(Config.USER_BUFFER_PER_PARTITION_BYTES);

    mOutBuffer.putInt(b);
  }

  public void append(byte[] buf) throws IOException, OutOfMemoryForPinDatasetException {
    append(buf, 0, buf.length);
  }

  public void append(byte[] buf, int off, int len) 
      throws IOException, OutOfMemoryForPinDatasetException {
    validateIO(false);

    if (mOutBuffer.position() + len >= Config.USER_BUFFER_PER_PARTITION_BYTES) {
      if (mSizeBytes != mFile.length()) {
        CommonUtils.runtimeException(
            String.format("mSize (%d) != mFile.length() (%d)", mSizeBytes, mFile.length()));
      }

      if (!mTachyonClient.requestSpace(mOutBuffer.position() + len)) {
        if (mDataset.needPin()) {
          mTachyonClient.outOfMemoryForPinDataset(mDatasetId);
          throw new OutOfMemoryForPinDatasetException("Local tachyon worker does not have enough space " +
              "or no worker for " + mDatasetId + ":" + mPartitionId);
        }
        throw new IOException("Local tachyon worker does not have enough space or no worker.");
      }
      mOut = mOutChannel.map(MapMode.READ_WRITE, mSizeBytes, mOutBuffer.position() + len);
      mSizeBytes += mOutBuffer.position() + len;

      mOutBuffer.flip();
      mOut.put(mOutBuffer);
      mOutBuffer.clear();
      mOut.put(buf, off, len);
    } else {
      mOutBuffer.put(buf, off, len);
    }
  }

  public void append(ByteBuffer buf) throws IOException, OutOfMemoryForPinDatasetException {
    append(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void append(ArrayList<ByteBuffer> bufs) 
      throws IOException, OutOfMemoryForPinDatasetException {
    for (int k = 0; k < bufs.size(); k ++) {
      append(bufs.get(k));
    }
  }

  public void cancel() {
    close(true);
  }

  public void close()  {
    close(false);
  }

  private void close(boolean cancel) {
    if (! mOpen) {
      return;
    }

    try {
      if (mRead) {
        if (mInChannel != null) {
          mInChannel.close();
          mFile.close();
        }
      } else {
        if (mOutChannel != null) {
          if (!cancel) {
            appendCurrentOutBuffer(1);
          }

          mOutChannel.close();
          mFile.close();
        }

        if (cancel) {
          mTachyonClient.releaseSpace(mSizeBytes);
        } else {
          if (mWriteThrough) {
            String hdfsFolder = mTachyonClient.createAndGetUserHDFSTempFolder();
            HdfsClient tHdfsClient = new HdfsClient(hdfsFolder);
            tHdfsClient.copyFromLocalFile(false, true, mFilePath,
                hdfsFolder + "/" + mDatasetId + "-" + mPartitionId);
          }

          if (!mTachyonClient.addDonePartition(mDatasetId, mPartitionId, mWriteThrough)) {
            throw new IOException("Failed to add a partition to the tachyon system.");
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } catch (SuspectedPartitionSizeException e) {
      LOG.error(e.getMessage(), e);
    } catch (PartitionDoesNotExistException e) {
      LOG.error(e.getMessage(), e);
    } catch (PartitionAlreadyExistException e) {
      LOG.error(e.getMessage(), e);
    }
    mTachyonClient.unlockPartition(mDatasetId, mPartitionId);

    mOpen = false;
  }

  public FileSplit getHdfsFileSplit() {
    return mHDFSFileSplit;
  }

  public PartitionInputStream getInputStream() {
    return new PartitionInputStream(this);
  }

  public PartitionOutputStream getOutputStream() {
    return new PartitionOutputStream(this);
  }

  public int getSize() {
    return mSizeBytes;
  }

  public void open(String wr) throws IOException {
    open(wr, false);
  }

  public void open(String wr, boolean writeThrough) throws IOException {
    if (wr.equals("r")) {
      mRead = true;
    } else if (wr.equals("w")) {
      mRead = false;
      mWriteThrough = writeThrough;
    } else {
      CommonUtils.runtimeException("Wrong option to open a partition: " + wr);
    }

    mOpen = true;

    if (!mRead) {
      mFolder = mTachyonClient.createAndGetUserTempFolder();
      if (mFolder == null) {
        throw new IOException("Failed to create temp user folder for tachyon client.");
      }
      mFilePath = mFolder.getPath() + "/" + mDatasetId + "-" + mPartitionId;
      mFile = new RandomAccessFile(mFilePath, "rw");
      mOutChannel = mFile.getChannel();
      mSizeBytes = 0;
      LOG.info("File " + mFilePath + " is there!");
      mOutBuffer = ByteBuffer.allocate(Config.USER_BUFFER_PER_PARTITION_BYTES + 4);
      mOutBuffer.order(ByteOrder.nativeOrder());
    } else {
      mInByteBuffer = readByteBuffer();
      mTachyonClient.lockPartition(mDatasetId, mPartitionId);
    }
  }
  
  public int read() throws IOException {
    return mInByteBuffer.get();
  }

  // TODO Need to have append/write() like READ API!
  public ByteBuffer readByteBuffer() 
      throws UnknownHostException, FileNotFoundException, IOException {
    validateIO(true);

    ByteBuffer ret = null;

    ret = readByteBufferFromLocal();
    if (ret == null) {
      ret = readByteBufferFromRemote();
    }
    
    if (ret != null) {
      return ret;
    }

    boolean recacheSucceed = recacheData();

    if (recacheSucceed) {
      ret = readByteBufferFromLocal();
    }

    if (ret == null) {
      new IOException("Failed to read Dataset " + mDataset.getDatasetPath() +
          " Partition " + mPartitionId);
    }
    return ret;
  }

  private ByteBuffer readByteBufferFromLocal() throws IOException {
    ByteBuffer ret = null;

    if (mTachyonClient.getRootFolder() != null) {
      mFolder = new File(mTachyonClient.getRootFolder());
      String localFileName = mFolder.getPath() + "/" + mDatasetId + "-" + mPartitionId;
      try {
        mFile = new RandomAccessFile(localFileName, "r");
        mSizeBytes = (int) mFile.length();
        mInChannel = mFile.getChannel();
        ret = mInChannel.map(FileChannel.MapMode.READ_ONLY, 0, mSizeBytes);
        ret.order(ByteOrder.nativeOrder());
        mTachyonClient.accessLocalPartition(mDatasetId, mPartitionId);
        return ret;
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
        ret = null;
      }
    }

    return ret;
  }

  private ByteBuffer readByteBufferFromRemote() throws IOException {
    ByteBuffer ret = null;

    LOG.info("Try to find and read from remote workers.");

    mPartitionInfo = mTachyonClient.getPartitionInfo(mDatasetId, mPartitionId);

    if (mPartitionInfo == null) {
      throw new IOException("Can not find info about " + mDatasetId + " " + mPartitionId);
    }

    mSizeBytes = mPartitionInfo.mSizeBytes;

    LOG.info("readByteBuffer() PartitionInfo " + mPartitionInfo);

    for (Entry<Long, NetAddress> entry : mPartitionInfo.mLocations.entrySet()) {
      String host = entry.getValue().mHost;
      if (host.equals(InetAddress.getLocalHost().getHostAddress())) {
        String localFileName = mFolder.getPath() + "/" + mDatasetId + "-" + mPartitionId;
        LOG.error("Master thinks the local machine has data! But " + localFileName + " is not!");
      } else {
        LOG.info("readByteBuffer() Read from remote machine: " + host + ":" +
            Config.WORKER_DATA_SERVER_PORT);
        try {
          ret = retrieveByteBufferFromRemoteMachine(
              new InetSocketAddress(host, Config.WORKER_DATA_SERVER_PORT));
          if (ret != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }

    return ret;
  }

  private boolean recacheData() throws IOException {
    if (mPartitionInfo == null || !mPartitionInfo.mHasCheckpointed) {
      return false;
    }

    String path = mPartitionInfo.mCheckpointPath;
    if (!Config.USING_HDFS) {
      return false;
    }

    HdfsClient tHdfsClient = new HdfsClient(path);
    FSDataInputStream inputStream = tHdfsClient.open(path);
    Partition tPartition = mDataset.getPartition(mPartitionId);
    tPartition.open("w", false);
    byte buffer[] = new byte[Config.USER_BUFFER_PER_PARTITION_BYTES * 4];

    int limit;
    while ((limit = inputStream.read(buffer)) >= 0) {
      if (limit != 0) {
        try {
          tPartition.append(buffer, 0, limit);
        } catch (IOException e) {
          LOG.error(e.getMessage());
          return false;
        } catch (OutOfMemoryForPinDatasetException e) {
          CommonUtils.runtimeException(e);
        }
      }
    }

    tPartition.close();
    
    return true;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address) 
      throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    DataServerMessage sendMsg = 
        DataServerMessage.createPartitionRequestMessage(mDatasetId, mPartitionId);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    DataServerMessage recvMsg = 
        DataServerMessage.createPartitionResponseMessage(false, mDatasetId, mPartitionId);
    while (!recvMsg.isMessageReady()) {
      recvMsg.recv(socketChannel);
    }

    socketChannel.close();

    if (recvMsg.getDatasetId() < 0) {
      LOG.info("Data " + recvMsg.getDatasetId() + ":" + recvMsg.getPartitionId() + 
          " is not in remote machine.");
      return null;
    }

    return recvMsg.getReadOnlyData();
  }

  public void setHDFSFileSplit(FileSplit fs) {
    mHDFSFileSplit = fs;
  }

  private void validateIO(boolean read) {
    if (!mOpen) {
      CommonUtils.runtimeException("The partition was never openned or has been closed.");
    }
    if (read != mRead) {
      CommonUtils.runtimeException("The partition was opened for " + 
          (mRead ? "Read" : "Write") + ". " + 
          (read ? "Read" : "Write") + " operation is not available.");
    }
  }
}