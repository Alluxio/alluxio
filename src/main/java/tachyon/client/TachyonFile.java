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
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.DataServerMessage;
import tachyon.CommonUtils;
import tachyon.HdfsClient;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.OutOfMemoryForPinFileException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon File.
 * @author haoyuan
 */
public class TachyonFile {
  private final Logger LOG = LoggerFactory.getLogger(TachyonFile.class);
  private final TachyonClient mTachyonClient;
  private final ClientFileInfo mClientFileInfo;
  private final int mId;

  private String mFileName;
  private List<NetAddress> mLocations;

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

  public TachyonFile(TachyonClient tachyonClient, ClientFileInfo fileInfo) {
    mTachyonClient = tachyonClient;
    mClientFileInfo = fileInfo;
    mId = mClientFileInfo.getId();
    mFileName = mClientFileInfo.getFileName();
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

  public void append(byte[] buf) throws IOException, OutOfMemoryForPinFileException {
    append(buf, 0, buf.length);
  }

  public void append(byte[] b, int off, int len) 
      throws IOException, OutOfMemoryForPinFileException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    validateIO(false);

    if (mOutBuffer.position() + len >= Config.USER_BUFFER_PER_PARTITION_BYTES) {
      if (mSizeBytes != mFile.length()) {
        CommonUtils.runtimeException(
            String.format("mSize (%d) != mFile.length() (%d)", mSizeBytes, mFile.length()));
      }

      if (!mTachyonClient.requestSpace(mOutBuffer.position() + len)) {
        if (mClientFileInfo.isNeedPin()) {
          mTachyonClient.outOfMemoryForPinDataset(mId);
          throw new OutOfMemoryForPinFileException("Local tachyon worker does not have enough " +
              "space or no worker for " + mId);
        }
        throw new IOException("Local tachyon worker does not have enough space or no worker.");
      }
      mOut = mOutChannel.map(MapMode.READ_WRITE, mSizeBytes, mOutBuffer.position() + len);
      mSizeBytes += mOutBuffer.position() + len;

      mOutBuffer.flip();
      mOut.put(mOutBuffer);
      mOutBuffer.clear();
      mOut.put(b, off, len);
    } else {
      mOutBuffer.put(b, off, len);
    }
  }

  public void append(ByteBuffer buf) throws IOException, OutOfMemoryForPinFileException {
    append(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void append(ArrayList<ByteBuffer> bufs) 
      throws IOException, OutOfMemoryForPinFileException {
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
            tHdfsClient.copyFromLocalFile(false, true, mFilePath, hdfsFolder + "/" + mId);
          }

          if (!mTachyonClient.addDoneFile(mId, mWriteThrough)) {
            throw new IOException("Failed to add a partition to the tachyon system.");
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    } catch (SuspectedFileSizeException e) {
      LOG.error(e.getMessage(), e);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage(), e);
    } catch (FileAlreadyExistException e) {
      LOG.error(e.getMessage(), e);
    }
    mTachyonClient.unlockFile(mId);

    mOpen = false;
  }

  public FileSplit getHdfsFileSplit() {
    return mHDFSFileSplit;
  }

  public TFileInputStream getInputStream() {
    validateIO(true);
    return new TFileInputStream(this);
  }

  public TFileOutputStream getOutputStream() {
    validateIO(false);
    return new TFileOutputStream(this);
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
      mFilePath = mFolder.getPath() + "/" + mId;
      mFile = new RandomAccessFile(mFilePath, "rw");
      mOutChannel = mFile.getChannel();
      mSizeBytes = 0;
      LOG.info("File " + mFilePath + " is there!");
      mOutBuffer = ByteBuffer.allocate(Config.USER_BUFFER_PER_PARTITION_BYTES + 4);
      mOutBuffer.order(ByteOrder.nativeOrder());
    } else {
      mInByteBuffer = readByteBuffer();
      mTachyonClient.lockFile(mId);
    }
  }

  public int read() throws IOException {
    return mInByteBuffer.get();
  }

  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int ret = Math.min(len, mInByteBuffer.remaining());
    mInByteBuffer.get(b, off, len);
    return ret;
  }

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
      new IOException("Failed to read file " + mFileName);
    }
    return ret;
  }

  private ByteBuffer readByteBufferFromLocal() throws IOException {
    ByteBuffer ret = null;

    if (mTachyonClient.getRootFolder() != null) {
      mFolder = new File(mTachyonClient.getRootFolder());
      String localFileName = mFolder.getPath() + "/" + mId;
      try {
        mFile = new RandomAccessFile(localFileName, "r");
        mSizeBytes = (int) mFile.length();
        mInChannel = mFile.getChannel();
        ret = mInChannel.map(FileChannel.MapMode.READ_ONLY, 0, mSizeBytes);
        ret.order(ByteOrder.nativeOrder());
        mTachyonClient.accessLocalFile(mId);
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

    mLocations = mTachyonClient.getFileLocations(mId);

    if (mLocations == null) {
      throw new IOException("Can not find location info about " + mFileName + " " + mId);
    }

    LOG.info("readByteBuffer() PartitionInfo " + mLocations);

    for (int k = 0 ;k < mLocations.size(); k ++) {
      String host = mLocations.get(k).mHost;
      if (host.equals(InetAddress.getLocalHost().getHostAddress())) {
        String localFileName = mFolder.getPath() + "/" + mId;
        LOG.error("Master thinks the local machine has data! But " + localFileName + " is not!");
      } else {
        LOG.info("readByteBuffer() Read from remote machine: " + host + ":" +
            Config.WORKER_DATA_SERVER_PORT);
        try {
          ret = retrieveByteBufferFromRemoteMachine(
              new InetSocketAddress(host, mLocations.get(k).mPort));
          if (ret != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }

    mSizeBytes = ret.limit();

    return ret;
  }

  private boolean recacheData() throws IOException {
    if (mClientFileInfo.checkpointPath.equals("")) {
      return false;
    }

    String path = mClientFileInfo.checkpointPath;
    if (!Config.USING_HDFS) {
      return false;
    }

    HdfsClient tHdfsClient = new HdfsClient(path);
    FSDataInputStream inputStream = tHdfsClient.open(path);
    TachyonFile tTFile = mTachyonClient.getFile(mFileName);
    tTFile.open("w", false);
    byte buffer[] = new byte[Config.USER_BUFFER_PER_PARTITION_BYTES * 4];

    int limit;
    while ((limit = inputStream.read(buffer)) >= 0) {
      if (limit != 0) {
        try {
          tTFile.append(buffer, 0, limit);
        } catch (IOException e) {
          LOG.error(e.getMessage());
          return false;
        } catch (OutOfMemoryForPinFileException e) {
          CommonUtils.runtimeException(e);
        }
      }
    }

    tTFile.close();

    return true;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address) 
      throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    DataServerMessage sendMsg = DataServerMessage.createPartitionRequestMessage(mId);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    DataServerMessage recvMsg = DataServerMessage.createPartitionResponseMessage(false, mId);
    while (!recvMsg.isMessageReady()) {
      recvMsg.recv(socketChannel);
    }

    socketChannel.close();

    if (recvMsg.getFileId() < 0) {
      LOG.info("Data " + recvMsg.getFileId() + " is not in remote machine.");
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