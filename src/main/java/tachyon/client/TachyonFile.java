package tachyon.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Config;
import tachyon.DataServerMessage;
import tachyon.CommonUtils;
import tachyon.HdfsClient;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
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

  private OpType mIoType = null;
  private long mSizeBytes;

  private ByteBuffer mBuffer;

  private RandomAccessFile mLocalFile;
  private FileChannel mLocalFileChannel;

  // TODO Use mCheckpointInputStream
  private InputStream mCheckpointInputStream;
  private OutputStream mCheckpointOutputStream;

  public TachyonFile(TachyonClient tachyonClient, ClientFileInfo fileInfo) {
    mTachyonClient = tachyonClient;
    mClientFileInfo = fileInfo;
    mId = mClientFileInfo.getId();
  }

  /**
   * This API is not recommended to use.
   * 
   * @param path file's checkpoint path.
   * @return true if the checkpoint path is added successfully, false otherwise.
   * @throws TException 
   * @throws SuspectedFileSizeException 
   * @throws FileDoesNotExistException 
   */
  public boolean addCheckpointPath(String path)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException {
    HdfsClient tHdfsClient = new HdfsClient(path);
    long sizeBytes = tHdfsClient.getFileSize(path);
    if (mTachyonClient.addCheckpointPath(mId, path, sizeBytes)) {
      mClientFileInfo.sizeBytes = sizeBytes;
      mClientFileInfo.checkpointPath = path;
      return true;
    }

    return false;
  }

  private synchronized void appendCurrentBuffer(int minimalPosition) throws IOException {
    if (mBuffer.position() >= minimalPosition) {
      if (mIoType.isWriteCache()) {
        if (mSizeBytes != mLocalFile.length()) {
          CommonUtils.runtimeException(
              String.format("mSize (%d) != mFile.length() (%d)", mSizeBytes, mLocalFile.length()));
        }

        if (!mTachyonClient.requestSpace(mBuffer.position())) {
          if (mClientFileInfo.isNeedPin()) {
            mTachyonClient.outOfMemoryForPinFile(mId);
            throw new IOException("Local tachyon worker does not have enough " +
                "space or no worker for " + mId);
          }
          throw new IOException("Local tachyon worker does not have enough space.");
        }
        mBuffer.flip();
        MappedByteBuffer out = 
            mLocalFileChannel.map(MapMode.READ_WRITE, mSizeBytes, mBuffer.limit());
        out.put(mBuffer);
      }

      if (mIoType.isWriteThrough()) {
        mBuffer.flip();
        mCheckpointOutputStream.write(mBuffer.array(), 0, mBuffer.limit());
      }

      mSizeBytes += mBuffer.limit();
      mBuffer.clear();
    }
  }

  public void append(byte b) throws IOException {
    validateIO(false);

    appendCurrentBuffer(Config.USER_BUFFER_PER_PARTITION_BYTES);

    mBuffer.put(b);
  }

  public void append(int b) throws IOException {
    validateIO(false);

    appendCurrentBuffer(Config.USER_BUFFER_PER_PARTITION_BYTES);

    mBuffer.putInt(b);
  }

  public void append(byte[] buf) throws IOException {
    append(buf, 0, buf.length);
  }

  public void append(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }

    validateIO(false);

    if (mBuffer.position() + len >= Config.USER_BUFFER_PER_PARTITION_BYTES) {
      if (mIoType.isWriteCache()) {
        if (mSizeBytes != mLocalFile.length()) {
          CommonUtils.runtimeException(
              String.format("mSize (%d) != mFile.length() (%d)", mSizeBytes, mLocalFile.length()));
        }

        if (!mTachyonClient.requestSpace(mBuffer.position() + len)) {
          if (mClientFileInfo.isNeedPin()) {
            mTachyonClient.outOfMemoryForPinFile(mId);
            throw new IOException("Local tachyon worker does not have enough " +
                "space or no worker for " + mId);
          }
          throw new IOException("Local tachyon worker does not have enough space or no worker.");
        }

        mBuffer.flip();
        MappedByteBuffer out =
            mLocalFileChannel.map(MapMode.READ_WRITE, mSizeBytes, mBuffer.limit() + len);
        out.put(mBuffer);
        out.put(b, off, len);
      }

      if (mIoType.isWriteThrough()) {
        mBuffer.flip();
        mCheckpointOutputStream.write(mBuffer.array(), 0, mBuffer.limit());
        mCheckpointOutputStream.write(b, off, len);
      }

      mSizeBytes += mBuffer.limit();
      mBuffer.clear();
    } else {
      mBuffer.put(b, off, len);
    }
  }

  public void append(ByteBuffer buf) throws IOException {
    append(buf.array(), buf.position(), buf.limit() - buf.position());
  }

  public void append(ArrayList<ByteBuffer> bufs) throws IOException {
    for (int k = 0; k < bufs.size(); k ++) {
      append(bufs.get(k));
    }
  }

  public void cancel() throws IOException {
    close(true);
  }

  public void close() throws IOException  {
    close(false);
  }

  private void close(boolean cancel) throws IOException {
    if (mIoType == null) {
      return;
    }

    IOException ioE = null;

    try {
      if (mIoType.isRead()) {
        if (mLocalFileChannel != null) {
          mLocalFileChannel.close();
          mLocalFile.close();
        }
      } else {
        if (!cancel) {
          appendCurrentBuffer(1);
        }

        if (mLocalFileChannel != null) {
          mLocalFileChannel.close();
          mLocalFile.close();
        }

        if (cancel) {
          mTachyonClient.releaseSpace(mSizeBytes);
        } else {
          if (mIoType.isWriteThrough()) {
            mCheckpointOutputStream.flush();
            mCheckpointOutputStream.close();
            mTachyonClient.addCheckpoint(mId);
          }

          if (mIoType.isWriteCache()) {
            mTachyonClient.cacheFile(mId);
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      ioE = e;
    } catch (SuspectedFileSizeException e) {
      LOG.error(e.getMessage(), e);
      ioE = new IOException(e);
    } catch (FileDoesNotExistException e) {
      LOG.error(e.getMessage(), e);
      ioE = new IOException(e);
    } catch (FailedToCheckpointException e) {
      LOG.error(e.getMessage(), e);
      ioE = new IOException(e);
    }
    mTachyonClient.unlockFile(mId);

    mIoType = null;

    if (ioE != null) {
      throw ioE;
    }
  }

  public InputStream getInputStream() throws IOException {
    validateIO(true);
    return new TFileInputStream(this);
  }

  public OutputStream getOutputStream() throws IOException {
    validateIO(false);
    return new TFileOutputStream(this);
  }

  public long getSize() {
    return mSizeBytes;
  }

  public List<String> getLocationHosts() {
    List<NetAddress> locations = mTachyonClient.getFileLocations(mId);
    List<String> ret = new ArrayList<String>(locations.size());
    if (locations != null) {
      for (int k = 0; k < locations.size(); k ++) {
        ret.add(locations.get(k).mHost);
      }
    }

    return ret;
  }

  public void open(OpType io) throws IOException {
    if (io == null) {
      throw new IOException("OpType can not be null.");
    }

    mIoType = io;

    if (mIoType.isWrite()) {
      mBuffer = ByteBuffer.allocate(Config.USER_BUFFER_PER_PARTITION_BYTES + 4);
      mBuffer.order(ByteOrder.nativeOrder());

      if (mIoType.isWriteCache()) {
        if (!mTachyonClient.hasLocalWorker()) {
          throw new IOException("No local worker on this machine.");
        }
        File localFolder = mTachyonClient.createAndGetUserTempFolder();
        if (localFolder == null) {
          throw new IOException("Failed to create temp user folder for tachyon client.");
        }
        String localFilePath = localFolder.getPath() + "/" + mId;
        mLocalFile = new RandomAccessFile(localFilePath, "rw");
        mLocalFileChannel = mLocalFile.getChannel();
        mSizeBytes = 0;
        LOG.info("File " + localFilePath + " was created!");
      }

      if (mIoType.isWriteThrough()) {
        String hdfsFolder = mTachyonClient.createAndGetUserHDFSTempFolder();
        HdfsClient tHdfsClient = new HdfsClient(hdfsFolder);
        mCheckpointOutputStream = tHdfsClient.create(hdfsFolder + "/" + mId);
      }
    } else {
      mTachyonClient.lockFile(mId);
      mBuffer = null;
      mCheckpointInputStream = null;
      if (mIoType.isReadTryCache()) {
        mBuffer = readByteBuffer();
      }
      if (mBuffer == null && !mClientFileInfo.checkpointPath.equals("")) {
        HdfsClient tHdfsClient = new HdfsClient(mClientFileInfo.checkpointPath);
        mCheckpointInputStream = tHdfsClient.open(mClientFileInfo.checkpointPath);
      }
      if (mBuffer == null && mCheckpointInputStream == null) {
        throw new IOException("Can not find file " + mClientFileInfo.getPath());
      }
    }
  }

  public int read() throws IOException {
    validateIO(true);
    if (mBuffer != null) {
      try {
        return mBuffer.get();
      } catch (java.nio.BufferUnderflowException e) {
        close();
        return -1;
      }
    }
    return mCheckpointInputStream.read();
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

    validateIO(true);
    if (mBuffer != null) {
      int ret = Math.min(len, mBuffer.remaining());
      if (ret == 0) {
        close();
        return -1;
      }
      mBuffer.get(b, off, len);
      return ret;
    }

    return mCheckpointInputStream.read(b, off, len);
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

    if (mClientFileInfo.checkpointPath.equals("")) {
      throw new IOException("Failed to read file " + mClientFileInfo.getPath() + " no CK or Cache");
    }

    if (mIoType.isReadTryCache() && mTachyonClient.hasLocalWorker()) {
      boolean recacheSucceed = recacheData();
      if (recacheSucceed) {
        ret = readByteBufferFromLocal();
      }
    }

    return ret;
  }

  private ByteBuffer readByteBufferFromLocal() throws IOException {
    ByteBuffer ret = null;

    if (mTachyonClient.getRootFolder() != null) {
      String localFileName = mTachyonClient.getRootFolder() + "/" + mId;
      try {
        mLocalFile = new RandomAccessFile(localFileName, "r");
        mSizeBytes = mLocalFile.length();
        mLocalFileChannel = mLocalFile.getChannel();
        ret = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, mSizeBytes);
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

    List<NetAddress> fileLocations = mTachyonClient.getFileLocations(mId);

    if (fileLocations == null) {
      throw new IOException("Can not find location info: " + mClientFileInfo.getPath() + " " + mId);
    }

    LOG.info("readByteBufferFromRemote() " + fileLocations);

    for (int k = 0 ;k < fileLocations.size(); k ++) {
      String host = fileLocations.get(k).mHost;
      if (host.equals(InetAddress.getLocalHost().getHostName()) 
          || host.equals(InetAddress.getLocalHost().getHostAddress())) {
        String localFileName = mTachyonClient.getRootFolder() + "/" + mId;
        LOG.warn("Master thinks the local machine has data! But " + localFileName + " is not!");
      } else {
        LOG.info("readByteBufferFromRemote() : " + host + ":" + Config.WORKER_DATA_SERVER_PORT +
            " current host is " + InetAddress.getLocalHost().getHostName() + " " +
            InetAddress.getLocalHost().getHostAddress());
        try {
          // TODO Using Config.WORKER_DATA_SERVER_PORT here is a bug. Fix it later.
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

    if (ret != null) {
      mSizeBytes = ret.limit();
      ret.order(ByteOrder.nativeOrder());
    }

    return ret;
  }

  private boolean recacheData() throws IOException {
    String path = mClientFileInfo.checkpointPath;
    HdfsClient tHdfsClient = new HdfsClient(path);
    FSDataInputStream inputStream = tHdfsClient.open(path);
    TachyonFile tTFile = mTachyonClient.getFile(mClientFileInfo.getId());
    tTFile.open(OpType.WRITE_CACHE);
    byte buffer[] = new byte[Config.USER_BUFFER_PER_PARTITION_BYTES * 4];

    int limit;
    while ((limit = inputStream.read(buffer)) >= 0) {
      if (limit != 0) {
        tTFile.append(buffer, 0, limit);
      }
    }

    tTFile.close();

    return true;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address) 
      throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    LOG.info("Connected to remote machine " + address + " sent");
    DataServerMessage sendMsg = DataServerMessage.createPartitionRequestMessage(mId);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    LOG.info("Data " + mId + " to remote machine " + address + " sent");

    DataServerMessage recvMsg = DataServerMessage.createPartitionResponseMessage(false, mId);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    LOG.info("Data " + mId + " from remote machine " + address + " received");

    socketChannel.close();

    if (!recvMsg.isMessageReady()) {
      LOG.info("Data " + mId + " from remote machine is not ready.");
      return null;
    }

    if (recvMsg.getFileId() < 0) {
      LOG.info("Data " + recvMsg.getFileId() + " is not in remote machine.");
      return null;
    }

    return recvMsg.getReadOnlyData();
  }

  private void validateIO(boolean read) throws IOException {
    if (mIoType == null) {
      CommonUtils.runtimeException("The partition was never openned or has been closed.");
    }
    if (read != mIoType.isRead()) {
      CommonUtils.runtimeException("The partition was opened for " + 
          (mIoType.isRead() ? "Read" : "Write") + ". " + 
          (read ? "Read" : "Write") + " operation is not available.");
    }
  }

  public boolean isReady() {
    return mClientFileInfo.isReady();
  }
}