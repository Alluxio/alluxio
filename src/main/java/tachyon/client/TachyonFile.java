package tachyon.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.DataServerMessage;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon File.
 */
public class TachyonFile implements Comparable<TachyonFile> {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  final TachyonFS TFS;
  final int FID;

  private boolean mLockedFile = false;

  TachyonFile(TachyonFS tachyonClient, int fid) {
    TFS = tachyonClient;
    FID = fid;
  }

  /**
   * This API is not recommended to use.
   *
   * @param path file's checkpoint path.
   * @return true if the checkpoint path is added successfully, false otherwise.
   * @throws TException 
   * @throws SuspectedFileSizeException 
   * @throws FileDoesNotExistException 
   * @throws IOException 
   */
  public boolean addCheckpointPath(String path)
      throws FileDoesNotExistException, SuspectedFileSizeException, TException, IOException {
    return TFS.addCheckpointPath(FID, path);
  }

  public InStream getInStream(OpType opType) throws IOException {
    if (opType == null) {
      throw new IOException("OpType can not be null.");
    } else if (opType.isWrite()) {
      throw new IOException("OpType is not read type: " + opType);
    }
    return new InStream(this, opType);
  }

  public OutStream getOutStream(OpType opType) throws IOException {
    if (opType == null) {
      throw new IOException("OpType can not be null.");
    } else if (opType.isRead()) {
      throw new IOException("OpType is not write type: " + opType);
    }
    return new OutStream(this, opType);
  }

  public String getPath() {
    return TFS.getPath(FID);
  }

  public List<String> getLocationHosts() throws IOException {
    List<NetAddress> locations = TFS.getFileNetAddresses(FID);
    List<String> ret = new ArrayList<String>(locations.size());
    if (locations != null) {
      for (int k = 0; k < locations.size(); k ++) {
        ret.add(locations.get(k).mHost);
      }
    }

    return ret;
  }

  public boolean isFile() {
    return !TFS.isFolder(FID);
  }

  public boolean isFolder() {
    return TFS.isFolder(FID);
  }

  public boolean isInLocalMemory() {
    throw new RuntimeException("Unsupported");
  }

  public boolean isInMemory() {
    return TFS.isInMemory(FID);
  }

  public boolean isReady() {
    return TFS.isReady(FID);
  }

  public long length() {
    return TFS.getFileSizeBytes(FID);
  }

  public ByteBuffer readByteBuffer() {
    if (!isReady()) {
      return null;
    }

    mLockedFile = TFS.lockFile(FID);

    ByteBuffer ret = null;
    ret = readLocalByteBuffer();
    if (ret == null) {
      TFS.unlockFile(FID);
      mLockedFile = false;

      // TODO Make it local cache if the OpType is try cache.
      ret = readRemoteByteBuffer();
    }

    return ret;
  }

  private ByteBuffer readLocalByteBuffer() {
    if (TFS.getRootFolder() != null) {
      String localFileName = TFS.getRootFolder() + Constants.PATH_SEPARATOR + FID;
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");
        FileChannel localFileChannel = localFile.getChannel();
        ByteBuffer ret = localFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, localFile.length());
        localFile.close();
        ret.order(ByteOrder.nativeOrder());
        TFS.accessLocalFile(FID);
        return ret;
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.info("Failed to read local file " + localFileName + " with " + e.getMessage());
      } 
    }

    return null;
  }

  private ByteBuffer readRemoteByteBuffer() {
    ByteBuffer ret = null;

    LOG.info("Try to find and read from remote workers.");
    try {
      List<NetAddress> fileLocations = TFS.getFileNetAddresses(FID);
      LOG.info("readByteBufferFromRemote() " + fileLocations);

      for (int k = 0; k < fileLocations.size(); k ++) {
        String host = fileLocations.get(k).mHost;
        int port = fileLocations.get(k).mPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        if (host.equals(InetAddress.getLocalHost().getHostName()) 
            || host.equals(InetAddress.getLocalHost().getHostAddress())) {
          String localFileName = TFS.getRootFolder() + "/" + FID;
          LOG.warn("Master thinks the local machine has data " + localFileName + "! But not!");
        } else {
          LOG.info(host + ":" + (port + 1) +
              " current host is " + InetAddress.getLocalHost().getHostName() + " " +
              InetAddress.getLocalHost().getHostAddress());

          try {
            ret = retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port + 1));
            if (ret != null) {
              break;
            }
          } catch (IOException e) {
            LOG.error(e.getMessage());
            ret = null;
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote " + e.getMessage());
    }

    if (ret != null) {
      ret.order(ByteOrder.nativeOrder());
    }

    return ret;
  }

  // TODO remove this method. do streaming cache.
  public boolean recache() {
    boolean succeed = true;
    String path = TFS.getCheckpointPath(FID);
    UnderFileSystem tHdfsClient = UnderFileSystem.get(path);
    InputStream inputStream;
    try {
      inputStream = tHdfsClient.open(path);
    } catch (IOException e) {
      return false;
    }
    TachyonFile tTFile = TFS.getFile(FID);
    try {
      OutStream os = tTFile.getOutStream(OpType.WRITE_CACHE);
      byte buffer[] = new byte[USER_CONF.FILE_BUFFER_BYTES * 4];

      int limit;
      while ((limit = inputStream.read(buffer)) >= 0) {
        if (limit != 0) {
          try {
            os.write(buffer, 0, limit);
          } catch (IOException e) {
            LOG.warn(e);
            succeed = false;
            break;
          }
        }
      }
      if (succeed) {
        os.close();
      } else {
        os.cancel();
      }
    } catch (IOException e) {
      return false;
    }

    return succeed;
  }

  public void releaseFileLock() {
    if (mLockedFile) {
      TFS.unlockFile(FID);
    }
  }

  public boolean renameTo(String path) {
    // TODO
    throw new RuntimeException("Rename is not supported yet");
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address) 
      throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    LOG.info("Connected to remote machine " + address + " sent");
    DataServerMessage sendMsg = DataServerMessage.createFileRequestMessage(FID);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    LOG.info("Data " + FID + " to remote machine " + address + " sent");

    DataServerMessage recvMsg = DataServerMessage.createFileResponseMessage(false, FID);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        break;
      }
    }
    LOG.info("Data " + FID + " from remote machine " + address + " received");

    socketChannel.close();

    if (!recvMsg.isMessageReady()) {
      LOG.info("Data " + FID + " from remote machine is not ready.");
      return null;
    }

    if (recvMsg.getFileId() < 0) {
      LOG.info("Data " + recvMsg.getFileId() + " is not in remote machine.");
      return null;
    }

    return recvMsg.getReadOnlyData();
  }

  @Override
  public int hashCode() {
    return getPath().hashCode() ^ 1234321;
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj != null) && (obj instanceof TachyonFile)) {
      return compareTo((TachyonFile)obj) == 0;
    }
    return false;
  }

  @Override
  public int compareTo(TachyonFile o) {
    return getPath().compareTo(o.getPath());
  }

  @Override
  public String toString() {
    return getPath();
  }
}