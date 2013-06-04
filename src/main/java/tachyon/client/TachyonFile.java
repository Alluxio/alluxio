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
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.NetAddress;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon File.
 */
public class TachyonFile {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final UserConf USER_CONF = UserConf.get();

  final TachyonFS CLIENT;
  final ClientFileInfo CLIENT_FILE_INFO;
  final int FID;

  private boolean mLockedFile = false;

  public TachyonFile(TachyonFS tachyonClient, ClientFileInfo fileInfo) {
    CLIENT = tachyonClient;
    CLIENT_FILE_INFO = fileInfo;
    FID = CLIENT_FILE_INFO.getId();
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
    UnderFileSystem ufs = UnderFileSystem.getUnderFileSystem(path);
    long sizeBytes = ufs.getFileSize(path);
    if (CLIENT.addCheckpointPath(FID, path)) {
      CLIENT_FILE_INFO.sizeBytes = sizeBytes;
      CLIENT_FILE_INFO.checkpointPath = path;
      return true;
    }

    return false;
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
    return CLIENT_FILE_INFO.getPath();
  }

  public long getSize() {
    return CLIENT_FILE_INFO.getSizeBytes();
  }

  public List<String> getLocationHosts() throws IOException {
    List<NetAddress> locations = CLIENT.getFileNetAddresses(FID);
    List<String> ret = new ArrayList<String>(locations.size());
    if (locations != null) {
      for (int k = 0; k < locations.size(); k ++) {
        ret.add(locations.get(k).mHost);
      }
    }

    return ret;
  }

  public long length() {
    return CLIENT_FILE_INFO.sizeBytes;
  }

  public ByteBuffer readByteBuffer() {
    if (!isReady()) {
      return null;
    }

    mLockedFile = CLIENT.lockFile(FID);

    ByteBuffer ret = null;
    ret = readByteBufferFromLocal();
    if (ret == null) {
      CLIENT.unlockFile(FID);
      mLockedFile = false;

      // TODO Make it local cache if the OpType is try cache.
      ret = readByteBufferFromRemote();
    }

    return ret;
  }

  private ByteBuffer readByteBufferFromLocal() {
    if (CLIENT.getRootFolder() != null) {
      String localFileName = CLIENT.getRootFolder() + Constants.PATH_SEPARATOR + FID;
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");
        FileChannel localFileChannel = localFile.getChannel();
        ByteBuffer ret = localFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, localFile.length());
        localFile.close();
        ret.order(ByteOrder.nativeOrder());
        CLIENT.accessLocalFile(FID);
        return ret;
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.info("Failed to read local file " + localFileName + " with " + e.getMessage());
      } 
    }

    return null;
  }

  private ByteBuffer readByteBufferFromRemote() {
    ByteBuffer ret = null;

    LOG.info("Try to find and read from remote workers.");
    try {
      List<NetAddress> fileLocations = CLIENT.getFileNetAddresses(FID);
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
          String localFileName = CLIENT.getRootFolder() + "/" + FID;
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

  public boolean recacheData() {
    boolean succeed = true;
    String path = CLIENT_FILE_INFO.checkpointPath;
    UnderFileSystem tHdfsClient = UnderFileSystem.getUnderFileSystem(path);
    InputStream inputStream;
    try {
      inputStream = tHdfsClient.open(path);
    } catch (IOException e) {
      return false;
    }
    TachyonFile tTFile = CLIENT.getFile(CLIENT_FILE_INFO.getId());
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
        CLIENT_FILE_INFO.setInMemory(true);
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
      CLIENT.unlockFile(FID);
    }
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

  public boolean isInMemory() {
    // TODO Make this query the master.
    return CLIENT_FILE_INFO.isInMemory();
  }

  public boolean isReady() {
    return CLIENT_FILE_INFO.isReady();
  }
}