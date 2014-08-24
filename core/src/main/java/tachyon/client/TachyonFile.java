package tachyon.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.worker.DataServerMessage;

/**
 * Tachyon File.
 */
public class TachyonFile implements Comparable<TachyonFile> {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  final TachyonFS mTachyonFS;
  final int mFileId;
  private final UserConf USER_CONF = UserConf.get();

  private Object mUFSConf = null;

  /**
   * A Tachyon File handler, based file id
   *
   * @param tfs
   *          the Tachyon file system client handler
   * @param fid
   *          the file id
   */
  TachyonFile(TachyonFS tfs, int fid) {
    mTachyonFS = tfs;
    mFileId = fid;
  }

  @Override
  public int compareTo(TachyonFile o) {
    if (mFileId == o.mFileId) {
      return 0;
    }
    return mFileId < o.mFileId ? -1 : 1;
  }

  @Override
  public boolean equals(Object obj) {
    if ((obj != null) && (obj instanceof TachyonFile)) {
      return compareTo((TachyonFile) obj) == 0;
    }
    return false;
  }

  /**
   * Return the block id of a block in the file, specified by blockIndex
   *
   * @param blockIndex
   *          the index of the block in this file
   * @return the block id
   * @throws IOException
   */
  public long getBlockId(int blockIndex) throws IOException {
    return mTachyonFS.getBlockId(mFileId, blockIndex);
  }

  /**
   * Return the block's size of this file
   *
   * @return the block's size in bytes
   * @throws IOException
   */
  public long getBlockSizeByte() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", true).getBlockSizeByte();
  }

  /**
   * Return the creation time of this file
   *
   * @return the creation time, in milliseconds
   * @throws IOException
   */
  public long getCreationTimeMs() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", true).getCreationTimeMs();
  }

  public int getDiskReplication() {
    // TODO Implement it.
    return 3;
  }

  /**
   * Return the InStream of this file, use the specified read type. If it has no block, return an
   * EmptyBlockInStream. Else if it has only one block ,return a BlockInStream of the block. Else,
   * return a FileInStream.
   *
   * @param readType
   *          the InStream's read type
   * @return the InStream
   * @throws IOException
   */
  public InStream getInStream(ReadType readType) throws IOException {
    if (readType == null) {
      throw new IOException("ReadType can not be null.");
    }

    if (!isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    }

    List<Long> blocks = mTachyonFS.getFileStatus(mFileId, "", false).getBlockIds();

    if (blocks.size() == 0) {
      return new EmptyBlockInStream(this, readType);
    } else if (blocks.size() == 1) {
      return BlockInStream.get(this, readType, 0, mUFSConf);
    }

    return new FileInStream(this, readType, mUFSConf);
  }

  /**
   * Get a ClientBlockInfo by the file id and block index
   * 
   * @param fid
   *          the file id
   * @param blockIndex
   *          The index of the block in the file.
   * @return the ClientBlockInfo of the specified block
   * @throws IOException
   */
  public synchronized ClientBlockInfo getClientBlockInfo(int blockIndex) throws IOException {
    return mTachyonFS.getClientBlockInfo(getBlockId(blockIndex));
  }

  /**
   * Returns the local filename for the block if that file exists on the local file system. This is
   * an alpha power-api feature for applications that want short-circuit-read files directly. There
   * is no guarantee that the file still exists after this call returns, as Tachyon may evict blocks
   * from memory at any time.
   *
   * @param blockIndex
   *          The index of the block in the file.
   * @return filename on local file system or null if file not present on local file system.
   * @throws IOException
   */
  public String getLocalFilename(int blockIndex) throws IOException {
    ClientBlockInfo blockInfo = getClientBlockInfo(blockIndex);

    String rootFolder = mTachyonFS.getLocalDataFolder();
    if (rootFolder != null) {
      String localFileName = CommonUtils.concat(rootFolder, blockInfo.getBlockId());
      File file = new File(localFileName);
      if (file.exists()) {
        return localFileName;
      }
    }
    return null;
  }

  /**
   * Return the net address of all the location hosts
   *
   * @return the list of those net address, in String
   * @throws IOException
   */
  public List<String> getLocationHosts() throws IOException {
    List<NetAddress> locations = getClientBlockInfo(0).getLocations();
    List<String> ret = null;
    if (locations != null) {
      ret = new ArrayList<String>(locations.size());
      for (int k = 0; k < locations.size(); k ++) {
        ret.add(locations.get(k).mHost);
      }
    }

    return ret;
  }

  /**
   * Return the number of blocks the file has.
   *
   * @return the number of blocks
   * @throws IOException
   */
  public int getNumberOfBlocks() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", false).getBlockIds().size();
  }

  /**
   * Return the OutStream of this file, use the specified write type. Always return a FileOutStream.
   *
   * @param writeType
   *          the OutStream's write type
   * @return the OutStream
   * @throws IOException
   */
  public OutStream getOutStream(WriteType writeType) throws IOException {
    if (isComplete()) {
      throw new IOException("Overriding after completion not supported.");
    }

    if (writeType == null) {
      throw new IOException("WriteType can not be null.");
    }

    return new FileOutStream(this, writeType, mUFSConf);
  }

  /**
   * Return the path of this file in the Tachyon file system
   *
   * @return the path
   * @throws IOException
   */
  public String getPath() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", false).getPath();
  }

  /**
   * To get the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  public Object getUFSConf() {
    return mUFSConf;
  }

  /**
   * Return the under filesystem path in the under file system of this file
   *
   * @return the under filesystem path
   * @throws IOException
   */
  String getUfsPath() throws IOException {
    ClientFileInfo info = mTachyonFS.getFileStatus(mFileId, "", true);

    if (!info.getUfsPath().isEmpty()) {
      return info.getUfsPath();
    }

    return mTachyonFS.getFileStatus(mFileId, "", false).getUfsPath();
  }

  /**
   * Get the block id by the file id and offset. it will check whether the file and the block exist.
   * 
   * @param fid
   *          the file id
   * @param offset
   *          The offset of the file.
   * @return the block id if exists
   * @throws IOException
   */
  long getBlockIdBasedOnOffset(long offset) throws IOException {
    int index = (int) (offset / mTachyonFS.getFileStatus(mFileId, "", true).getBlockSizeByte());

    return mTachyonFS.getBlockId(mFileId, index);
  }

  @Override
  public int hashCode() {
    return mFileId;
  }

  /**
   * Return whether this file is complete or not
   *
   * @return true if this file is complete, false otherwise
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ClientFileInfo info = mTachyonFS.getFileStatus(mFileId, "", true);

    return info.isComplete ? true : mTachyonFS.getFileStatus(mFileId, "", false).isComplete;
  }

  /**
   * @return true if this is a directory, false otherwise
   * @throws IOException
   */
  public boolean isDirectory() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", true).isFolder;
  }

  /**
   * @return true if this is a file, false otherwise
   * @throws IOException
   */
  public boolean isFile() throws IOException {
    return !isDirectory();
  }

  /**
   * Return whether the file is in memory or not. Note that a file may be partly in memory. This
   * value is true only if the file is fully in memory.
   *
   * @return true if the file is fully in memory, false otherwise
   * @throws IOException
   */
  public boolean isInMemory() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", false).getInMemoryPercentage() == 100;
  }

  /**
   * @return the file size in bytes
   * @throws IOException
   */
  public long length() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", false).getLength();
  }

  /**
   * @return true if this file is pinned, false otherwise
   * @throws IOException
   */
  public boolean needPin() throws IOException {
    return mTachyonFS.getFileStatus(mFileId, "", false).isPinned;
  }

  /**
   * Advanced API.
   *
   * Return a TachyonByteBuffer of the block specified by the blockIndex
   *
   * @param blockIndex
   *          The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException
   */
  public TachyonByteBuffer readByteBuffer(int blockIndex) throws IOException {
    if (!isComplete()) {
      return null;
    }

    TachyonByteBuffer ret = readLocalByteBuffer(blockIndex);
    if (ret == null) {
      // TODO Make it local cache if the OpType is try cache.
      ret = readRemoteByteBuffer(getClientBlockInfo(blockIndex));
    }

    return ret;
  }

  /**
   * Get the the whole block.
   *
   * @param blockIndex
   *          The block index of the current file to read.
   * @return TachyonByteBuffer containing the block.
   * @throws IOException
   */
  TachyonByteBuffer readLocalByteBuffer(int blockIndex) throws IOException {
    return readLocalByteBuffer(blockIndex, 0, -1);
  }

  /**
   * Read local block return a TachyonByteBuffer
   * 
   * @param blockId
   *          The id of the block.
   * @param offset
   *          The start position to read.
   * @param len
   *          The length to read. -1 represents read the whole block.
   * @return <code>TachyonByteBuffer</code> containing the block.
   * @throws IOException
   */
  private TachyonByteBuffer readLocalByteBuffer(int blockIndex, long offset, long len)
      throws IOException {
    if (offset < 0) {
      throw new IOException("Offset can not be negative: " + offset);
    }
    if (len < 0 && len != -1) {
      throw new IOException("Length can not be negative except -1: " + len);
    }

    ClientBlockInfo info = getClientBlockInfo(blockIndex);
    long blockId = info.blockId;

    int blockLockId = mTachyonFS.getBlockLockId();
    if (!mTachyonFS.lockBlock(blockId, blockLockId)) {
      return null;
    }
    String localFileName = getLocalFilename(blockIndex);
    if (localFileName != null) {
      try {
        RandomAccessFile localFile = new RandomAccessFile(localFileName, "r");

        long fileLength = localFile.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
        }
        if (error != null) {
          localFile.close();
          throw new IOException(error);
        }

        if (len == -1) {
          len = fileLength - offset;
        }

        FileChannel localFileChannel = localFile.getChannel();
        ByteBuffer buf = localFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, len);
        localFileChannel.close();
        localFile.close();
        mTachyonFS.accessLocalBlock(blockId);
        return new TachyonByteBuffer(mTachyonFS, buf, blockId, blockLockId);
      } catch (FileNotFoundException e) {
        LOG.info(localFileName + " is not on local disk.");
      } catch (IOException e) {
        LOG.warn("Failed to read local file " + localFileName + " because:", e);
      }
    }

    mTachyonFS.unlockBlock(blockId, blockLockId);
    return null;
  }

  /**
   * Get the the whole block from remote workers.
   *
   * @param blockInfo
   *          The blockInfo of the block to read.
   * @return TachyonByteBuffer containing the block.
   */
  TachyonByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo) {
    ByteBuffer buf = null;

    LOG.info("Try to find and read from remote workers.");
    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("readByteBufferFromRemote() " + blockLocations);

      for (int k = 0; k < blockLocations.size(); k ++) {
        String host = blockLocations.get(k).mHost;
        int port = blockLocations.get(k).mSecondaryPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        final String hostname = InetAddress.getLocalHost().getHostName();
        final String hostaddress = InetAddress.getLocalHost().getHostAddress();
        if (host.equals(hostname) || host.equals(hostaddress)) {
          String localFileName =
              CommonUtils.concat(mTachyonFS.getLocalDataFolder(), blockInfo.blockId);
          LOG.warn("Reading remotely even though request is local; file is " + localFileName);
        }
        LOG.info(host + ":" + port + " current host is " + hostname + " " + hostaddress);

        try {
          buf = retrieveRemoteByteBuffer(new InetSocketAddress(host, port), blockInfo.blockId);
          if (buf != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error(e);
          buf = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote ", e);
    }

    return buf == null ? null : new TachyonByteBuffer(mTachyonFS, buf, blockInfo.blockId, -1);
  }

  // TODO remove this method. do streaming cache. This is not a right API.
  public boolean recache() throws IOException {
    int numberOfBlocks = getNumberOfBlocks();
    if (numberOfBlocks == 0) {
      return true;
    }

    boolean succeed = true;
    for (int k = 0; k < numberOfBlocks; k ++) {
      succeed &= recache(k);
    }

    return succeed;
  }

  /**
   * Re-cache the block into memory
   *
   * @param blockIndex
   *          The block index of the current file.
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  boolean recache(int blockIndex) throws IOException {
    boolean succeed = true;
    String path = getUfsPath();
    UnderFileSystem underFsClient = UnderFileSystem.get(path);

    try {
      InputStream inputStream = underFsClient.open(path);

      long length = getBlockSizeByte();
      long offset = blockIndex * length;
      inputStream.skip(offset);

      byte buffer[] = new byte[USER_CONF.FILE_BUFFER_BYTES * 4];

      BlockOutStream bos = new BlockOutStream(this, WriteType.TRY_CACHE, blockIndex);
      try {
        int limit;
        while (length > 0 && ((limit = inputStream.read(buffer)) >= 0)) {
          if (limit != 0) {
            try {
              if (length >= limit) {
                bos.write(buffer, 0, limit);
                length -= limit;
              } else {
                bos.write(buffer, 0, (int) length);
                length = 0;
              }
            } catch (IOException e) {
              LOG.warn(e);
              succeed = false;
              break;
            }
          }
        }
      } finally {
        if (succeed) {
          bos.close();
        } else {
          bos.cancel();
        }
      }
    } catch (IOException e) {
      LOG.warn(e);
      return false;
    }

    return succeed;
  }

  /**
   * Rename this file
   *
   * @param path
   *          the new name
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public boolean rename(String path) throws IOException {
    return mTachyonFS.rename(mFileId, path);
  }

  private ByteBuffer retrieveRemoteByteBuffer(InetSocketAddress address, long blockId)
      throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    try {
      socketChannel.connect(address);

      LOG.info("Connected to remote machine " + address + " sent");
      DataServerMessage sendMsg = DataServerMessage.createBlockRequestMessage(blockId);
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      LOG.info("Data " + blockId + " to remote machine " + address + " sent");

      DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          break;
        }
      }
      LOG.info("Data " + blockId + " from remote machine " + address + " received");

      if (!recvMsg.isMessageReady()) {
        LOG.info("Data " + blockId + " from remote machine is not ready.");
        return null;
      }

      if (recvMsg.getBlockId() < 0) {
        LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
        return null;
      }

      return recvMsg.getReadOnlyData();
    } finally {
      socketChannel.close();
    }
  }

  /**
   * To set the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs' implementation.
   *
   * @param conf
   *          The configuration object accepted by ufs.
   */
  public void setUFSConf(Object conf) {
    mUFSConf = conf;
  }

  @Override
  public String toString() {
    try {
      return getPath();
    } catch (IOException e) {
      throw new RuntimeException("File does not exist anymore: " + mFileId);
    }
  }
}
