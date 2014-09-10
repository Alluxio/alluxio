package tachyon.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.NetworkUtils;
import tachyon.worker.DataServerMessage;

/**
 * BlockInStream for remote block.
 */
public class RemoteBlockInStream extends BlockInStream {
  private static final int BUFFER_SIZE = UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE;
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private ClientBlockInfo mBlockInfo;
  private InputStream mCheckpointInputStream = null;
  private long mReadByte;
  private ByteBuffer mCurrentBuffer = null;
  private long mBufferStartPosition = 0;

  private boolean mRecache = true;
  private BlockOutStream mBlockOutStream = null;

  private Object mUFSConf = null;

  /**
   * @param file
   *          the file the block belongs to
   * @param readType
   *          the InStream's read type
   * @param blockIndex
   *          the index of the block in the file
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    this(file, readType, blockIndex, null);
  }

  /**
   * @param file
   *          the file the block belongs to
   * @param readType
   *          the InStream's read type
   * @param blockIndex
   *          the index of the block in the file
   * @param ufsConf
   *          the under file system configuration
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex, Object ufsConf)
      throws IOException {
    super(file, readType, blockIndex);

    mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);
    mReadByte = 0;
    mBufferStartPosition = 0;

    if (!mFile.isComplete()) {
      throw new IOException("File " + mFile.getPath() + " is not ready to read");
    }

    mRecache = readType.isCache();
    if (mRecache) {
      mBlockOutStream = new BlockOutStream(file, WriteType.TRY_CACHE, blockIndex);
    }

    updateCurrentBuffer();

    mUFSConf = ufsConf;
    if (mCurrentBuffer == null) {
      setupStreamFromUnderFs(mBlockInfo.offset, mUFSConf);

      if (mCheckpointInputStream == null) {
        mTachyonFS.reportLostFile(mFile.mFileId);

        throw new IOException("Can not find the block " + mFile + " " + mBlockIndex);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      if (mRecache) {
        mBlockOutStream.cancel();
      }
      if (mCheckpointInputStream != null) {
        mCheckpointInputStream.close();
      }
    }
    mClosed = true;
  }

  private void doneRecache() throws IOException {
    if (mRecache) {
      mBlockOutStream.close();
    }
  }

  @Override
  public int read() throws IOException {
    mReadByte ++;
    if (mReadByte > mBlockInfo.length) {
      doneRecache();
      return -1;
    }

    if (mCurrentBuffer != null) {
      if (mCurrentBuffer.remaining() == 0) {
        mBufferStartPosition = mReadByte - 1;
        updateCurrentBuffer();
      }
      if (mCurrentBuffer != null) {
        int ret = mCurrentBuffer.get() & 0xFF;
        if (mRecache) {
          mBlockOutStream.write(ret);
        }
        return ret;
      }
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte - 1, mUFSConf);
    }

    int ret = mCheckpointInputStream.read() & 0xFF;
    if (mRecache) {
      mBlockOutStream.write(ret);
    }
    return ret;
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    long ret = mBlockInfo.length - mReadByte;
    if (ret < len) {
      len = (int) ret;
    }

    if (ret == 0) {
      return -1;
    }

    if (mCurrentBuffer != null) {
      if (mCurrentBuffer.remaining() == 0) {
        mBufferStartPosition = mReadByte;
        updateCurrentBuffer();
      }
      if (mCurrentBuffer != null) {
        ret = Math.min(ret, mCurrentBuffer.remaining());
        ret = Math.min(ret, len);
        mCurrentBuffer.get(b, off, (int) ret);
        mReadByte += ret;
        if (mRecache) {
          mBlockOutStream.write(b, off, (int) ret);
          if (mReadByte == mBlockInfo.length) {
            doneRecache();
          }
        }
        return (int) ret;
      }
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte, mUFSConf);
    }

    ret = mCheckpointInputStream.read(b, off, len);

    mReadByte += ret;
    if (mRecache) {
      mBlockOutStream.write(b, off, (int) ret);
      if (mReadByte == mBlockInfo.length) {
        doneRecache();
      }
    }
    return (int) ret;
  }

  private ByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo, long offset, long len) {
    ByteBuffer buf = null;

    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("Block locations:" + blockLocations);

      for (int k = 0; k < blockLocations.size(); k ++) {
        String host = blockLocations.get(k).mHost;
        int port = blockLocations.get(k).mSecondaryPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress())
            || host.equals(NetworkUtils.getLocalHostName())) {
          String localFileName =
              CommonUtils.concat(mTachyonFS.getLocalDataFolder(), blockInfo.blockId);
          LOG.warn("Master thinks the local machine has data " + localFileName + "! But not!");
        }
        LOG.info(host + ":" + port + " current host is "
            + NetworkUtils.getLocalHostName() + " " + NetworkUtils.getLocalIpAddress());

        try {
          buf =
              retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port),
                  blockInfo.blockId, offset, len);
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
      buf = null;
    }

    return buf;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address, long blockId,
      long offset, long length) throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    try {
      socketChannel.connect(address);

      LOG.info("Connected to remote machine " + address + " sent");
      DataServerMessage sendMsg =
          DataServerMessage.createBlockRequestMessage(blockId, offset, length);
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      LOG.info("Data " + blockId + " to remote machine " + address + " sent");

      DataServerMessage recvMsg = DataServerMessage.createBlockResponseMessage(false, blockId);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          LOG.warn("Read nothing");
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

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }
    mRecache = false;
    if (mCurrentBuffer != null) {
      mReadByte = pos;
      if (mBufferStartPosition <= pos && pos < mBufferStartPosition + mCurrentBuffer.limit()) {
        mCurrentBuffer.position((int) (pos - mBufferStartPosition));
      } else {
        mBufferStartPosition = pos;
        updateCurrentBuffer();
      }
    } else {
      if (mCheckpointInputStream != null) {
        mCheckpointInputStream.close();
      }

      setupStreamFromUnderFs(mBlockInfo.offset + pos, mUFSConf);
    }
  }

  private void setupStreamFromUnderFs(long offset, Object conf) throws IOException {
    String checkpointPath = mFile.getUfsPath();
    if (!checkpointPath.equals("")) {
      LOG.info("May stream from underlayer fs: " + checkpointPath);
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath, conf);
      try {
        mCheckpointInputStream = underfsClient.open(checkpointPath);
        while (offset > 0) {
          long skipped = mCheckpointInputStream.skip(offset);
          offset -= skipped;
          if (skipped == 0) {
            throw new IOException("Failed to find the start position " + offset + " for block "
                + mBlockInfo);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to read from checkpoint " + checkpointPath + " for File "
            + mFile.mFileId, e);
        mCheckpointInputStream = null;
      }
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long ret = mBlockInfo.length - mReadByte;
    if (ret > n) {
      ret = n;
    }

    if (mCurrentBuffer != null) {
      if (mCurrentBuffer.remaining() < ret) {
        mBufferStartPosition = mReadByte + ret;
        updateCurrentBuffer();
      }
      if (mCurrentBuffer != null) {
        if (ret > 0) {
          if (mRecache) {
            mBlockOutStream.cancel();
          }
          mRecache = false;
        }
        return (int) ret;
      }
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte, mUFSConf);
    }

    long tmp = mCheckpointInputStream.skip(ret);
    ret = Math.min(ret, tmp);
    mReadByte += ret;

    if (ret > 0) {
      if (mRecache) {
        mBlockOutStream.cancel();
      }
      mRecache = false;
    }
    return ret;
  }

  private void updateCurrentBuffer() throws IOException {
    long length = BUFFER_SIZE;
    if (mBufferStartPosition + length > mBlockInfo.length) {
      length = mBlockInfo.length - mBufferStartPosition;
    }

    LOG.info(String.format("Try to find remote worker and read block %d from %d, with len %d",
        mBlockInfo.blockId, mBufferStartPosition, length));

    mCurrentBuffer = readRemoteByteBuffer(mBlockInfo, mBufferStartPosition, length);

    if (mCurrentBuffer == null) {
      mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);
      mCurrentBuffer = readRemoteByteBuffer(mBlockInfo, mBufferStartPosition, length);
    }
  }
}
