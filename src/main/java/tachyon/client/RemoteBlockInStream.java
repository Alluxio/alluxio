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
import tachyon.DataServerMessage;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;

/**
 * BlockInStream for remote block.
 */
public class RemoteBlockInStream extends BlockInStream {
  private static final int BUFFER_SIZE = UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private ClientBlockInfo mBlockInfo;
  private InputStream mCheckpointInputStream = null;
  private long mReadByte;
  private ByteBuffer mCurrentBuffer = null;
  private long mBufferStartPosition = 0;

  private boolean mRecache = true;
  private BlockOutStream mBlockOutStream = null;

  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    super(file, readType, blockIndex);

    mBlockInfo = TFS.getClientBlockInfo(FILE.FID, BLOCK_INDEX);
    mReadByte = 0;
    mBufferStartPosition = 0;

    if (!FILE.isComplete()) {
      throw new IOException("File " + FILE.getPath() + " is not ready to read");
    }

    mRecache = readType.isCache();
    if (mRecache) {
      mBlockOutStream = new BlockOutStream(file, WriteType.TRY_CACHE, blockIndex);
    }

    updateCurrentBuffer();

    if (mCurrentBuffer == null) {
      setupStreamFromUnderFs(mBlockInfo.offset);

      if (mCheckpointInputStream == null) {
        throw new IOException("Can not find the block " + FILE + " " + BLOCK_INDEX);
      }
    }
  }

  private void setupStreamFromUnderFs(long offset) {
    String checkpointPath = TFS.getCheckpointPath(FILE.FID);
    if (!checkpointPath.equals("")) {
      LOG.info("May stream from underlayer fs: " + checkpointPath);
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath);
      try {
        mCheckpointInputStream = underfsClient.open(checkpointPath);
        while (offset > 0) {
          long skipped = mCheckpointInputStream.skip(offset);
          offset -= skipped;
          if (skipped == 0) {
            throw new IOException("Failed to find the start position " + offset +
                " for block " + mBlockInfo);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to read from checkpoint " + checkpointPath + " for File " + FILE.FID + 
            "\n" + e);
        mCheckpointInputStream = null;
      }
    }
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
      mBlockInfo = TFS.getClientBlockInfo(FILE.FID, BLOCK_INDEX);
      mCurrentBuffer = readRemoteByteBuffer(mBlockInfo, mBufferStartPosition, length);
    }
  }

  private ByteBuffer readRemoteByteBuffer(ClientBlockInfo blockInfo, long offset, long len) {
    ByteBuffer buf = null;

    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("Block locations:" + blockLocations);

      for (int k = 0; k < blockLocations.size(); k ++) {
        String host = blockLocations.get(k).mHost;
        int port = blockLocations.get(k).mPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        if (host.equals(InetAddress.getLocalHost().getHostName()) 
            || host.equals(InetAddress.getLocalHost().getHostAddress())) {
          String localFileName = TFS.getRootFolder() + "/" + blockInfo.blockId;
          LOG.warn("Master thinks the local machine has data " + localFileName + "! But not!");
        } 
        LOG.info(host + ":" + (port + 1) +
            " current host is " + InetAddress.getLocalHost().getHostName() + " " +
            InetAddress.getLocalHost().getHostAddress());

        try {
          buf = retrieveByteBufferFromRemoteMachine(
              new InetSocketAddress(host, port + 1), blockInfo.blockId, offset, len);
          if (buf != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
          buf = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote " + e.getMessage());
      buf = null;
    }

    return buf;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address, 
      long blockId, long offset, long length) throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.connect(address);

    LOG.info("Connected to remote machine " + address + " sent");
    DataServerMessage sendMsg =
        DataServerMessage.createBlockRequestMessage(blockId, offset, length);
    while (!sendMsg.finishSending()) {
      sendMsg.send(socketChannel);
    }

    LOG.info("Data " + blockId + " to remote machine " + address + " sent");

    DataServerMessage recvMsg =
        DataServerMessage.createBlockResponseMessage(false, blockId);
    while (!recvMsg.isMessageReady()) {
      int numRead = recvMsg.recv(socketChannel);
      if (numRead == -1) {
        LOG.warn("Read nothing");
      }
    }
    LOG.info("Data " + blockId + " from remote machine " + address + " received");

    socketChannel.close();

    if (!recvMsg.isMessageReady()) {
      LOG.info("Data " + blockId + " from remote machine is not ready.");
      return null;
    }

    if (recvMsg.getBlockId() < 0) {
      LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
      return null;
    }

    return recvMsg.getReadOnlyData();
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
        int ret = mCurrentBuffer.get();
        if (mRecache) {
          mBlockOutStream.write(ret);
        }
        return ret;
      }
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte - 1);
    }

    int ret = mCheckpointInputStream.read();
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
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte);
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
      setupStreamFromUnderFs(mBlockInfo.offset + mReadByte);
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
}
