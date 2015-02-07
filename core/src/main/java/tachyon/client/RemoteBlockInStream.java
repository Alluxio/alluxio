/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.InputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.conf.UserConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.NetworkUtils;
import tachyon.worker.nio.DataServerMessage;

/**
 * BlockInStream for remote block.
 */
public class RemoteBlockInStream extends BlockInStream {
  /** The number of bytes to read remotely every time we need to do a remote read */
  private static final int BUFFER_SIZE = UserConf.get().REMOTE_READ_BUFFER_SIZE_BYTE;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The block info of the block we are reading */
  private ClientBlockInfo mBlockInfo;
  /**
   * An input stream for the checkpointed copy of the block. If we are ever unable to read part of
   * the block from the workers, we use this checkpoint stream
   */
  private InputStream mCheckpointInputStream = null;

  /**
   * The position in the checkpointed file that the open input stream is on, relative to the block.
   * That is, if the checkpoint input stream is set to read the 0th byte of the current block,
   * mCheckpointPos will be 0, even if we are n blocks into the input stream, so the actual file
   * position may be something else.
   */
  private long mCheckpointPos = -1;

  /**
   * The position in the block we are currently at, relative to the block. The
   * position relative to the file would be mBlockInfo.offset + mBlockPos.
   */
  private long mBlockPos = 0;

  /**
   * A byte buffer for the current chunk of the block we are reading from
   */
  private ByteBuffer mCurrentBuffer = null;

  /** We keep track of the position relative to the block that the current buffer starts at. */
  private long mBufferStartPos;

  /**
   * true if we are re-caching the file. The re-caching gets canceled if we do anything other than a
   * straight read through the file. That means, any skipping or seeking around will cancel the
   * re-cache.
   */
  private boolean mRecache;

  /**
   * If we are re-caching the file, we write it to a block out stream as we read it.
   */
  private BlockOutStream mBlockOutStream = null;

  /**
   * The under filesystem configuration that we use to set up the checkpoint input stream
   */
  private Object mUFSConf = null;

  /**
   * The maximum number of tries to read a remote block. Since the stored ClientBlockInfo might not
   * be accurate when executing a remote read, we refresh it and retry reading a certain number of
   * times before giving up.
   */
  private static final int MAX_REMOTE_READ_ATTEMPTS = 2;

  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the file
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex) throws IOException {
    this(file, readType, blockIndex, null);
  }

  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the file
   * @param ufsConf the under file system configuration
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex, Object ufsConf)
      throws IOException {
    super(file, readType, blockIndex);

    if (!mFile.isComplete()) {
      throw new IOException("File " + mFile.getPath() + " is not ready to read");
    }

    mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);

    mRecache = readType.isCache();
    if (mRecache) {
      mBlockOutStream = new BlockOutStream(file, WriteType.TRY_CACHE, blockIndex);
    }

    mUFSConf = ufsConf;
  }

  /**
   * Cancels the re-caching attempt
   * 
   * @throws IOException
   */
  private void cancelRecache() throws IOException {
    if (mRecache) {
      mRecache = false;
      if (mBlockOutStream != null) {
        mBlockOutStream.cancel();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    if (mRecache) {
      // We only finish re-caching if we've gotten to the end of the file
      if (mBlockPos == mBlockInfo.length) {
        mBlockOutStream.close();
      } else {
        mBlockOutStream.cancel();
      }
    }
    if (mCheckpointInputStream != null) {
      mCheckpointInputStream.close();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    byte[] b = new byte[1];
    if (read(b) == -1) {
      return -1;
    }
    return (int) b[0] & 0xFF;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    } else if (mBlockPos == mBlockInfo.length) {
      return -1;
    }

    // We read at most len bytes, but if mBlockPos + len exceeds the length of the file, we only
    // read up to the end of the file
    len = (int) Math.min(len, mBlockInfo.length - mBlockPos);
    int bytesLeft = len;
    // While we still have bytes to read, make sure the buffer is set to read the byte at mBlockPos.
    // If we fail to set mCurrentBuffer, we stream the rest from the underfs
    while (bytesLeft > 0 && updateCurrentBuffer()) {
      int bytesToRead = (int) Math.min(bytesLeft, mCurrentBuffer.remaining());
      mCurrentBuffer.get(b, off, bytesToRead);
      if (mRecache) {
        mBlockOutStream.write(b, off, bytesToRead);
      }
      off += bytesToRead;
      bytesLeft -= bytesToRead;
      mBlockPos += bytesToRead;
    }
    if (bytesLeft > 0) {
      // We failed to read everything from mCurrentBuffer, so we need to stream the rest from the
      // underfs
      if (!setupStreamFromUnderFs()) {
        LOG.error("Failed to read at position " + mBlockPos + " in block "
            + mBlockInfo.getBlockId() + " from workers or underfs");
        // Return the number of bytes we managed to read
        return len - bytesLeft;
      }
      while (bytesLeft > 0) {
        int readBytes = mCheckpointInputStream.read(b, off, bytesLeft);
        if (readBytes <= 0) {
          LOG.error("Checkpoint stream read 0 bytes, which shouldn't ever happen");
          return len - bytesLeft;
        }
        if (mRecache) {
          mBlockOutStream.write(b, off, readBytes);
        }
        off += readBytes;
        bytesLeft -= readBytes;
        mBlockPos += readBytes;
        mCheckpointPos += readBytes;
      }
    }
    return len;
  }

  public static ByteBuffer readRemoteByteBuffer(TachyonFS tachyonFS, ClientBlockInfo blockInfo,
      long offset, long len) {
    ByteBuffer buf = null;

    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("Block locations:" + blockLocations);

      for (NetAddress blockLocation : blockLocations) {
        String host = blockLocation.mHost;
        int port = blockLocation.mSecondaryPort;

        // The data is not in remote machine's memory if port == -1.
        if (port == -1) {
          continue;
        }
        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress())
            || host.equals(NetworkUtils.getLocalHostName())) {
          LOG.warn("Master thinks the local machine has data, But not! blockId:{}",
              blockInfo.blockId);
        }
        LOG.info(host + ":" + port + " current host is " + NetworkUtils.getLocalHostName() + " "
            + NetworkUtils.getLocalIpAddress());

        try {
          buf =
              retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port),
                  blockInfo.blockId, offset, len);
          if (buf != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error("Fail to retrieve byte buffer for block " + blockInfo.blockId
              + " from remote " + host + ":" + port + " with offset " + offset + " and length "
              + len, e);
          buf = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote ", e);
      buf = null;
    }

    return buf;
  }

  private static ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address,
      long blockId, long offset, long length) throws IOException {
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

      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, blockId, null);
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
      throw new IOException("Seek position is negative: " + pos);
    } else if (pos > mBlockInfo.length) {
      throw new IOException("Seek position is past block size: " + pos + ", Block Size = "
          + mBlockInfo.length);
    } else if (pos == mBlockPos) {
      // There's nothing to do
      return;
    }
    // Since we're not doing a straight read-through, we've invalidated our re-caching attempt.
    cancelRecache();
    mBlockPos = pos;
  }

  /**
   * Sets up the underfs stream to read at mBlockPos
   * 
   * @return true if the input stream is set to read at mBlockPos, false otherwise
   **/
  private boolean setupStreamFromUnderFs() throws IOException {
    if (mCheckpointInputStream == null || mBlockPos < mCheckpointPos) {
      // We need to open the stream first, or reopen it if we went past our current block pos (which
      // can happen if we seek backwards
      String checkpointPath = mFile.getUfsPath();
      LOG.info("Opening stream from underlayer fs: " + checkpointPath);
      if (checkpointPath.equals("")) {
        return false;
      }
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath, mUFSConf);
      mCheckpointInputStream = underfsClient.open(checkpointPath);
      // We skip to the offset of the block in the file, so we're at the beginning of the block.
      if (mCheckpointInputStream.skip(mBlockInfo.offset) != mBlockInfo.offset) {
        throw new IOException("Failed to skip to the block offset " + mBlockInfo.offset
            + " in the checkpoint file");
      }
      mCheckpointPos = 0;
    }
    // We need to skip to mBlockPos
    while (mCheckpointPos < mBlockPos) {
      long skipped = mCheckpointInputStream.skip(mBlockPos - mCheckpointPos);
      if (skipped <= 0) {
        throw new IOException("Failed to skip to the position " + mBlockPos + " for block "
            + mBlockInfo);
      }
      mCheckpointPos += skipped;
    }
    return true;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    // Since we're not doing a straight read-through, we've invalidated our re-caching attempt.
    cancelRecache();
    long skipped = Math.min(n, mBlockInfo.length - mBlockPos);
    mBlockPos += skipped;
    return skipped;
  }

  /**
   * Makes sure mCurrentBuffer is set to read at mBlockPos. If it is already, we do
   * nothing. Otherwise, we set mBufferStartPos accordingly and try to read the correct range of
   * bytes remotely. If we fail to read remotely, mCurrentBuffer will be null at the end of the
   * function
   * 
   * @return true if mCurrentBuffer was successfully set to read at mBlockPos, or false if the
   *         remote read failed.
   * @throws IOException
   */
  private boolean updateCurrentBuffer() throws IOException {
    if (mCurrentBuffer != null && mBufferStartPos <= mBlockPos
        && mBlockPos < Math.min(mBufferStartPos + BUFFER_SIZE, mBlockInfo.length)) {
      // We move the buffer to read at mBlockPos
      mCurrentBuffer.position((int) (mBlockPos - mBufferStartPos));
      return true;
    }

    // We must read in a new block. By starting at mBlockPos, we ensure that the next byte read will
    // be the one at mBlockPos
    mBufferStartPos = mBlockPos;
    long length = Math.min(BUFFER_SIZE, mBlockInfo.length - mBufferStartPos);
    LOG.info("Try to find remote worker and read block {} from {}, with len {}",
        mBlockInfo.blockId, mBufferStartPos, length);

    for (int i = 0; i < MAX_REMOTE_READ_ATTEMPTS; i ++) {
      mCurrentBuffer = readRemoteByteBuffer(mTachyonFS, mBlockInfo, mBufferStartPos, length);
      if (mCurrentBuffer != null) {
        return true;
      }
      // The read failed, refresh the block info and try again
      mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);
    }
    return false;
  }
}
