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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientBlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

/**
 * BlockInStream for remote block.
 */
public class RemoteBlockInStream extends BlockInStream {
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
   * The position in the block we are currently at, relative to the block. The position relative to
   * the file would be mBlockInfo.offset + mBlockPos.
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
   * True initially, will be false after a cache miss, meaning no worker had this block in memory.
   * Afterward, all reads will go directly to the under filesystem.
   */
  private boolean mAttemptReadFromWorkers = true;

  /**
   * If we are re-caching the file, we write it to a block out stream as we read it.
   */
  private BlockOutStream mBlockOutStream = null;

  /**
   * The under filesystem configuration that we use to set up the checkpoint input stream
   */
  private Object mUFSConf = null;

  /**
   * The bytes read from remote.
   */
  private long mBytesReadRemote = 0;

  /**
   * The maximum number of tries to read a remote block. Since the stored ClientBlockInfo might not
   * be accurate when executing a remote read, we refresh it and retry reading a certain number of
   * times before giving up.
   */
  private static final int MAX_REMOTE_READ_ATTEMPTS = 2;

  /** A reference to the current reader so we can clear it after reading is finished. */
  private RemoteBlockReader mCurrentReader = null;

  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the file
   * @param tachyonConf the TachyonConf instance for this file output stream.
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex, TachyonConf tachyonConf)
      throws IOException {
    this(file, readType, blockIndex, null, tachyonConf);
  }

  /**
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param blockIndex the index of the block in the file
   * @param ufsConf the under file system configuration
   * @throws IOException
   */
  RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex, Object ufsConf,
      TachyonConf tachyonConf) throws IOException {
    super(file, readType, blockIndex, tachyonConf);

    if (!mFile.isComplete()) {
      throw new IOException("File " + mFile.getPath() + " is not ready to read");
    }

    mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);

    mRecache = readType.isCache();

    mUFSConf = ufsConf;
  }

  /**
   * Only called by {@link getDummyStream}.
   * An alternative to construct a RemoteBlockInstream, bypassing all the checks.
   * The returned RemoteBlockInStream can be used to call {#link #readRemoteByteBuffer}.
   *
   * @param file, any, could be null
   * @param readType, any type
   * @param blockIndex, any index
   * @param ufsConf, any ufs configuration
   * @param tachyonConf, any
   * @param addFlag, add another field so that this constructor differentiates
   */
  private RemoteBlockInStream(TachyonFile file, ReadType readType, int blockIndex, Object ufsConf,
      TachyonConf tachyonConf, boolean addFlag) {
    super(file, readType, blockIndex, tachyonConf);
  }

  /**
   * Return a dummy RemoteBlockInStream object.
   * The object can be used to perform near-stateless read using {@link #readRemoteByteBuffer}.
   * (The only state kept is a handler for the underlying reader, so we can close the reader
   * when we close the dummy stream.)
   *
   * <p>
   * See {@link tachyon.client.TachyonFile#readRemoteByteBuffer(ClientBlockInfo)} for usage.
   *
   * @return a dummy RemoteBlockInStream object.
   */
  public static RemoteBlockInStream getDummyStream() {
    return new RemoteBlockInStream(new TachyonFile(null, -1, null),
        ReadType.NO_CACHE, -1, null, null, true);
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
    if (mRecache && mBlockOutStream != null) {
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
    if (mBytesReadRemote > 0) {
      mTachyonFS.getClientMetrics().incBlocksReadRemote(1);
    }
    closeReader();
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
    // Lazy initialization of the out stream for caching to avoid collisions with other caching
    // attempts that are invalidated later due to seek/skips
    if (bytesLeft > 0 && mBlockOutStream == null && mRecache) {
      try {
        mBlockOutStream = BlockOutStream.get(mFile, WriteType.TRY_CACHE, mBlockIndex, mTachyonConf);
        // We should only cache when we are writing to a local worker
        if (mBlockOutStream instanceof RemoteBlockOutStream) {
          LOG.info("Cannot find a local worker to write to, recache attempt cancelled.");
          cancelRecache();
        }
      } catch (IOException ioe) {
        LOG.warn("Recache attempt failed.", ioe);
        cancelRecache();
      }
    }

    // While we still have bytes to read, make sure the buffer is set to read the byte at mBlockPos.
    // If we fail to set mCurrentBuffer, we stream the rest from the underfs
    while (bytesLeft > 0 && mAttemptReadFromWorkers && updateCurrentBuffer()) {
      int bytesToRead = Math.min(bytesLeft, mCurrentBuffer.remaining());
      mCurrentBuffer.get(b, off, bytesToRead);
      if (mRecache) {
        mBlockOutStream.write(b, off, bytesToRead);
      }
      off += bytesToRead;
      bytesLeft -= bytesToRead;
      mBlockPos += bytesToRead;
    }
    mBytesReadRemote += len - bytesLeft;
    mTachyonFS.getClientMetrics().incBytesReadRemote(len - bytesLeft);

    if (bytesLeft > 0) {
      // Unable to read from worker memory, reading this block from underfs in the future.
      mAttemptReadFromWorkers = false;
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
        mTachyonFS.getClientMetrics().incBytesReadUfs(readBytes);
      }
    }
    return len;
  }

  public ByteBuffer readRemoteByteBuffer(TachyonFS tachyonFS, ClientBlockInfo blockInfo,
      long offset, long len, TachyonConf conf) {
    ByteBuffer buf = null;

    try {
      List<NetAddress> blockLocations = blockInfo.getLocations();
      LOG.info("Block locations:" + blockLocations);
      String localhost = NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, conf);

      for (NetAddress blockLocation : blockLocations) {
        String host = blockLocation.mHost;
        int port = blockLocation.mSecondaryPort;

        // The data is not in remote machine's memory if primary port == -1. We check primary port
        // because if the data is in the under storage, the secondary port (data transfer port)
        // will be set.
        if (blockLocation.mPort == -1) {
          continue;
        }

        if (host.equals(InetAddress.getLocalHost().getHostName())
            || host.equals(InetAddress.getLocalHost().getHostAddress()) || host.equals(localhost)) {
          LOG.warn("Master thinks the local machine has data, but not!"
              + "(or local read is disabled) blockId:{}", blockInfo.blockId);
        }
        LOG.info(host + ":" + port + " current host is " + localhost + " "
            + NetworkAddressUtils.getLocalIpAddress(conf));

        try {
          buf =
              retrieveByteBufferFromRemoteMachine(new InetSocketAddress(host, port),
                  blockInfo.blockId, offset, len, conf);
          if (buf != null) {
            break;
          }
        } catch (IOException e) {
          LOG.error("Fail to retrieve byte buffer for block " + blockInfo.blockId + " from remote "
              + host + ":" + port + " with offset " + offset + " and length " + len, e);
          buf = null;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get read data from remote ", e);
      buf = null;
    }

    return buf;
  }

  private ByteBuffer retrieveByteBufferFromRemoteMachine(InetSocketAddress address,
      long blockId, long offset, long length, TachyonConf conf) throws IOException {
    // always clear the previous reader before assigning it to a new one
    closeReader();
    mCurrentReader = RemoteBlockReader.Factory.createRemoteBlockReader(conf);
    return mCurrentReader.readRemoteBlock(address, blockId, offset, length);
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
      UnderFileSystem underfsClient = UnderFileSystem.get(checkpointPath, mUFSConf, mTachyonConf);
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
   * Makes sure mCurrentBuffer is set to read at mBlockPos. If it is already, we do nothing.
   * Otherwise, we set mBufferStartPos accordingly and try to read the correct range of bytes
   * remotely. If we fail to read remotely, mCurrentBuffer will be null at the end of the function
   *
   * @return true if mCurrentBuffer was successfully set to read at mBlockPos, or false if the
   *         remote read failed.
   * @throws IOException
   */
  private boolean updateCurrentBuffer() throws IOException {
    long bufferSize =
        mTachyonConf.getBytes(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE);
    if (mCurrentBuffer != null && mBufferStartPos <= mBlockPos
        && mBlockPos < Math.min(mBufferStartPos + bufferSize, mBlockInfo.length)) {
      // We move the buffer to read at mBlockPos
      mCurrentBuffer.position((int) (mBlockPos - mBufferStartPos));
      return true;
    }

    // We must read in a new block. By starting at mBlockPos, we ensure that the next byte read will
    // be the one at mBlockPos
    mBufferStartPos = mBlockPos;
    long length = Math.min(bufferSize, mBlockInfo.length - mBufferStartPos);
    LOG.info(String.format("Try to find remote worker and read block %d from %d, with len %d",
        mBlockInfo.blockId, mBufferStartPos, length));

    for (int i = 0; i < MAX_REMOTE_READ_ATTEMPTS; i ++) {
      mCurrentBuffer =
          readRemoteByteBuffer(mTachyonFS, mBlockInfo, mBufferStartPos, length, mTachyonConf);
      if (mCurrentBuffer != null) {
        return true;
      }
      // The read failed, refresh the block info and try again
      mBlockInfo = mFile.getClientBlockInfo(mBlockIndex);
    }
    return false;
  }

  /**
   * Clear the previous reader, release the resource it references.
   */
  private void closeReader() throws IOException {
    if (mCurrentReader != null) {
      try {
        mCurrentReader.close();
      } finally {
        mCurrentReader = null;
      }
    }
  }
}
