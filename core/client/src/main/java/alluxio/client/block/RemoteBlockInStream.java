/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.client.ClientContext;
import alluxio.client.RemoteBlockReader;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides a streaming API to read a block in Alluxio. The data will be transferred
 * through an Alluxio worker's dataserver to the client.
 */
@NotThreadSafe
public final class RemoteBlockInStream extends BufferedBlockInStream {
  /** The address of the worker to read the data from. */
  private final WorkerNetAddress mWorkerNetAddress;
  /** mWorkerNetAddress converted to an InetSocketAddress. */
  private final InetSocketAddress mWorkerInetSocketAddress;
  /** The returned lock id after acquiring the block lock. */
  private final Long mLockId;

  /** Client to communicate with the remote worker. */
  private final BlockWorkerClient mBlockWorkerClient;
  /** The block store context which provides block worker clients. */
  private final BlockStoreContext mContext;
  private final ClientMetrics mMetrics;

  /**
   * Creates a new remote block input stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker address
   * @throws IOException if the block is not available on the remote worker
   */
  public RemoteBlockInStream(long blockId, long blockSize, WorkerNetAddress workerNetAddress)
      throws IOException {
    super(blockId, blockSize);
    mWorkerNetAddress = workerNetAddress;
    mWorkerInetSocketAddress =
        new InetSocketAddress(workerNetAddress.getHost(), workerNetAddress.getDataPort());

    mContext = BlockStoreContext.INSTANCE;
    mBlockWorkerClient = mContext.acquireWorkerClient(workerNetAddress);

    try {
      mLockId = mBlockWorkerClient.lockBlock(blockId).getLockId();
      if (mLockId == null) {
        throw new IOException(ExceptionMessage.BLOCK_UNAVAILABLE.getMessage(blockId));
      }
      mMetrics = mBlockWorkerClient.getClientMetrics();
    } catch (IOException e) {
      mContext.releaseWorkerClient(mBlockWorkerClient);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    // TODO(calvin): Perhaps verify that something was read from this stream
    mMetrics.incBlocksReadRemote(1);

    try {
      mBlockWorkerClient.unlockBlock(mBlockId);
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseWorkerClient(mBlockWorkerClient);
    }
    mClosed = true;
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    mBuffer.clear();
    int bytesRead = readFromRemote(mBuffer.array(), 0, len);
    mBuffer.limit(bytesRead);
  }

  @Override
  protected int directRead(byte[] b, int off, int len) throws IOException {
    return readFromRemote(b, off, len);
  }

  /**
   * Increments the number of bytes read metric.
   *
   * @param bytes number of bytes to record as read
   */
  @Override
  protected void incrementBytesReadMetric(int bytes) {
    mMetrics.incBytesReadRemote(bytes);
  }

  /**
   * @return the {@link WorkerNetAddress} from which this RemoteBlockInStream is reading
   */
  public WorkerNetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  /**
   * Reads a portion of the block from the remote worker.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array
   * @return the number of bytes successfully read
   * @throws IOException if an error occurs reading the data
   */
  private int readFromRemote(byte[] b, int off, int len) throws IOException {
    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block.
    int toRead = (int) Math.min(len, remaining());
    int bytesLeft = toRead;
    while (bytesLeft > 0) {
      // TODO(calvin): Fix needing to recreate reader each time.
      RemoteBlockReader reader =
          RemoteBlockReader.Factory.create(ClientContext.getConf());
      try {
        ByteBuffer data = reader.readRemoteBlock(mWorkerInetSocketAddress, mBlockId, getPosition(),
            bytesLeft, mLockId, mBlockWorkerClient.getSessionId());
        int bytesRead = data.remaining();
        data.get(b, off, bytesRead);
        bytesLeft -= bytesRead;
      } finally {
        reader.close();
      }
    }

    return toRead;
  }
}
