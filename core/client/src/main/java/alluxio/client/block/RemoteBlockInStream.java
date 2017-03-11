/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.client.Locatable;
import alluxio.client.RemoteBlockReader;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Counter;
import com.google.common.io.Closer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a streaming API to read a block in Alluxio. The data will be transferred
 * through an Alluxio worker's dataserver to the client.
 */
@NotThreadSafe
public final class RemoteBlockInStream extends BufferedBlockInStream implements Locatable {
  /** Used to manage closeable resources. */
  private final Closer mCloser;
  /** The address of the worker to read the data from. */
  private final WorkerNetAddress mWorkerNetAddress;
  /** mWorkerNetAddress converted to an InetSocketAddress. */
  private final InetSocketAddress mWorkerInetSocketAddress;
  /** The returned lock id after acquiring the block lock. */
  private final Long mLockId;

  /** Client to communicate with the remote worker. */
  private final BlockWorkerClient mBlockWorkerClient;
  /** The file system context which provides block worker clients. */
  private final FileSystemContext mContext;
  /** {@link RemoteBlockReader} for this instance. */
  private RemoteBlockReader mReader;

  /**
   * Creates a new remote block input stream.
   *
   * @param blockId the block id
   * @param blockSize the block size
   * @param workerNetAddress the worker address
   * @param context the file system context to use for acquiring worker and master clients
   * @param options the instream options
   * @throws IOException if the block is not available on the remote worker
   */
  public RemoteBlockInStream(long blockId, long blockSize, WorkerNetAddress workerNetAddress,
      FileSystemContext context, InStreamOptions options) throws IOException {
    super(blockId, blockSize);
    mWorkerNetAddress = workerNetAddress;
    mWorkerInetSocketAddress =
        new InetSocketAddress(workerNetAddress.getHost(), workerNetAddress.getDataPort());

    mContext = context;
    mCloser = Closer.create();

    try {
      mBlockWorkerClient = mCloser.register(mContext.createBlockWorkerClient(workerNetAddress));
      LockBlockResult result = mBlockWorkerClient.lockBlock(blockId);
      mLockId = result.getLockId();
    } catch (AlluxioException e) {
      mCloser.close();
      throw new IOException(e);
    } catch (IOException e) {
      mCloser.close();
      throw e;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    super.seek(pos);
    Metrics.SEEKS_REMOTE.inc();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    if (mBlockIsRead) {
      Metrics.BLOCKS_READ_REMOTE.inc();
    }
    try {
      mBlockWorkerClient.unlockBlock(mBlockId);
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
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
    Metrics.BYTES_READ_REMOTE.inc(bytes);
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

    if (mReader == null) {
      mReader = mCloser.register(RemoteBlockReader.Factory.create(mContext));
    }

    while (bytesLeft > 0) {
      ByteBuffer data = mReader.readRemoteBlock(mWorkerInetSocketAddress, mBlockId, getPosition(),
          bytesLeft, mLockId, mBlockWorkerClient.getSessionId());
      int bytesRead = data.remaining();
      data.get(b, off, bytesRead);
      bytesLeft -= bytesRead;
    }

    return toRead;
  }

  @Override
  public InetSocketAddress location() {
    return mBlockWorkerClient.getDataServerAddress();
  }

  @Override
  public boolean isLocal() {
    return false;
  }

  /**
   * Class that contains metrics about RemoteBlockInStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_READ_REMOTE =
        MetricsSystem.clientCounter("BlocksReadRemote");
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.clientCounter("BytesReadRemote");
    private static final Counter SEEKS_REMOTE = MetricsSystem.clientCounter("SeeksRemote");

    private Metrics() {} // prevent instantiation
  }
}
