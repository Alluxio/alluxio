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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.Locatable;
import alluxio.client.RemoteBlockReader;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
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
public final class UfsBlockInStream extends BufferedBlockInStream implements Locatable {
  private static final long UFS_BLOCK_OPEN_RETRY_INTERVAL_MS = Constants.SECOND_MS;

  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final boolean mNoCache;
  private final boolean mLocal;

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
   * @param context the file system context to use for acquiring worker and master clients
   * @param options the instream options
   * @throws IOException if the block is not available on the remote worker
   */
  private UfsBlockInStream(FileSystemContext context, long blockId, long blockSize,
      BlockWorkerClient blockWorkerClient, InStreamOptions options)
      throws IOException {
    super(blockId, blockSize);

    mContext = context;
    mCloser = Closer.create();
    mCloser.register(blockWorkerClient);
    mBlockWorkerClient = blockWorkerClient;
    mNoCache = !options.getAlluxioStorageType().isStore();
    mLocal = blockWorkerClient.getDataServerAddress().getHostName()
        .equals(NetworkAddressUtils.getLocalHostName());
  }

  public static UfsBlockInStream createUfsBlockInStream(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, WorkerNetAddress workerNetAddress,
      InStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockResult result;
      LockBlockOptions lockBlockOptions =
          LockBlockOptions.defaults().setUfsPath(ufsPath).setOffset(blockStart)
              .setBlockSize(blockSize).setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency());

      long timeout = System.currentTimeMillis() + Configuration
          .getLong(PropertyKey.USER_UFS_BLOCK_OPEN_TIMEOUT_MS);
      while (true) {
        try {
          result = blockWorkerClient.lockBlock(blockId, lockBlockOptions);
          break;
        } catch (UfsBlockAccessTokenUnavailableException e) {
          if (System.currentTimeMillis() >= timeout) {
            throw e;
          }
          CommonUtils.sleepMs(UFS_BLOCK_OPEN_RETRY_INTERVAL_MS);
        }
      }
      if (result.getLockId() >= 0) {
        // The block exists in the Alluxio now.
        blockWorkerClient.unlockBlock(blockId);
        closer.close();
        return null;
      }
      return new UfsBlockInStream(context, blockId, blockSize, blockWorkerClient, options);
    } catch (AlluxioException e) {
      closer.close();
      throw new IOException(e);
    } catch (IOException e) {
      closer.close();
      throw e;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    super.seek(pos);
    Metrics.SEEKS_UFS.inc();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    if (mBlockIsRead) {
      Metrics.BLOCKS_READ_UFS.inc();
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

  public InetSocketAddress location() {
    return mBlockWorkerClient.getDataServerAddress();
  }

  @Override
  public boolean isLocal() {
    return mLocal;
  }

  @Override
  protected void bufferedRead(int len) throws IOException {
    mBuffer.clear();
    int bytesRead = readFromUfs(mBuffer.array(), 0, len);
    mBuffer.limit(bytesRead);
  }

  @Override
  protected int directRead(byte[] b, int off, int len) throws IOException {
    return readFromUfs(b, off, len);
  }

  /**
   * Increments the number of bytes read metric.
   *
   * @param bytes number of bytes to record as read
   */
  @Override
  protected void incrementBytesReadMetric(int bytes) {
    Metrics.BYTES_READ_UFS.inc(bytes);
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
  private int readFromUfs(byte[] b, int off, int len) throws IOException {
    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block.
    int toRead = (int) Math.min(len, remaining());
    int bytesLeft = toRead;

    if (mReader == null) {
      mReader = mCloser.register(RemoteBlockReader.Factory.create(mContext));
    }

    while (bytesLeft > 0) {
      ByteBuffer data = mReader
          .readUfsBlock(mBlockWorkerClient.getDataServerAddress(), mBlockId, getPosition(),
              bytesLeft, mBlockWorkerClient.getSessionId(), mNoCache);
      int bytesRead = data.remaining();
      data.get(b, off, bytesRead);
      bytesLeft -= bytesRead;
    }

    return toRead;
  }

  /**
   * Class that contains metrics about UfsBlockInStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_READ_UFS =
        MetricsSystem.clientCounter("BlocksReadUFS");
    private static final Counter BYTES_READ_UFS = MetricsSystem.clientCounter("BytesReadUFS");
    private static final Counter SEEKS_UFS = MetricsSystem.clientCounter("SeeksUFS");

    private Metrics() {} // prevent instantiation
  }
}
