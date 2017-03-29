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
import alluxio.PropertyKey;
import alluxio.client.Locatable;
import alluxio.client.UnderFileSystemBlockReader;
import alluxio.client.block.options.LockBlockOptions;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.AlluxioException;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.LockBlockResult;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a streaming API to read a UFS block from an Alluxio worker. The data will
 * be transferred through an Alluxio worker's dataserver to the client.
 */
@NotThreadSafe
public final class UnderFileSystemBlockInStream extends BufferedBlockInStream implements Locatable {
  /** Used to manage closeable resources. */
  private final Closer mCloser;
  /** If set, the block will not be cached into Alluxio because of this read. */
  private final boolean mNoCache;
  /** Whether we are reading from a local Alluxio worker or a remote one. */
  private final boolean mLocal;

  /** Client to communicate with the remote worker. */
  private final BlockWorkerClient mBlockWorkerClient;
  /** The file system context which provides block worker clients. */
  private final FileSystemContext mContext;
  /** {@link UnderFileSystemBlockReader} for this instance. */
  private UnderFileSystemBlockReader mReader;

  /**
   * Creates an instance of {@link UnderFileSystemBlockInStream}.
   * This method keeps polling the block worker until the block is cached to Alluxio or
   * it successfully acquires a UFS read token with a timeout.
   * (1) If the block is cached to Alluxio after polling, it returns {@link BufferedBlockInStream}
   *     to read the block from Alluxio storage.
   * (2) If a UFS read token is acquired after polling, it returns
   *     {@link UnderFileSystemBlockInStream} to read the block from an Alluxio worker that reads
   *     the block from UFS.
   * (3) If the polling times out, an {@link IOException} with cause
   *     {@link alluxio.exception.UfsBlockAccessTokenUnavailableException} is thrown.
   *
   * @param context the file system context
   * @param ufsPath the UFS path
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockStart the start position of the block in the UFS file
   * @param workerNetAddress the worker network address
   * @param options the in stream options
   * @return the {@link UnderFileSystemBlockInStream} instance or the {@link BufferedBlockOutStream}
   *         that reads from Alluxio directly
   * @throws IOException if it fails to create {@link UnderFileSystemBlockInStream}
   */
  public static BufferedBlockInStream create(FileSystemContext context, String ufsPath,
      long blockId, long blockSize, long blockStart, WorkerNetAddress workerNetAddress,
      InStreamOptions options) throws IOException {
    Closer closer = Closer.create();
    try {
      BlockWorkerClient blockWorkerClient =
          closer.register(context.createBlockWorkerClient(workerNetAddress));
      LockBlockOptions lockBlockOptions =
          LockBlockOptions.defaults().setUfsPath(ufsPath).setOffset(blockStart)
              .setBlockSize(blockSize).setMaxUfsReadConcurrency(options.getMaxUfsReadConcurrency());
      LockBlockResult result =
          closer.register(blockWorkerClient.lockUfsBlock(blockId, lockBlockOptions)).getResult();
      if (result.getLockBlockStatus().blockInAlluxio()) {
        boolean local = blockWorkerClient.getDataServerAddress().getHostName()
            .equals(NetworkAddressUtils.getClientHostName());
        if (local && Configuration.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED)) {
          LocalFileBlockReader reader =
              closer.register(new LocalFileBlockReader(result.getBlockPath()));
          return LocalBlockInStream
              .createWithLockedBlock(blockWorkerClient, blockId, blockSize, reader, closer,
                  options);
        } else {
          return RemoteBlockInStream
              .createWithLockedBlock(context, blockWorkerClient, blockId, blockSize,
                  result.getLockId(), closer, options);
        }
      }
      Preconditions.checkState(result.getLockBlockStatus().ufsTokenAcquired());
      return new UnderFileSystemBlockInStream(context, blockId, blockSize, blockWorkerClient,
          closer, options);
    } catch (AlluxioException | IOException e) {
      CommonUtils.closeQuitely(closer);
      throw CommonUtils.castToIOException(e);
    }
  }

  /**
   * Creates a new UFS block input stream. This requires the block to be locked beforehand.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size
   * @param blockWorkerClient the block worker client
   * @param closer the closer registered with closable resources open so far
   * @param options the instream options
   */
  private UnderFileSystemBlockInStream(FileSystemContext context, long blockId, long blockSize,
      BlockWorkerClient blockWorkerClient, Closer closer, InStreamOptions options) {
    super(blockId, blockSize);

    mContext = context;
    mBlockWorkerClient = blockWorkerClient;
    mCloser = closer;
    mNoCache = !options.getAlluxioStorageType().isStore();
    mLocal = blockWorkerClient.getDataServerAddress().getHostName()
        .equals(NetworkAddressUtils.getClientHostName());
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
    mClosed = true;
    mCloser.close();
  }

  @Override
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
    int bytesRead = readFromUnderFileSystem(mBuffer.array(), 0, len);
    mBuffer.limit(bytesRead);
  }

  @Override
  protected int directRead(byte[] b, int off, int len) throws IOException {
    return readFromUnderFileSystem(b, off, len);
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
   * Reads a portion of the UFS block from the Alluxio worker.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array
   * @return the number of bytes successfully read
   * @throws IOException if an error occurs reading the data
   */
  private int readFromUnderFileSystem(byte[] b, int off, int len) throws IOException {
    // We read at most len bytes, but if mPos + len exceeds the length of the block, we only
    // read up to the end of the block.
    int toRead = (int) Math.min(len, remaining());
    int bytesLeft = toRead;

    if (mReader == null) {
      mReader = mCloser.register(UnderFileSystemBlockReader.Factory.create(mContext));
    }

    while (bytesLeft > 0) {
      ByteBuffer data = mReader
          .read(mBlockWorkerClient.getDataServerAddress(), mBlockId, getPosition(), bytesLeft,
              mBlockWorkerClient.getSessionId(), mNoCache);
      int bytesRead = data.remaining();
      data.get(b, off, bytesRead);
      off += bytesRead;
      bytesLeft -= bytesRead;
    }

    return toRead;
  }

  /**
   * Class that contains metrics about UnderFileSystemBlockInStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BLOCKS_READ_UFS = MetricsSystem.clientCounter("BlocksReadUFS");
    private static final Counter BYTES_READ_UFS = MetricsSystem.clientCounter("BytesReadUFS");
    private static final Counter SEEKS_UFS = MetricsSystem.clientCounter("SeeksUFS");

    private Metrics() {} // prevent instantiation
  }
}
