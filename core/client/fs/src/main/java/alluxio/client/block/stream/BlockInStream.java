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

package alluxio.client.block.stream;

import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.ReadRequest;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link InputStream} implementation that is based on {@link DataReader}s to
 * stream data chunk by chunk.
 */
@NotThreadSafe
public class BlockInStream extends InputStream implements BoundedStream, Seekable,
    PositionedReadable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockInStream.class);

  /** the source tracking where the block is from. */
  public enum BlockInStreamSource {
    PROCESS_LOCAL, // The block is from a worker in the same process
    NODE_LOCAL, // The block is from a separate worker process on the same node
    REMOTE, // The block is from a remote worker
    UFS // The block is in UFS
  }

  private final WorkerNetAddress mAddress;
  private final BlockInStreamSource mInStreamSource;
  /** The id of the block or UFS file to which this instream provides access. */
  private final long mId;
  /** The size in bytes of the block. */
  private final long mLength;
  private final byte[] mSingleByte = new byte[1];

  /** Current position of the stream, relative to the start of the block. */
  private long mPos = 0;
  /** The current data chunk. */
  private DataBuffer mCurrentChunk;

  private DataReader mDataReader;
  private final DataReader.Factory mDataReaderFactory;

  private boolean mClosed = false;
  private boolean mEOF = false;

  /**
   * Creates a {@link BlockInStream}.
   *
   * One of several read behaviors:
   *
   * 1. Domain socket - if the data source is the local worker and the local worker has a domain
   * socket server
   * 2. Short-Circuit - if the data source is the local worker
   * 3. Local Loopback Read - if the data source is the local worker and short circuit is disabled
   * 4. Read from remote worker - if the data source is a remote worker
   * 5. UFS Read from worker - if the data source is UFS, read from the UFS policy's designated
   * worker (ufs -> local or remote worker -> client)
   *
   * @param context the file system context
   * @param info the block info
   * @param dataSource the Alluxio worker which should read the data
   * @param dataSourceType the source location of the block
   * @param options the instream options
   * @return the {@link BlockInStream} object
   */
  public static BlockInStream create(FileSystemContext context, BlockInfo info,
      WorkerNetAddress dataSource, BlockInStreamSource dataSourceType, InStreamOptions options)
      throws IOException {
    long blockId = info.getBlockId();
    long blockSize = info.getLength();

    if (dataSourceType == BlockInStreamSource.PROCESS_LOCAL) {
      // Interaction between the current client and the worker it embedded to should
      // go through worker internal communication directly without RPC involves
      LOG.debug("Creating worker process local input stream for block {} @ {}",
          blockId, dataSource);
      return createProcessLocalBlockInStream(context, dataSource, blockId, blockSize, options);
    }

    AlluxioConfiguration alluxioConf = context.getClusterConf();
    boolean shortCircuit = alluxioConf.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED);
    boolean shortCircuitPreferred =
        alluxioConf.getBoolean(PropertyKey.USER_SHORT_CIRCUIT_PREFERRED);
    boolean sourceSupportsDomainSocket = NettyUtils.isDomainSocketSupported(dataSource);
    boolean sourceIsLocal = dataSourceType == BlockInStreamSource.NODE_LOCAL;

    // Short circuit is enabled when
    // 1. data source is local node
    // 2. alluxio.user.short.circuit.enabled is true
    // 3. the worker's domain socket is not configured
    //      OR alluxio.user.short.circuit.preferred is true
    if (sourceIsLocal && shortCircuit && (shortCircuitPreferred || !sourceSupportsDomainSocket)) {
      LOG.debug("Creating short circuit input stream for block {} @ {}", blockId, dataSource);
      try {
        return createLocalBlockInStream(context, dataSource, blockId, blockSize, options);
      } catch (NotFoundException e) {
        // Failed to do short circuit read because the block is not available in Alluxio.
        // We will try to read via gRPC. So this exception is ignored.
        LOG.warn("Failed to create short circuit input stream for block {} @ {}. Falling back to "
            + "network transfer", blockId, dataSource);
      }
    }

    // gRPC
    LOG.debug("Creating gRPC input stream for block {} @ {} from client {} reading through {} ("
        + "data locates in the local worker {}, shortCircuitEnabled {}, "
        + "shortCircuitPreferred {}, sourceSupportDomainSocket {})",
        blockId, dataSource, NetworkAddressUtils.getClientHostName(alluxioConf), dataSource,
        sourceIsLocal, shortCircuit, shortCircuitPreferred, sourceSupportsDomainSocket);
    return createGrpcBlockInStream(context, dataSource, dataSourceType, blockId,
        blockSize, options);
  }

  /**
   * Creates a {@link BlockInStream} to read from the worker process-local to this client
   * directly without RPC involves, if the block does not exist in this worker, will read from
   * the UFS storage via this worker.
   *
   * @param context the file system context
   * @param address the network address of the gRPC data server to read from
   * @param blockId the block ID
   * @param length the block length
   * @param options the in stream options
   * @return the {@link BlockInStream} created
   */
  private static BlockInStream createProcessLocalBlockInStream(FileSystemContext context,
      WorkerNetAddress address, long blockId, long length, InStreamOptions options) {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(
        PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES);
    return new BlockInStream(new BlockWorkerDataReader.Factory(
        context.getProcessLocalWorker(), blockId, chunkSize, options),
        conf, address, BlockInStreamSource.PROCESS_LOCAL, blockId, length);
  }

  /**
   * Creates a {@link BlockInStream} to read from a local file.
   *
   * @param context the file system context
   * @param address the network address of the gRPC data server to read from
   * @param blockId the block ID
   * @param length the block length
   * @param options the in stream options
   * @return the {@link BlockInStream} created
   */
  private static BlockInStream createLocalBlockInStream(FileSystemContext context,
      WorkerNetAddress address, long blockId, long length, InStreamOptions options)
      throws IOException {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(
        PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES);
    return new BlockInStream(
        new LocalFileDataReader.Factory(context, address, blockId, chunkSize, options),
        conf, address, BlockInStreamSource.NODE_LOCAL, blockId, length);
  }

  /**
   * Creates a {@link BlockInStream} to read from a gRPC data server.
   *
   * @param context the file system context
   * @param address the address of the gRPC data server
   * @param blockSource the source location of the block
   * @param blockSize the block size
   * @param blockId the block id
   * @return the {@link BlockInStream} created
   */
  private static BlockInStream createGrpcBlockInStream(FileSystemContext context,
      WorkerNetAddress address, BlockInStreamSource blockSource,
      long blockId, long blockSize, InStreamOptions options) {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(
        PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
    // Construct the partial read request
    ReadRequest.Builder builder = ReadRequest.newBuilder()
        .setBlockId(blockId)
        .setPromote(ReadType.fromProto(options.getOptions().getReadType()).isPromote())
        .setOpenUfsBlockOptions(options.getOpenUfsBlockOptions(blockId)) // Add UFS fallback options
        .setPositionShort(options.getPositionShort())
        .setChunkSize(chunkSize);
    DataReader.Factory factory;
    if (context.getClusterConf().getBoolean(PropertyKey.FUSE_SHARED_CACHING_READER_ENABLED)
        && blockSize > chunkSize * 4) {
      // Heuristic to resolve issues/12146, guarded by alluxio.fuse.shared.caching.reader.enabled
      // GrpcDataReader instances are shared across FileInStreams to mitigate seek cost
      factory = new SharedGrpcDataReader.Factory(context, address, builder, blockSize);
    } else {
      factory = new GrpcDataReader.Factory(context, address, builder);
    }
    return new BlockInStream(factory, conf, address, blockSource, blockId, blockSize);
  }

  /**
   * Creates a {@link BlockInStream} to read from a specific remote server. Should only be used
   * in cases where the data source and method of reading is known, ie. worker - worker
   * communication.
   *
   * @param context the file system context
   * @param blockId the block id
   * @param address the address of the gRPC data server
   * @param blockSource the source location of the block
   * @param blockSize the size of the block
   * @param ufsOptions the ufs read options
   * @return the {@link BlockInStream} created
   */
  public static BlockInStream createRemoteBlockInStream(FileSystemContext context, long blockId,
      WorkerNetAddress address, BlockInStreamSource blockSource, long blockSize,
      Protocol.OpenUfsBlockOptions ufsOptions) {
    AlluxioConfiguration conf = context.getClusterConf();
    long chunkSize = conf.getBytes(
        PropertyKey.USER_STREAMING_READER_CHUNK_SIZE_BYTES);
    ReadRequest readRequest = ReadRequest.newBuilder().setBlockId(blockId)
        .setOpenUfsBlockOptions(ufsOptions).setChunkSize(chunkSize).buildPartial();
    DataReader.Factory factory = new GrpcDataReader.Factory(context, address,
        readRequest.toBuilder());
    return new BlockInStream(factory, conf, address, blockSource, blockId, blockSize);
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param dataReaderFactory the data reader factory
   * @param conf the Alluxio configuration
   * @param address the address of the gRPC data server
   * @param blockSource the source location of the block
   * @param id the ID (either block ID or UFS file ID)
   * @param length the length
   */
  @VisibleForTesting
  protected BlockInStream(DataReader.Factory dataReaderFactory, AlluxioConfiguration conf,
      WorkerNetAddress address, BlockInStreamSource blockSource, long id, long length) {
    mDataReaderFactory = dataReaderFactory;
    mAddress = address;
    mInStreamSource = blockSource;
    mId = id;
    mLength = length;
  }

  @Override
  public long getPos() {
    return mPos;
  }

  @Override
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    return read(ByteBuffer.wrap(b), off, len);
  }

  /**
   * Reads up to len bytes of data from the input stream into the byte buffer.
   *
   * @param byteBuffer the buffer into which the data is read
   * @param off the start offset in the buffer at which the data is written
   * @param len the maximum number of bytes to read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data because
   *         the end of the stream has been reached
   */
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= byteBuffer.capacity(),
        PreconditionMessage.ERR_BUFFER_STATE.toString(), byteBuffer.capacity(), off, len);
    checkIfClosed();
    if (len == 0) {
      return 0;
    }
    if (mPos == mLength) {
      return -1;
    }
    readChunk();
    if (mCurrentChunk == null) {
      mEOF = true;
    }
    if (mEOF) {
      closeDataReader();
      Preconditions
          .checkState(mPos >= mLength, PreconditionMessage.BLOCK_LENGTH_INCONSISTENT.toString(),
              mId, mLength, mPos);
      return -1;
    }
    int toRead = Math.min(len, mCurrentChunk.readableBytes());
    byteBuffer.position(off).limit(off + toRead);
    mCurrentChunk.readBytes(byteBuffer);
    mPos += toRead;
    if (mPos == mLength) {
      // a performance improvement introduced by https://github.com/Alluxio/alluxio/issues/14020
      closeDataReader();
    }
    return toRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (pos < 0 || pos >= mLength) {
      return -1;
    }

    int lenCopy = len;
    try (DataReader reader = mDataReaderFactory.create(pos, len)) {
      // We try to read len bytes instead of returning after reading one chunk because
      // it is not free to create/close a DataReader.
      while (len > 0) {
        DataBuffer dataBuffer = null;
        try {
          dataBuffer = reader.readChunk();
          if (dataBuffer == null) {
            break;
          }
          Preconditions.checkState(dataBuffer.readableBytes() <= len);
          int toRead = dataBuffer.readableBytes();
          dataBuffer.readBytes(b, off, toRead);
          len -= toRead;
          off += toRead;
        } finally {
          if (dataBuffer != null) {
            dataBuffer.release();
          }
        }
      }
    }
    if (lenCopy == len) {
      return -1;
    }
    return lenCopy - len;
  }

  @Override
  public long remaining() {
    return mEOF ? 0 : mLength - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions
        .checkArgument(pos <= mLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_REGION.toString(),
            mId);
    if (pos == mPos) {
      return;
    }
    // When alluxio.fuse.shared.caching.reader.enabled is on (to resolve issues/12146),
    // use the heuristic to improve seek performance with fewer data reader close.
    if (mDataReader instanceof SharedGrpcDataReader) {
      seekForSharedGrpcDataReader(pos);
      return;
    }
    if (pos < mPos) {
      mEOF = false;
    }
    closeDataReader();
    mPos = pos;
  }

  private void seekForSharedGrpcDataReader(long pos) throws IOException {
    if (pos < mPos) {
      mEOF = false;
      // because the reader is shared, let's not close it but simply seek
      ((SharedGrpcDataReader) mDataReader).seek(pos);
      if (mCurrentChunk != null) {
        mCurrentChunk.release();
        mCurrentChunk = null;
      }
    } else {
      // TODO(lu) combine the original seek logic and the following general improvements
      // that are helpful in both fuse and non-fuse scenarios
      // Try to read data already received but haven't processed
      long curPos = mPos;
      while (mCurrentChunk != null && curPos < pos) {
        long nextPos = curPos + mCurrentChunk.readableBytes();
        if (nextPos <= pos) {
          curPos = nextPos;
          mCurrentChunk.release();
          mCurrentChunk = mDataReader.readChunk();
        } else {
          // TODO(chaowang) introduce seek in DataBuffer
          int toRead = (int) (pos - curPos);
          final byte[] b = new byte[toRead];
          mCurrentChunk.readBytes(b, 0, toRead);
          curPos = pos;
        }
      }

      if (curPos < pos) {
        // Not enough data in queue, close the data reader
        closeDataReader();
      }
    }
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(remaining(), n);
    mPos += toSkip;

    closeDataReader();
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      closeDataReader();
    } finally {
      mDataReaderFactory.close();
    }
    mClosed = true;
  }

  /**
   * @return the underlying data reader factory
   */
  @VisibleForTesting
  public DataReader.Factory getDataReaderFactory() {
    return mDataReaderFactory;
  }

  /**
   * Reads a new chunk from the channel if all of the current chunk is read.
   */
  private void readChunk() throws IOException {
    if (mDataReader == null) {
      mDataReader = mDataReaderFactory.create(mPos, mLength - mPos);
    }

    if (mCurrentChunk != null && mCurrentChunk.readableBytes() == 0) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mCurrentChunk == null) {
      mCurrentChunk = mDataReader.readChunk();
    }
  }

  /**
   * Close the current data reader.
   */
  private void closeDataReader() throws IOException {
    if (mCurrentChunk != null) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mDataReader != null) {
      mDataReader.close();
    }
    mDataReader = null;
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
  }

  /**
   * @return the address of the data server
   */
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * @return the source of the block location
   */
  public BlockInStreamSource getSource() {
    return mInStreamSource;
  }

  /**
   * @return the block ID
   */
  public long getId() {
    return mId;
  }
}
