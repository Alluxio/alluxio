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

import alluxio.client.block.shortcircuit.SSRGetFileDescriptorClient;
import alluxio.client.block.shortcircuit.OpenBlockMessage;
import alluxio.conf.AlluxioConfiguration;
import alluxio.client.ReadType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.wire.WorkerNetAddress;

import alluxio.worker.block.io.LocalFileSecureBlockReader;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A secure data reader that reads packets from a local file .
 */
@NotThreadSafe
public final class LocalFileSecureDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileSecureDataReader.class);

  /**
   * The file reader to read a local block.
   */
  private final LocalFileSecureBlockReader mReader;
  private final long mEnd;
  private final long mChunkSize;
  private long mPos;
  private boolean mClosed;
 
  /**
   * Creates an instance of {@link LocalFileSecureDataReader}.
   *
   * @param reader    the file reader to the block path
   * @param offset    the offset
   * @param len       the length to read
   * @param chunkSize the chunk size
   */
  private LocalFileSecureDataReader(LocalFileSecureBlockReader reader, long offset, long len, long chunkSize) {
    mReader = reader;
    Preconditions.checkArgument(chunkSize > 0);
    mPos = offset;
    mEnd = Math.min(mReader.getLength(), offset + len);
    mChunkSize = chunkSize;
  }
  
  @Override
  public DataBuffer readChunk() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer = mReader.read(mPos, Math.min(mChunkSize, mEnd - mPos));
    DataBuffer dataBuffer = new NioDataBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    MetricsSystem.counter(MetricKey.CLIENT_BYTES_READ_LOCAL.getName()).inc(dataBuffer.getLength());
    MetricsSystem.meter(MetricKey.CLIENT_BYTES_READ_LOCAL_THROUGHPUT.getName())
        .mark(dataBuffer.getLength());
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mReader.decreaseUsageCount();
  }

  /**
   * Factory class to create {@link LocalFileSecureDataReader}s.
   */
  @NotThreadSafe
  public static class Factory implements DataReader.Factory {
    private long mLocalReaderChunkSize;
    private LocalFileSecureBlockReader mReader;
    private boolean mClosed;
    private FileDescriptor mBlockFD;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param context              the file system context
     * @param address              the worker address
     * @param blockId              the block ID
     * @param localReaderChunkSize chunk size in bytes for local reads
     * @param options              the instream options
     */
    public Factory(FileSystemContext context, long blockId,
        long localReaderChunkSize, InStreamOptions options) throws IOException {
      AlluxioConfiguration conf = context.getClusterConf();
      mLocalReaderChunkSize = localReaderChunkSize;

      boolean isPromote = ReadType.fromProto(options.getOptions().getReadType()).isPromote();
      try {
        String ssrDomainSocketPath = conf.get(PropertyKey.WORKER_SECURE_SHORT_CIRCUIT_READ_DOMAIN_SOCKET_ADDRESS);
        FileDescriptor ssrFileDescriptor = new FileDescriptor();
        CountDownLatch ssrLatch = new CountDownLatch(1);
        // Here assign EventLoopGroup thread nums = 2, since its default num is NettyRuntime.availableProcessors() * 2.
        // In fact we don't need so many threads because they make performance bad. 
        EventLoopGroup workerGroup = new EpollEventLoopGroup(2);
        new SSRGetFileDescriptorClient(new OpenBlockMessage(blockId, isPromote), ssrFileDescriptor, ssrDomainSocketPath,
                                       workerGroup, ssrLatch).start();
        ssrLatch.await();
        mBlockFD = ssrFileDescriptor;
        workerGroup.shutdownGracefully(0, 0, TimeUnit.MICROSECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public DataReader create(long offset, long len) throws IOException {
      if (mReader == null) {
        mReader = new LocalFileSecureBlockReader(mBlockFD);
      }
      Preconditions.checkState(mReader.getUsageCount() == 0);
      mReader.increaseUsageCount();
      return new LocalFileSecureDataReader(mReader, offset, len, mLocalReaderChunkSize);
    }
    
    @Override
    public boolean isShortCircuit() {
      return true;
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      try {
        if (mReader != null) {
          mReader.close();
        }
      } finally {
        mClosed = true;
      }
    }
  }
}

