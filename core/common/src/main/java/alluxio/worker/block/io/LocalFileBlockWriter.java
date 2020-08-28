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

package alluxio.worker.block.io;

import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides write access to a temp block data file locally stored in managed storage.
 */
@NotThreadSafe
public class LocalFileBlockWriter extends BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFileBlockWriter.class);

  private final String mFilePath;
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private final Closer mCloser = Closer.create();
  private long mPosition;
  private boolean mClosed;

  /**
   * Constructs a Block writer given the file path of the block.
   *
   * @param path file path of the block
   */
  public LocalFileBlockWriter(String path) throws IOException {
    mFilePath = Preconditions.checkNotNull(path, "path");
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    long bytesWritten = write(mLocalFileChannel.size(), inputBuf.duplicate());
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    long bytesWritten = buf.readBytes(mLocalFileChannel, buf.readableBytes());
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(DataBuffer buffer) throws IOException {
    ByteBuf bytebuf = null;
    try {
      bytebuf = (ByteBuf) buffer.getNettyOutput();
    } catch (Throwable e) {
      LOG.debug("Failed to get ByteBuf from DataBuffer, write performance may be degraded.");
    }
    if (bytebuf != null) {
      return append(bytebuf);
    }
    long bytesWritten = write(mLocalFileChannel.size(), buffer);
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long getPosition() {
    return mPosition;
  }

  @Override
  public WritableByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    super.close();
    mCloser.close();
    mPosition = -1;
  }

  /**
   * Writes data to the block from an input {@link ByteBuffer}.
   *
   * @param offset starting offset of the block file to write
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was written
   */
  private long write(long offset, ByteBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.limit() - inputBuf.position();
    MappedByteBuffer outputBuf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    outputBuf.put(inputBuf);
    int bytesWritten = outputBuf.limit();
    BufferUtils.cleanDirectBuffer(outputBuf);
    return bytesWritten;
  }

  private long write(long offset, DataBuffer inputBuf) throws IOException {
    int inputBufLength = inputBuf.readableBytes();
    MappedByteBuffer outputBuf =
        mLocalFileChannel.map(FileChannel.MapMode.READ_WRITE, offset, inputBufLength);
    inputBuf.readBytes(outputBuf);
    int bytesWritten = outputBuf.limit();
    BufferUtils.cleanDirectBuffer(outputBuf);
    return bytesWritten;
  }
}
