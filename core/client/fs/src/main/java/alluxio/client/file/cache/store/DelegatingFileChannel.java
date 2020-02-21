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

package alluxio.client.file.cache.store;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A delegating {@link FileChannel} that also closes the FileInputStream as well.
 */
public class DelegatingFileChannel extends FileChannel {

  private FileChannel mFileChannel;
  private FileInputStream mInputStream;

  /**
   * Constructor for DelegatingFileChannel that takes in the {@link FileChannel} and {@link FileInputStream}
   * @param fileChannel the fileChannel
   * @param inputStream the input stream
   */
  public DelegatingFileChannel(FileChannel fileChannel, FileInputStream inputStream) {
    mFileChannel = fileChannel;
    mInputStream = inputStream;
  }


  @Override
  public int read(ByteBuffer dst) throws IOException {
    return mFileChannel.read(dst);
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
    return mFileChannel.read(dsts, offset, length);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return mFileChannel.write(src);
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
    return mFileChannel.write(srcs, offset, length);
  }

  @Override
  public long position() throws IOException {
    return mFileChannel.position();
  }

  @Override
  public FileChannel position(long newPosition) throws IOException {
    return mFileChannel.position(newPosition);
  }

  @Override
  public long size() throws IOException {
    return mFileChannel.size();
  }

  @Override
  public FileChannel truncate(long size) throws IOException {
    return mFileChannel.truncate(size);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    mFileChannel.force(metaData);
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return mFileChannel.transferTo(position, count, target);
  }

  @Override
  public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
    return mFileChannel.transferFrom(src, position, count);
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    return mFileChannel.read(dst, position);
  }

  @Override
  public int write(ByteBuffer src, long position) throws IOException {
    return mFileChannel.write(src, position);
  }

  @Override
  public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
    return mFileChannel.map(mode, position, size);
  }

  @Override
  public FileLock lock(long position, long size, boolean shared) throws IOException {
    return mFileChannel.lock(position, size, shared);
  }

  @Override
  public FileLock tryLock(long position, long size, boolean shared) throws IOException {
    return mFileChannel.tryLock(position, size, shared);
  }

  @Override
  protected void implCloseChannel() throws IOException {
    mFileChannel.close();
    mInputStream.close();
  }
}
