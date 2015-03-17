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

package tachyon.worker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

final class BlockHandlerLocal implements BlockHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Closer mCloser = Closer.create();

  BlockHandlerLocal(final String filePath) throws FileNotFoundException {
    mFilePath = Preconditions.checkNotNull(filePath);
    LOG.debug("{} is created", mFilePath);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    return new File(mFilePath).delete();
  }

  @Override
  public int read(ByteBuffer buffer) throws IOException {
    return mLocalFileChannel.read(buffer);
  }

  @Override
  public int write(ByteBuffer srcBuf) throws IOException {
    checkPermission();
    return mLocalFileChannel.write(srcBuf);
  }

  @Override
  public boolean isOpen() {
    return mLocalFileChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  /**
   * Check the permission of the block, if not set, set the permission
   * 
   * @throws IOException
   */
  private void checkPermission() throws IOException {
    if (!mPermission) {
      // change the permission of the file and use the sticky bit
      CommonUtils.changeLocalFileToFullPermission(mFilePath);
      CommonUtils.setLocalFileStickyBit(mFilePath);
      mPermission = true;
    }
  }

  @Override
  public FileRegion getFileRegion(long offset, long length) {
    return new DefaultFileRegion(mLocalFileChannel, offset, length);
  }

  @Override
  public long position() throws IOException {
    return mLocalFileChannel.position();
  }

  @Override
  public BlockHandler position(long newPosition) throws IOException {
    mLocalFileChannel.position(newPosition);
    return this;
  }

  @Override
  public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
    return mLocalFileChannel.transferTo(position, count, target);
  }
}
