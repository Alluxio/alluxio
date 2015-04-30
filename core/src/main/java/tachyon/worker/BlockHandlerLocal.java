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
import java.nio.channels.FileChannel.MapMode;

import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * BlockHandler for files on LocalFS, such as RamDisk, SSD and HDD.
 */
final class BlockHandlerLocal implements BlockHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Closer mCloser = Closer.create();

  BlockHandlerLocal(String filePath) throws FileNotFoundException {
    mFilePath = Preconditions.checkNotNull(filePath);
    LOG.debug("{} is created", mFilePath);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  /**
   * Check the bounds for reading
   * 
   * @param position the starting position in the block
   * @param length the size of the data to be read
   * @return actual read length
   * @throws IOException
   */
  private int checkBounds(long position, int length) throws IOException {
    long fileLength = mLocalFile.length();
    String error = null;
    if (position < 0 || position > fileLength) {
      error = String.format("Invalid start position(%d), file length(%d)",
          position, fileLength);
    } else if (length != -1 && length < 0) {
      error = String.format("Length(%d) can not be negative except -1", length);
    } else if (position + length > fileLength) {
      error =
          String.format("Start position(%d) plus length(%d) is larger than file length(%d)",
              position, length, fileLength);
    }
    if (error != null) {
      throw new IOException(error);
    }
    if (length == -1) {
      return (int) (fileLength - position);
    }
    return length;
  }

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

  public boolean delete() throws IOException {
    checkPermission();
    return new File(mFilePath).delete();
  }

  public FileRegion getFileRegion(long position, long length) {
    return new DefaultFileRegion(mLocalFileChannel, position, length);
  }

  public ByteBuffer read(long position, int length) throws IOException {
    int readLen = checkBounds(position, length);
    return mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, position, readLen);
  }

  public int transferTo(long position, int length, BlockHandler dest, long offset)
      throws IOException {
    ByteBuffer readBuf = read(position, length);
    return dest.write(offset, readBuf);
  }

  public int write(long position, ByteBuffer buf) throws IOException {
    checkPermission();
    int bufLen = buf.remaining();
    ByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, position, bufLen);
    out.put(buf);
    CommonUtils.cleanDirectBuffer(out);
    if (buf.remaining() == 0) {
      CommonUtils.cleanDirectBuffer(buf);
      return bufLen;
    } else {
      return bufLen - buf.remaining();
    }
  }
}
