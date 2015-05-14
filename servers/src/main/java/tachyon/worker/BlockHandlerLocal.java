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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * BlockHandler for files on LocalFS, such as RamDisk, SSD and HDD.
 */
public final class BlockHandlerLocal extends BlockHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Closer mCloser = Closer.create();

  BlockHandlerLocal(String filePath) throws IOException {
    mFilePath = Preconditions.checkNotNull(filePath);
    LOG.debug("{} is created", mFilePath);
    mLocalFile = mCloser.register(new RandomAccessFile(mFilePath, "rw"));
    mLocalFileChannel = mCloser.register(mLocalFile.getChannel());
  }

  @Override
  public int append(long offset, ByteBuffer buf) throws IOException {
    checkPermission();
    int bufLen = buf.limit();
    ByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, offset, bufLen);
    out.put(buf);
    CommonUtils.cleanDirectBuffer(out);

    return bufLen;
  }

  private void checkPermission() throws IOException {
    if (!mPermission) {
      // change the permission of the file and use the sticky bit
      CommonUtils.changeLocalFileToFullPermission(mFilePath);
      CommonUtils.setLocalFileStickyBit(mFilePath);
      mPermission = true;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    return new File(mFilePath).delete();
  }

  @Override
  public ByteChannel getChannel() {
    return mLocalFileChannel;
  }

  @Override
  public long getLength() throws IOException {
    return mLocalFile.length();
  }

  @Override
  public ByteBuffer read(long offset, int length) throws IOException {
    long fileLength = mLocalFile.length();
    String error = null;
    if (offset > fileLength) {
      error = String.format("offset(%d) is larger than file length(%d)", offset, fileLength);
    } else if (length != -1 && offset + length > fileLength) {
      error =
          String.format("offset(%d) plus length(%d) is larger than file length(%d)", offset,
              length, fileLength);
    }
    if (error != null) {
      throw new IOException(error);
    }
    if (length == -1) {
      length = (int) (fileLength - offset);
    }
    ByteBuffer buf = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    return buf;
  }
}
