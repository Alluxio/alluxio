/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * BlockHandler for files on LocalFS, such as RamDisk, SSD and HDD.
 */
public final class BlockHandlerLocal extends BlockHandler {

  private final Logger mLog = Logger.getLogger(Constants.LOGGER_TYPE);
  private final RandomAccessFile mLocalFile;
  private final FileChannel mLocalFileChannel;
  private boolean mPermission = false;
  private final String mFilePath;
  private final Deque<Closeable> mStack = new ArrayDeque<Closeable>(2);

  BlockHandlerLocal(String filePath) throws IOException {
    mFilePath = Preconditions.checkNotNull(filePath);
    mLog.debug(mFilePath + " is created");
    mLocalFile = new RandomAccessFile(mFilePath, "rw");
    mLocalFileChannel = mLocalFile.getChannel();
    mStack.push(mLocalFile);
    mStack.push(mLocalFileChannel);
  }

  @Override
  public int append(long blockOffset, ByteBuffer srcBuf) throws IOException {
    checkPermission();
    ByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, blockOffset, srcBuf.limit());
    out.put(srcBuf);

    return srcBuf.limit();
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
    IOException exception = null;
    while (!mStack.isEmpty()) {
      try {
        mStack.pop().close();
      } catch (IOException e) {
        if (exception == null) {
          exception = e;
        } else {
          mLog.error(e.getMessage(), e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    return new File(mFilePath).delete();
  }

  @Override
  public ByteBuffer read(long blockOffset, int length) throws IOException {
    long fileLength = mLocalFile.length();
    String error = null;
    if (blockOffset > fileLength) {
      error =
          String.format("blockOffset(%d) is larger than file length(%d)", blockOffset, fileLength);
    }
    if (error == null && length != -1 && blockOffset + length > fileLength) {
      error =
          String.format("blockOffset(%d) plus length(%d) is larger than file length(%d)",
              blockOffset, length, fileLength);
    }
    if (error != null) {
      throw new IllegalArgumentException(error);
    }
    if (length == -1) {
      length = (int) (fileLength - blockOffset);
    }
    ByteBuffer buf = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, blockOffset, length);
    return buf;
  }
}
