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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.util.CommonUtils;

/**
 * It is used for handling block files on LocalFS, such as RamDisk, SSD and HDD.
 */
public final class BlockHandlerLocal extends BlockHandler {

  private final RandomAccessFile LOCAL_FILE;
  private final FileChannel LOCAL_FILE_CHANNEL;
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private boolean mPermission = false;

  BlockHandlerLocal(String filePath) throws IOException {
    super(filePath);
    LOG.debug(FILE_PATH + " is created");
    LOCAL_FILE = new RandomAccessFile(FILE_PATH, "rw");
    LOCAL_FILE_CHANNEL = LOCAL_FILE.getChannel();
  }

  @Override
  public int appendCurrentBuffer(byte[] buf, long inFilePos, int offset, int length)
      throws IOException {
    checkPermission();
    ByteBuffer out = LOCAL_FILE_CHANNEL.map(MapMode.READ_WRITE, inFilePos, length);
    out.put(buf, offset, length);

    return offset + length;
  }

  private void checkPermission() throws IOException {
    if (!mPermission) {
      // change the permission of the file and use the sticky bit
      CommonUtils.changeLocalFileToFullPermission(FILE_PATH);
      CommonUtils.setLocalFileStickyBit(FILE_PATH);
      mPermission = true;
    }
  }

  @Override
  public void close() throws IOException {
    if (LOCAL_FILE_CHANNEL != null) {
      try {
        LOCAL_FILE_CHANNEL.close();
      } catch (IOException e) {
        if (LOCAL_FILE != null) {
          LOCAL_FILE.close();
        }
        throw e;
      }
    }
    if (LOCAL_FILE != null) {
      LOCAL_FILE.close();
    }
  }

  @Override
  public boolean delete() throws IOException {
    checkPermission();
    return new File(FILE_PATH).delete();
  }

  @Override
  public ByteBuffer readByteBuffer(long inFilePos, int length) throws IOException {
    long fileLength = LOCAL_FILE.length();
    String error = null;
    if (inFilePos > fileLength) {
      error = String.format("inFilePos(%d) is larger than file length(%d)", inFilePos, fileLength);
    }
    if (error == null && length != -1 && inFilePos + length > fileLength) {
      error =
          String.format("inFilePos(%d) plus length(%d) is larger than file length(%d)", inFilePos,
              length, fileLength);
    }
    if (error != null) {
      throw new IOException(error);
    }
    if (length == -1) {
      length = (int) (fileLength - inFilePos);
    }
    ByteBuffer buf = LOCAL_FILE_CHANNEL.map(FileChannel.MapMode.READ_ONLY, inFilePos, length);
    return buf;
  }

  public FileChannel readChannel() {
    return LOCAL_FILE_CHANNEL;
  }

  public FileChannel writeChannel() throws IOException {
    checkPermission();
    return LOCAL_FILE_CHANNEL;
  }
}
