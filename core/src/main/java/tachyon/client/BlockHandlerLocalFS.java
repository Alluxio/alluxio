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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import tachyon.util.CommonUtils;

/**
 * It is used for handling block files on LocalFS, such as RamDisk, SSD and HDD.
 */
public class BlockHandlerLocalFS extends BlockHandler {

  RandomAccessFile mLocalFile = null;
  FileChannel mLocalFileChannel = null;

  public BlockHandlerLocalFS(String path) throws IOException, FileNotFoundException {
    super(path);
    LOG.debug(mPath + " is created");
    mLocalFile = new RandomAccessFile(mPath, "rw");
    mLocalFileChannel = mLocalFile.getChannel();
    // change the permission of the temporary file in order that the worker can move it.
    CommonUtils.changeLocalFileToFullPermission(mPath);
    // use the sticky bit, only the client and the worker can write to the block
    CommonUtils.setLocalFileStickyBit(mPath);
  }

  @Override
  public int appendCurrentBuffer(byte[] buf, long inFileBytes, int offset, int length)
      throws IOException {
    if (mLocalFileChannel == null) {
      throw new IOException("Writing to unopened file!");
    }
    MappedByteBuffer out = mLocalFileChannel.map(MapMode.READ_WRITE, inFileBytes, length);
    out.put(buf, offset, length);

    return offset + length;
  }

  @Override
  public void close() throws IOException {
    if (mLocalFileChannel != null) {
      mLocalFileChannel.close();
    }
    if (mLocalFile != null) {
      mLocalFile.close();
    }
  }

  @Override
  public void delete() {
    if (mPath != null) {
      new File(mPath).delete();
    }
  }

  @Override
  public ByteBuffer readByteBuffer(int offset, int length) throws IOException {
    if (mLocalFileChannel == null || mLocalFile == null) {
      throw new IOException("Reading from unopened file!");
    }
    int fileLength = (int) mLocalFile.length();
    String error = null;
    if (offset > fileLength) {
      error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
    }
    if (error == null && length != -1 && offset + length > fileLength) {
      error =
          String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
              length, fileLength);
    }
    if (error != null) {
      mLocalFile.close();
      throw new IOException(error);
    }
    if (length == -1) {
      length = fileLength - offset;
    }
    ByteBuffer buf = mLocalFileChannel.map(FileChannel.MapMode.READ_ONLY, offset, length);
    return buf;
  }
}
